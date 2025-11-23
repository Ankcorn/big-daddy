import type { UpdateStatement, SelectStatement } from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import type { QueryResult, QueryHandlerContext, ShardInfo } from '../types';
import type { IndexMaintenanceEventJob } from '../../queue/types';
import { mergeResultsSimple } from '../utils';
import {
	executeOnShards,
	logWriteIfResharding,
	invalidateCacheForWrite,
	getCachedQueryPlanData,
} from '../utils/write';

/**
 * Build a key value for an indexed column(s) from a row
 * Returns null if any indexed column is NULL (NULL values are not indexed)
 */
function buildIndexKeyValue(row: Record<string, any>, columns: string[]): string | null {
	if (columns.length === 1) {
		const value = row[columns[0]!];
		if (value === null || value === undefined) {
			return null;
		}
		return String(value);
	} else {
		// Composite index - build key from all column values
		const values = columns.map((col) => row[col]);
		if (values.some((v) => v === null || v === undefined)) {
			return null;
		}
		return JSON.stringify(values);
	}
}

/**
 * Capture rows before and after UPDATE for index maintenance
 *
 * Executes three separate calls per shard:
 * 1. SELECT all indexed columns before UPDATE (capture old values)
 * 2. Execute the UPDATE to modify rows
 * 3. SELECT all indexed columns after UPDATE (capture new values)
 *
 * Important: SELECT statements intentionally have no WHERE clause so we capture ALL rows
 * in each shard. This is necessary for proper global deduplication across shards - we need
 * to know about all indexed values in the table to avoid incorrectly removing values that
 * are still used by other rows.
 *
 * Note: Unlike DELETE which batches [SELECT, DELETE], we execute UPDATE statements separately
 * because UPDATE's SET clause has parameters that SELECT doesn't use. This causes placeholder
 * count mismatches when batching statements with different parameter requirements.
 * The separate calls trade some round-trip efficiency for correct parameter binding.
 *
 * @returns Old rows per shard, new rows per shard, and update results
 */
async function captureUpdatedRows(
	context: QueryHandlerContext,
	tableName: string,
	statement: UpdateStatement,
	params: any[],
	shardsToQuery: ShardInfo[],
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): Promise<{
	oldRows: Map<number, Record<string, any>[]>;
	newRows: Map<number, Record<string, any>[]>;
	updateResults: QueryResult[];
}> {
	// Build the indexed column list
	const indexedColumns = new Set<string>();
	for (const index of virtualIndexes) {
		const columns = JSON.parse(index.columns) as string[];
		columns.forEach((col) => indexedColumns.add(col));
	}

	// Create a SELECT statement WITHOUT the WHERE clause to get all rows
	// This is necessary for proper global deduplication:
	// We need to know about ALL indexed values in the table to avoid incorrectly
	// removing values that are still used by other rows
	const selectStatement: SelectStatement = {
		type: 'SelectStatement',
		select: [...indexedColumns].map((column) => ({
			type: 'SelectClause',
			expression: { type: 'Identifier', name: column },
		})),
		from: statement.table,
		// Intentionally omit the WHERE clause - we want ALL rows for deduplication
	};

	// Execute SELECT before UPDATE
	const selectBeforeResults = await executeOnShards(
		context,
		shardsToQuery,
		selectStatement,
		[], // No params needed - SELECT has no WHERE clause
	);

	// Execute UPDATE
	const updateResults = await executeOnShards(
		context,
		shardsToQuery,
		statement,
		params,
	);

	// Execute SELECT after UPDATE
	const selectAfterResults = await executeOnShards(
		context,
		shardsToQuery,
		selectStatement,
		[], // No params needed - SELECT has no WHERE clause
	);

	// Map results by shard
	const oldRows = new Map<number, Record<string, any>[]>();
	const newRows = new Map<number, Record<string, any>[]>();
	const finalUpdateResults: QueryResult[] = [];

	for (let i = 0; i < shardsToQuery.length; i++) {
		const shard = shardsToQuery[i]!;
		const selectBeforeResult = (selectBeforeResults.results as QueryResult[])[i]!;
		const updateResult = (updateResults.results as QueryResult[])[i]!;
		const selectAfterResult = (selectAfterResults.results as QueryResult[])[i]!;

		const oldRowsData = (selectBeforeResult as any).rows || [];
		const newRowsData = (selectAfterResult as any).rows || [];

		oldRows.set(shard.shard_id, oldRowsData as Record<string, any>[]);
		newRows.set(shard.shard_id, newRowsData as Record<string, any>[]);
		finalUpdateResults.push({
			rows: [],
			rowsAffected: updateResult!.rowsAffected ?? 0,
		});
	}

	return { oldRows, newRows, updateResults: finalUpdateResults };
}

/**
 * Generate index maintenance events from old and new row values
 *
 * Compares the indexed values before and after the update across ALL shards.
 * Deduplicates globally to avoid removing values that are still present anywhere in the table.
 *
 * For example, if 'email@example.com' appears in rows on multiple shards:
 * - If row 1 (shard 0) changes away from 'email@example.com'
 * - But row 2 (shard 1) still has 'email@example.com'
 * - Then 'email@example.com' should NOT be removed from the index
 */
function dedupeUpdatedRowsToEvents(
	oldRows: Map<number, Record<string, any>[]>,
	newRows: Map<number, Record<string, any>[]>,
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): IndexMaintenanceEventJob['events'] {
	// Use a Map to track events: key is "index_name:key_value:shard_id"
	// This allows per-shard tracking while checking global deduplication
	const eventMap = new Map<
		string,
		{
			index_name: string;
			key_value: string;
			shard_id: number;
			operation: 'add' | 'remove';
		}
	>();

	// First pass: collect global old and new values across all shards
	const globalOldValues = new Map<string, Set<number>>(); // "index_name:key_value" -> set of shard IDs
	const globalNewValues = new Map<string, Set<number>>(); // "index_name:key_value" -> set of shard IDs

	for (const [shardId, oldRowsList] of oldRows.entries()) {
		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns) as string[];
			for (const row of oldRowsList) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue !== null) {
					const globalKey = `${index.index_name}:${keyValue}`;
					if (!globalOldValues.has(globalKey)) {
						globalOldValues.set(globalKey, new Set());
					}
					globalOldValues.get(globalKey)!.add(shardId);
				}
			}
		}
	}

	for (const [shardId, newRowsList] of newRows.entries()) {
		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns) as string[];
			for (const row of newRowsList) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue !== null) {
					const globalKey = `${index.index_name}:${keyValue}`;
					if (!globalNewValues.has(globalKey)) {
						globalNewValues.set(globalKey, new Set());
					}
					globalNewValues.get(globalKey)!.add(shardId);
				}
			}
		}
	}

	// Second pass: generate events based on global changes
	// For each shard, check which values were removed/added locally
	// But only queue a remove if the value is not present in ANY shard after the update
	for (const [shardId, oldRowsList] of oldRows.entries()) {
		const newRowsList = newRows.get(shardId) || [];

		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns) as string[];

			// Get old and new values for this shard
			const shardOldValues = new Set<string>();
			for (const row of oldRowsList) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue !== null) {
					shardOldValues.add(keyValue);
				}
			}

			const shardNewValues = new Set<string>();
			for (const row of newRowsList) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue !== null) {
					shardNewValues.add(keyValue);
				}
			}

			// Check what changed locally on this shard
			// But verify globally before queueing remove events
			for (const value of shardOldValues) {
				if (!shardNewValues.has(value)) {
					// Value was removed from this shard - check if it still exists globally
					const globalKey = `${index.index_name}:${value}`;
					const globalShards = globalNewValues.get(globalKey) || new Set();

					// Only queue removal if the value doesn't exist anywhere in the table
					if (globalShards.size === 0) {
						const eventKey = `${index.index_name}:${value}:${shardId}`;
						eventMap.set(eventKey, {
							index_name: index.index_name,
							key_value: value,
							shard_id: shardId,
							operation: 'remove',
						});
					}
				}
			}

			// Check additions: values that are new on this shard
			for (const value of shardNewValues) {
				if (!shardOldValues.has(value)) {
					// Value is new on this shard - but check if it was globally removed
					const globalKey = `${index.index_name}:${value}`;
					const wasGloballyOld = globalOldValues.has(globalKey);

					// Only queue addition if this is truly a new value (wasn't in the table before)
					if (!wasGloballyOld) {
						const eventKey = `${index.index_name}:${value}:${shardId}`;
						eventMap.set(eventKey, {
							index_name: index.index_name,
							key_value: value,
							shard_id: shardId,
							operation: 'add',
						});
					}
				}
			}
		}
	}

	return Array.from(eventMap.values());
}

/**
 * Handle UPDATE query
 *
 * This handler:
 * 1. Gets the query plan (which shards to update) from topology
 * 2. Logs the write if resharding is in progress
 * 3. Captures before/after rows if indexes exist, executes UPDATE
 * 4. Generates index maintenance events from the difference
 * 5. Invalidates relevant cache entries
 * 6. Merges and returns results
 */
export async function handleUpdate(
	statement: UpdateStatement,
	query: string,
	params: any[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, indexQueue, correlationId } = context;
	const tableName = statement.table.name;

	logger.setTags({ table: tableName });

	// STEP 1: Get cached query plan data
	const { planData } = await getCachedQueryPlanData(
		context,
		tableName,
		statement,
		params,
	);

	logger.info('Query plan determined for UPDATE', {
		shardsSelected: planData.shardsToQuery.length,
		indexesUsed: planData.virtualIndexes.length,
	});

	const shardsToQuery = planData.shardsToQuery;

	// STEP 2: Log write if resharding is in progress
	await logWriteIfResharding(tableName, statement.type, query, params, context);

	// STEP 3: Execute query - capture updated rows if indexes exist
	let results: QueryResult[];
	let shardStats: any[];

	if (planData.virtualIndexes.length > 0) {
		// Optimized path: capture rows before/after for index maintenance + execute UPDATE
		const { oldRows, newRows, updateResults } = await captureUpdatedRows(
			context,
			tableName,
			statement,
			params,
			shardsToQuery,
			planData.virtualIndexes,
		);

		logger.info('Updated rows captured for index maintenance', {
			shardsQueried: shardsToQuery.length,
			capturedShards: oldRows.size,
		});

		// Generate index events from the difference
		const events = dedupeUpdatedRowsToEvents(oldRows, newRows, planData.virtualIndexes);

		logger.info('Generated deduped index maintenance events', {
			eventCount: events.length,
		});

		// Queue the index maintenance events if any
		if (events.length > 0 && indexQueue) {
			const job: IndexMaintenanceEventJob = {
				type: 'maintain_index_events',
				database_id: databaseId,
				table_name: tableName,
				events,
				created_at: new Date().toISOString(),
				correlation_id: correlationId,
			};

			await indexQueue.send(job);

			logger.info('Index maintenance events queued', {
				eventCount: events.length,
			});
		}

		// Use the update results from capture
		results = updateResults;
		shardStats = shardsToQuery.map((s: ShardInfo, i: number) => ({
			shardId: s.shard_id,
			nodeId: s.node_id,
			rowsReturned: 0,
			rowsAffected: updateResults[i]?.rowsAffected ?? 0,
			duration: 0, // TODO: track duration in captureUpdatedRows
		}));
	} else {
		// Standard path: no indexes, just execute UPDATE
		const execResult = await executeOnShards(context, shardsToQuery, statement, params);

		logger.info('Shard execution completed for UPDATE', {
			shardsQueried: shardsToQuery.length,
		});

		// Single statement execution returns QueryResult[] (not QueryResult[][])
		results = execResult.results as QueryResult[];
		shardStats = execResult.shardStats;
	}

	// STEP 4: Invalidate cache entries for write operation
	invalidateCacheForWrite(context, tableName, statement, planData.virtualIndexes, params);

	// STEP 5: Merge results from all shards
	const result = mergeResultsSimple(results, false);

	// Add shard statistics
	result.shardStats = shardStats;

	logger.info('UPDATE query completed', {
		shardsQueried: shardsToQuery.length,
		rowsAffected: result.rowsAffected,
	});

	return result;
}
