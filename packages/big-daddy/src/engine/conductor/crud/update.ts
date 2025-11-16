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
 * Uses executeOnShards with batch statement support to:
 * 1. SELECT the indexed columns before UPDATE (capture old values)
 * 2. Execute the UPDATE to modify rows
 * 3. SELECT the indexed columns after UPDATE (capture new values)
 * Both queries are executed in sequence per shard in a single call, reducing round trips.
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

	const selectStatement: SelectStatement = {
		type: 'SelectStatement',
		select: [...indexedColumns].map((column) => ({
			type: 'SelectClause',
			expression: { type: 'Identifier', name: column },
		})),
		from: statement.table,
		where: statement.where,
	};

	// Execute SELECT + UPDATE + SELECT in a single batch call to executeOnShards
	// This reduces network round-trips: instead of 3 calls per shard, we make 1 call
	// The results array contains [selectResults, updateResults, selectResults] for each shard
	const { results: batchResults } = await executeOnShards(
		context,
		shardsToQuery,
		[selectStatement, statement, selectStatement], // SELECT before, UPDATE, SELECT after
		params,
	);

	// Map results by shard
	const oldRows = new Map<number, Record<string, any>[]>();
	const newRows = new Map<number, Record<string, any>[]>();
	const finalUpdateResults: QueryResult[] = [];

	for (let i = 0; i < shardsToQuery.length; i++) {
		const shard = shardsToQuery[i]!;
		const [selectBeforeResult, updateResult, selectAfterResult] = (batchResults as QueryResult[][])[i]!;

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
 * Compares the indexed values before and after the update within each shard.
 * Deduplicates by shard to avoid removing values that are still present.
 */
function dedupeUpdatedRowsToEvents(
	oldRows: Map<number, Record<string, any>[]>,
	newRows: Map<number, Record<string, any>[]>,
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): IndexMaintenanceEventJob['events'] {
	// Use a Map to track events by shard: key is "index_name:key_value:shard_id"
	const eventMap = new Map<
		string,
		{
			index_name: string;
			key_value: string;
			shard_id: number;
			operation: 'add' | 'remove';
		}
	>();

	// Process each shard
	for (const [shardId, oldRowsList] of oldRows.entries()) {
		const newRowsList = newRows.get(shardId) || [];

		// For each index, determine what values to add/remove
		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns) as string[];

			// Build sets of values before and after
			const oldValues = new Set<string>();
			for (const row of oldRowsList) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue !== null) {
					oldValues.add(keyValue);
				}
			}

			const newValues = new Set<string>();
			for (const row of newRowsList) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue !== null) {
					newValues.add(keyValue);
				}
			}

			// Determine removals (values in old but not in new)
			for (const value of oldValues) {
				if (!newValues.has(value)) {
					const eventKey = `${index.index_name}:${value}:${shardId}`;
					eventMap.set(eventKey, {
						index_name: index.index_name,
						key_value: value,
						shard_id: shardId,
						operation: 'remove',
					});
				}
			}

			// Determine additions (values in new but not in old)
			for (const value of newValues) {
				if (!oldValues.has(value)) {
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
