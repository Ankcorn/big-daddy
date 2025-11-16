import type { DeleteStatement, SelectStatement } from '@databases/sqlite-ast';
import { parse, generate } from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import type { QueryResult, QueryHandlerContext, ShardInfo } from '../types';
import type { IndexMaintenanceEventJob } from '../../queue/types';
import { mergeResultsSimple } from '../utils';
import {
	executeOnShards,
	logWriteIfResharding,
	invalidateCacheForWrite,
	getCachedQueryPlanData,
	enqueueIndexMaintenanceJob,
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
 * Capture rows that will be deleted before deletion, for index maintenance
 *
 * Uses executeOnShards with batch statement support to:
 * 1. SELECT the indexed columns to capture what will be deleted
 * 2. Execute the DELETE to remove the rows
 * Both queries are executed in sequence per shard in a single call, reducing round trips.
 *
 * @returns Captured rows per shard and delete results
 */
async function captureDeletedRows(
	context: QueryHandlerContext,
	tableName: string,
	statement: DeleteStatement,
	params: any[],
	shardsToQuery: ShardInfo[],
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): Promise<{ capturedRows: Map<number, Record<string, any>[]>; deleteResults: QueryResult[] }> {
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

	// Execute both SELECT and DELETE in a single batch call to executeOnShards
	// This reduces network round-trips: instead of 2 calls per shard, we make 1 call
	// The results array contains [selectResults, deleteResults] for each shard
	const { results: batchResults } = await executeOnShards(
		context,
		shardsToQuery,
		[selectStatement, statement], // Execute both statements in sequence per shard
		params,
	);

	// Map results by shard
	const capturedRows = new Map<number, Record<string, any>[]>();
	const finalDeleteResults: QueryResult[] = [];

	for (let i = 0; i < shardsToQuery.length; i++) {
		const shard = shardsToQuery[i]!;
		const [selectResult, deleteResult] = (batchResults as QueryResult[][])[i]!;

		const rows = (selectResult as any).rows || [];
		capturedRows.set(shard.shard_id, rows as Record<string, any>[]);
		finalDeleteResults.push({
			rows: [],
			rowsAffected: deleteResult!.rowsAffected ?? 0,
		});
	}

	return { capturedRows, deleteResults: finalDeleteResults };
}

/**
 * Deduplicate captured deleted rows into index maintenance events
 *
 * Groups by (index_name, key_value, shard_id) to deduplicate
 * multiple rows with the same indexed value on the same shard
 */
function dedupeDeletedRowsToEvents(
	capturedRows: Map<number, Record<string, any>[]>,
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): IndexMaintenanceEventJob['events'] {
	// Use a Set to deduplicate: key is "index_name:key_value:shard_id"
	const dedupedMap = new Map<
		string,
		{
			index_name: string;
			key_value: string;
			shard_id: number;
			operation: 'remove';
		}
	>();

	// Process each shard's captured rows
	for (const [shardId, rows] of capturedRows.entries()) {
		// For each index, extract key values from rows
		for (const index of virtualIndexes) {
			const columns = JSON.parse(index.columns) as string[];

			for (const row of rows) {
				const keyValue = buildIndexKeyValue(row, columns);
				if (keyValue === null) {
					// Skip NULL values - they're not indexed
					continue;
				}

				const dedupKey = `${index.index_name}:${keyValue}:${shardId}`;
				if (!dedupedMap.has(dedupKey)) {
					dedupedMap.set(dedupKey, {
						index_name: index.index_name,
						key_value: keyValue,
						shard_id: shardId,
						operation: 'remove',
					});
				}
			}
		}
	}

	return Array.from(dedupedMap.values());
}

/**
 * Handle DELETE query
 *
 * This handler:
 * 1. Gets the query plan (which shards to delete from) from topology
 * 2. Logs the write if resharding is in progress
 * 3. Executes the query on all target shards in parallel
 * 4. Enqueues async index maintenance for affected indexes
 * 5. Invalidates relevant cache entries
 * 6. Merges and returns results
 */
export async function handleDelete(
	statement: DeleteStatement,
	query: string,
	params: any[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, indexQueue, correlationId } = context;
	const tableName = statement.table.name;

	logger.setTags({ table: tableName });

	// STEP 1: Get cached query plan data
	const { planData } = await getCachedQueryPlanData(context, tableName, statement, params);

	logger.info('Query plan determined for DELETE', {
		shardsSelected: planData.shardsToQuery.length,
		indexesUsed: planData.virtualIndexes.length,
	});

	const shardsToQuery = planData.shardsToQuery;

	// STEP 2: Log write if resharding is in progress
	await logWriteIfResharding(tableName, statement.type, query, params, context);

	// STEP 3: Execute query - capture deleted rows if indexes exist
	let results: QueryResult[];
	let shardStats: any[];

	if (planData.virtualIndexes.length > 0) {
		// Optimized path: capture rows for index maintenance + execute DELETE
		const { capturedRows, deleteResults } = await captureDeletedRows(
			context,
			tableName,
			statement,
			params,
			shardsToQuery,
			planData.virtualIndexes,
		);

		logger.info('Deleted rows captured for index maintenance', {
			shardsQueried: shardsToQuery.length,
			capturedShards: capturedRows.size,
		});

		// Deduplicate captured rows into index events
		const events = dedupeDeletedRowsToEvents(capturedRows, planData.virtualIndexes);

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

		// Use the delete results from capture
		results = deleteResults;
		shardStats = shardsToQuery.map((s: ShardInfo, i: number) => ({
			shardId: s.shard_id,
			nodeId: s.node_id,
			rowsReturned: 0,
			rowsAffected: deleteResults[i]?.rowsAffected ?? 0,
			duration: 0, // TODO: track duration in captureDeletedRows
		}));
	} else {
		// Standard path: no indexes, just execute DELETE
		const execResult = await executeOnShards(context, shardsToQuery, statement, params);

		logger.info('Shard execution completed for DELETE', {
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

	logger.info('DELETE query completed', {
		shardsQueried: shardsToQuery.length,
		rowsAffected: result.rowsAffected,
	});

	return result;
}
