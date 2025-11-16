import type { SelectStatement, InsertStatement, UpdateStatement, DeleteStatement } from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import type { QueryResult, ShardStats, QueryHandlerContext, ShardInfo } from '../types';
import type { IndexMaintenanceJob, IndexJob } from '../../queue/types';
import { injectVirtualShardFilter } from './helpers';
/**
 * Execute a query (SELECT/INSERT/UPDATE/DELETE) on shards in parallel batches
 *
 * This method:
 * 1. Processes shards in batches of 7 to avoid overwhelming the system
 * 2. Injects _virtualShard filter for shard-specific isolation
 * 3. Tracks execution statistics for each shard
 * 4. Returns merged results and shard performance data
 */
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statement: SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
	params: any[],
): Promise<{ results: QueryResult[]; shardStats: ShardStats[] }> {
	const { storage } = context;
	const BATCH_SIZE = 7;
	const allResults: QueryResult[] = [];
	const allShardStats: ShardStats[] = [];

	// Process shards in batches of 7
	for (let i = 0; i < shardsToQuery.length; i += BATCH_SIZE) {
		const batch = shardsToQuery.slice(i, i + BATCH_SIZE);
		const batchNum = Math.floor(i / BATCH_SIZE) + 1;

		const batchResults = await Promise.all(
			batch.map(async (shard) => {
				const shardStartTime = Date.now();
				const storageId = storage.idFromName(shard.node_id);
				const storageStub = storage.get(storageId);

				try {
					// Clone the statement to avoid mutations when processing multiple shards
					const statementClone = JSON.parse(JSON.stringify(statement));
					const { modifiedQuery, modifiedParams } = injectVirtualShardFilter(statementClone, params, shard.shard_id);

					const rawResult = (await storageStub.executeQuery({
						query: modifiedQuery,
						params: modifiedParams,
					})) as any;

					const shardDuration = Date.now() - shardStartTime;

					const shardStat: ShardStats = {
						shardId: shard.shard_id,
						nodeId: shard.node_id,
						rowsReturned: rawResult.rows.length,
						rowsAffected: rawResult.rowsAffected ?? 0,
						duration: shardDuration,
					};

					allShardStats.push(shardStat);

					return {
						rows: rawResult.rows,
						rowsAffected: rawResult.rowsAffected ?? 0,
					};
				} catch (error) {
					logger.error(`${statement.type} shard query failed`, {
						shardId: shard.shard_id,
						error: error instanceof Error ? error.message : String(error),
					});
					throw error;
				}
			}),
		);

		allResults.push(...batchResults);
	}

	return { results: allResults, shardStats: allShardStats };
}

/**
 * Log write operations during resharding for later replay
 */
export async function logWriteIfResharding(
	tableName: string,
	operationType: string,
	query: string,
	params: any[],
	context: QueryHandlerContext,
): Promise<void> {
	const { topology, databaseId, indexQueue } = context;

	// Get the current resharding state for this table
	const topologyId = topology.idFromName(databaseId);
	const topologyStub = topology.get(topologyId);
	const reshardingState = await topologyStub.getReshardingState(tableName);

	// Only log if resharding is active and in copying phase
	if (!reshardingState || reshardingState.status !== 'copying') {
		return;
	}

	// Log the write operation to the queue for later replay
	const operation = (operationType === 'InsertStatement' ? 'INSERT' : operationType === 'UpdateStatement' ? 'UPDATE' : 'DELETE') as
		| 'INSERT'
		| 'UPDATE'
		| 'DELETE';

	const changeLogEntry = {
		type: 'resharding_change_log' as const,
		resharding_id: reshardingState.change_log_id,
		database_id: databaseId,
		table_name: tableName,
		operation,
		query,
		params,
		timestamp: Date.now(),
		correlation_id: context.correlationId,
	};

	try {
		if (indexQueue) {
			await indexQueue.send(changeLogEntry);
		}
	} catch (error) {
		// Log but don't fail - write logging should not block query execution
		logger.warn('Failed to log write during resharding', {
			table: tableName,
			operation,
			error: (error as Error).message,
		});
	}
}

/**
 * Enqueue index maintenance job for UPDATE/DELETE operations
 * This is async - index updates happen in the background
 */
export async function enqueueIndexMaintenanceJob(
	context: QueryHandlerContext,
	tableName: string,
	statement: UpdateStatement | DeleteStatement,
	shardIds: number[],
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): Promise<void> {
	const { indexQueue } = context;

	if (!indexQueue) {
		return;
	}

	// For UPDATE: determine which indexes are affected by the updated columns
	const updatedColumns = statement.type === 'UpdateStatement' ? statement.set.map((s) => s.column.name) : undefined;

	// Filter to affected indexes only
	const affectedIndexes = updatedColumns
		? virtualIndexes.filter((idx) => {
				const indexCols = JSON.parse(idx.columns);
				return indexCols.some((col: string) => updatedColumns.includes(col));
			})
		: virtualIndexes; // DELETE affects all indexes

	if (affectedIndexes.length === 0) {
		return; // No indexes to maintain
	}

	const job: IndexMaintenanceJob = {
		type: 'maintain_index',
		database_id: context.databaseId,
		table_name: tableName,
		operation: statement.type === 'UpdateStatement' ? 'UPDATE' : 'DELETE',
		shard_ids: shardIds,
		affected_indexes: affectedIndexes.map((idx) => idx.index_name),
		updated_columns: updatedColumns,
		created_at: new Date().toISOString(),
		correlation_id: context.correlationId,
	};

	try {
		if (indexQueue) {
			await indexQueue.send(job);
		}
	} catch (error) {
		logger.error('Failed to enqueue index maintenance job', {
			error: (error as Error).message,
		});
		// Don't throw - index operations should not block queries
	}
}

/**
 * Invalidate cache entries for write operations
 */
export function invalidateCacheForWrite(
	context: QueryHandlerContext,
	tableName: string,
	statement: InsertStatement | UpdateStatement | DeleteStatement,
	virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
	params: any[],
): void {
	const { cache } = context;

	// Always invalidate the query plan cache for this table
	cache.invalidateTable(tableName);

	// Invalidate index caches for affected index values
	if (virtualIndexes.length === 0) {
		return; // No indexes to invalidate
	}

	if (statement.type === 'InsertStatement') {
		invalidateIndexCacheForInsert(cache, statement, virtualIndexes, params);
	} else if (statement.type === 'UpdateStatement') {
		invalidateIndexCacheForUpdate(cache, statement, virtualIndexes);
	} else if (statement.type === 'DeleteStatement') {
		invalidateIndexCacheForDelete(cache, statement, virtualIndexes);
	}
}

/**
 * Invalidate index cache entries for INSERT operation
 */
function invalidateIndexCacheForInsert(
	cache: any,
	statement: InsertStatement,
	virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
	params: any[],
): void {
	if (!statement.columns || statement.values.length === 0) {
		return;
	}

	const { extractKeyValueFromRow } = require('./utils');

	const columns = statement.columns;
	const row = statement.values[0]; // Only handle single-row inserts for now

	for (const index of virtualIndexes) {
		const indexColumns = JSON.parse(index.columns);
		const keyValue = extractKeyValueFromRow(columns, row, indexColumns, params);
		if (keyValue) {
			cache.invalidateIndexKeys(index.index_name, [keyValue]);
		}
	}
}

/**
 * Invalidate index cache entries for UPDATE operation
 */
function invalidateIndexCacheForUpdate(
	cache: any,
	statement: UpdateStatement,
	virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
): void {
	// Get the columns being updated
	const updatedColumns = statement.set.map((s) => s.column.name);

	// Only invalidate indexes that include updated columns
	const affectedIndexes = virtualIndexes.filter((idx) => {
		const indexCols = JSON.parse(idx.columns);
		return indexCols.some((col: string) => updatedColumns.includes(col));
	});

	// For UPDATE, we can't easily determine the old/new values without querying
	// So we invalidate the entire index cache for affected indexes
	for (const index of affectedIndexes) {
		cache.invalidateIndex(index.index_name);
	}
}

/**
 * Invalidate index cache entries for DELETE operation
 */
function invalidateIndexCacheForDelete(
	cache: any,
	statement: DeleteStatement,
	virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
): void {
	// For DELETE, we can't easily determine which values are being deleted without querying
	// So we invalidate the entire index cache for all indexes
	for (const index of virtualIndexes) {
		cache.invalidateIndex(index.index_name);
	}
}

/**
 * Get cached query plan data
 *
 * This function checks the cache first before calling the Topology DO.
 * For cacheable queries (SELECT/UPDATE/DELETE with WHERE clause), this can eliminate
 * the Topology DO call entirely.
 *
 * @returns Query plan data and whether it was a cache hit
 */
export async function getCachedQueryPlanData(
	context: QueryHandlerContext,
	tableName: string,
	statement: any,
	params: any[],
): Promise<{ planData: any; cacheHit: boolean }> {
	const { topology, databaseId, cache, correlationId } = context;

	// Try to build a cache key for this query
	const cacheKey = cache.buildQueryPlanCacheKey(tableName, statement, params);

	// Check cache if we have a valid cache key
	if (cacheKey) {
		const cached = cache.getQueryPlanData(cacheKey);
		if (cached) {
			return { planData: cached, cacheHit: true };
		}
	}

	// Cache miss - fetch from Topology DO
	const topologyId = topology.idFromName(databaseId);
	const topologyStub = topology.get(topologyId);
	const planData = await topologyStub.getQueryPlanData(tableName, statement, params, correlationId);

	// Store in cache if cacheable
	if (cacheKey) {
		const ttl = cache.getTTLForQuery(statement);
		cache.setQueryPlanData(cacheKey, planData, ttl);
	}

	return { planData, cacheHit: false };
}
