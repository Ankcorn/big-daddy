import type { InsertStatement } from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import type { QueryResult, QueryHandlerContext, ShardInfo } from '../types';
import { mergeResultsSimple } from '../utils';
import { executeOnShards, logWriteIfResharding, invalidateCacheForWrite, getCachedQueryPlanData } from '../utils/write';

/**
 * Handle INSERT query
 *
 * This handler:
 * 1. Gets the query plan (which shards to insert to) from topology
 * 2. Logs the write if resharding is in progress
 * 3. Executes the query on all target shards in parallel
 * 4. Enqueues async index maintenance for any indexes on this table
 * 5. Invalidates relevant cache entries
 * 6. Merges and returns results
 */
export async function handleInsert(
	statement: InsertStatement,
	query: string,
	params: any[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, storage, topology, cache, correlationId } = context;
	const tableName = statement.table.name;

	logger.setTags({ table: tableName });

	// STEP 1: Get cached query plan data
	const { planData } = await getCachedQueryPlanData(context, tableName, statement, params);

	logger.info('Query plan determined for INSERT', {
		shardsSelected: planData.shardsToQuery.length,
	});

	const shardsToQuery = planData.shardsToQuery;

	// STEP 2: Log write if resharding is in progress
	await logWriteIfResharding(tableName, statement.type, query, params, context);

	// STEP 3: Execute query on all target shards in parallel
	const { results, shardStats } = await executeOnShards(context, shardsToQuery, statement, params);

	logger.info('Shard execution completed for INSERT', {
		shardsQueried: shardsToQuery.length,
	});

	// STEP 4: Index maintenance is handled asynchronously by topology during the query plan
	// INSERT index building happens during getQueryPlanData call, not after

	// STEP 5: Invalidate cache entries for write operation
	invalidateCacheForWrite(context, tableName, statement, planData.virtualIndexes, params);

	// STEP 6: Merge results from all shards
	const result = mergeResultsSimple(results, false);

	// Add shard statistics
	result.shardStats = shardStats;

	logger.info('INSERT query completed', {
		shardsQueried: shardsToQuery.length,
		rowsAffected: result.rowsAffected,
	});

	return result;
}
