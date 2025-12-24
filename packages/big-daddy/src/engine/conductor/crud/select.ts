import type { SelectStatement } from "@databases/sqlite-ast";
import { logger } from "../../../logger";
import { extractTableName as extractTableNameFromAST } from "../../utils/ast-utils";
import type { QueryHandlerContext, QueryResult, SqlParam } from "../types";
import { mergeResultsSimple } from "../utils";
import { executeOnShards, getCachedQueryPlanData } from "../utils/write";

/**
 * Execute a SELECT query on the appropriate shards
 *
 * This handler:
 * 1. Gets the query plan (which shards to query) from topology
 * 2. Executes the query on all target shards in parallel
 * 3. Merges results from all shards
 * 4. Adds cache statistics
 */
export async function handleSelect(
	statement: SelectStatement,
	_query: string,
	params: SqlParam[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { cache } = context;
	const tableName = extractTableNameFromAST(statement);

	if (!tableName) {
		throw new Error("Could not determine table name from SELECT query");
	}

	// STEP 1: Get cached query plan data
	// This determines which shards to query
	const { planData, cacheHit } = await getCachedQueryPlanData(
		context,
		tableName,
		statement,
		params,
	);

	logger.info`Query plan determined for SELECT ${{ cacheHit }} ${{ shardsSelected: planData.shardsToQuery.length }} ${{ indexesUsed: planData.virtualIndexes.length }}`;

	const shardsToQuery = planData.shardsToQuery;

	// STEP 2: Execute query on all target shards in parallel
	const { results, shardStats } = await executeOnShards(
		context,
		shardsToQuery,
		statement,
		params,
	);

	// STEP 3: Merge results from all shards
	const result = mergeResultsSimple(results, statement);

	// STEP 4: Add cache statistics
	const stats = cache.getStats();
	result.cacheStats = {
		cacheHit,
		totalHits: stats.hits,
		totalMisses: stats.misses,
		cacheSize: stats.size,
	};

	// Add shard statistics
	result.shardStats = shardStats;

	logger.info`SELECT query completed ${{ shardsQueried: shardsToQuery.length }} ${{ rowCount: result.rows.length }}`;

	return result;
}
