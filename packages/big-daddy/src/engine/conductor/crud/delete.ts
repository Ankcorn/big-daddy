import type { DeleteStatement, SelectStatement } from "@databases/sqlite-ast";
import { logger } from "../../../logger";
import type { SqlParam as TopologySqlParam } from "../../topology/types";
import type {
	QueryHandlerContext,
	QueryResult,
	ShardInfo,
	SqlParam,
} from "../types";
import { mergeResultsSimple } from "../utils";
import { prepareIndexMaintenanceQueries } from "../utils/index-maintenance";
import {
	executeOnShards,
	getCachedQueryPlanData,
	invalidateCacheForWrite,
	logWriteIfResharding,
} from "../utils/write";

/**
 * Build SELECT statement for capturing indexed columns in DELETE rows
 */
function buildSelectForIndexedColumns(
	deleteStatement: DeleteStatement,
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): SelectStatement {
	const indexedColumns = new Set<string>();
	for (const index of virtualIndexes) {
		const columns = JSON.parse(index.columns) as string[];
		for (const col of columns) {
			indexedColumns.add(col);
		}
	}

	return {
		type: "SelectStatement",
		select: [...indexedColumns].map((column) => ({
			type: "SelectClause",
			expression: { type: "Identifier", name: column },
		})),
		from: deleteStatement.table,
		where: deleteStatement.where,
	};
}

/**
 * Handle DELETE query
 *
 * This handler:
 * 1. Gets the query plan (which shards to delete from) from topology
 * 2. Logs the write if resharding is in progress
 * 3. Prepares queries (SELECT + DELETE batch if indexes exist, else just DELETE)
 * 4. Executes the query on all target shards in parallel
 * 5. Dispatches index maintenance events if needed
 * 6. Invalidates relevant cache entries
 * 7. Merges and returns results
 */
export async function handleDelete(
	statement: DeleteStatement,
	query: string,
	params: SqlParam[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const tableName = statement.table.name;

	// STEP 1: Get cached query plan data
	const { planData } = await getCachedQueryPlanData(
		context,
		tableName,
		statement,
		params,
	);

	logger.info`Query plan determined for DELETE ${{ shardsSelected: planData.shardsToQuery.length }} ${{ indexesUsed: planData.virtualIndexes.length }}`;

	const shardsToQuery = planData.shardsToQuery;

	// STEP 2: Log write if resharding is in progress
	await logWriteIfResharding(tableName, statement.type, query, params, context);

	// STEP 3: Prepare queries (SELECT + DELETE batch if indexes, else just DELETE)
	const selectStatement =
		planData.virtualIndexes.length > 0
			? buildSelectForIndexedColumns(statement, planData.virtualIndexes)
			: undefined;

	const queries = prepareIndexMaintenanceQueries(
		planData.virtualIndexes.length > 0,
		statement,
		selectStatement,
		params,
	);

	// STEP 4: Execute on shards
	const execResult = await executeOnShards(context, shardsToQuery, queries);

	logger.info`Shard execution completed for DELETE ${{ shardsQueried: shardsToQuery.length }}`;

	// STEP 5: Synchronous index maintenance
	if (planData.virtualIndexes.length > 0) {
		const { databaseId, topology } = context;
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);

		const resultsArray = execResult.results as QueryResult[][];
		for (let i = 0; i < shardsToQuery.length; i++) {
			const [selectResult] = resultsArray[i]!;
			const rows = (selectResult?.rows || []) as Record<
				string,
				TopologySqlParam
			>[];
			const shard = shardsToQuery[i];
			if (!shard) continue;
			const shardId = shard.shard_id;

			if (rows.length > 0) {
				await topologyStub.maintainIndexesForDelete(
					rows,
					planData.virtualIndexes,
					shardId,
				);
			}
		}
	}

	// STEP 6: Invalidate cache entries for write operation
	invalidateCacheForWrite(
		context,
		tableName,
		statement,
		planData.virtualIndexes,
		params,
	);

	// STEP 7: Merge results from all shards
	let results: QueryResult[];
	if (planData.virtualIndexes.length > 0) {
		// Batch execution: extract delete results (second in batch)
		const resultsArray = execResult.results as QueryResult[][];
		results = resultsArray.map((batch) => batch[1]!);
	} else {
		// Single statement execution returns QueryResult[]
		results = execResult.results as QueryResult[];
	}

	const result = mergeResultsSimple(results, statement);

	// Add shard statistics
	result.shardStats = shardsToQuery.map((s: ShardInfo, i: number) => ({
		shardId: s.shard_id,
		nodeId: s.node_id,
		rowsReturned: 0,
		rowsAffected: results[i]?.rowsAffected ?? 0,
		duration: 0,
	}));

	// STEP 8: Decrement row counts for each shard
	if (result.rowsAffected && result.rowsAffected > 0) {
		const { databaseId, topology } = context;
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);

		// Build delta map from per-shard results (negative for deletions)
		const deltaByShard = new Map<number, number>();
		for (let i = 0; i < shardsToQuery.length; i++) {
			const shard = shardsToQuery[i];
			const shardResult = results[i];
			const rowsAffected = shardResult?.rowsAffected ?? 0;
			if (shard && rowsAffected > 0) {
				deltaByShard.set(shard.shard_id, -rowsAffected);
			}
		}

		if (deltaByShard.size > 0) {
			await topologyStub.batchBumpTableShardRowCounts(tableName, deltaByShard);
		}
	}

	logger.info`DELETE query completed ${{ shardsQueried: shardsToQuery.length }} ${{ rowsAffected: result.rowsAffected }}`;

	return result;
}
