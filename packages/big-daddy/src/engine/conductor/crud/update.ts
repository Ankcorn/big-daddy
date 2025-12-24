import type { SelectStatement, UpdateStatement } from "@databases/sqlite-ast";
import { logger } from "../../../logger";
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
 * Build SELECT statement for capturing indexed columns in UPDATE rows
 */
function buildSelectForIndexedColumns(
	updateStatement: UpdateStatement,
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
		from: updateStatement.table,
		where: updateStatement.where,
	};
}

/**
 * Count parameter placeholders in an AST node
 */
function countPlaceholders(node: unknown): number {
	if (!node) return 0;
	if (
		typeof node === "object" &&
		node !== null &&
		"type" in node &&
		node.type === "Placeholder"
	)
		return 1;
	let count = 0;
	if (Array.isArray(node)) {
		for (const item of node) {
			count += countPlaceholders(item);
		}
	} else if (typeof node === "object" && node !== null) {
		for (const value of Object.values(node)) {
			count += countPlaceholders(value);
		}
	}
	return count;
}

/**
 * Handle UPDATE query
 *
 * This handler:
 * 1. Gets the query plan (which shards to update) from topology
 * 2. Logs the write if resharding is in progress
 * 3. Prepares queries (SELECT before + UPDATE + SELECT after batch if indexes exist, else just UPDATE)
 * 4. Executes the query on all target shards in parallel
 * 5. Dispatches index maintenance events if needed
 * 6. Invalidates relevant cache entries
 * 7. Merges and returns results
 */
export async function handleUpdate(
	statement: UpdateStatement,
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

	logger.info`Query plan determined for UPDATE ${{ shardsSelected: planData.shardsToQuery.length }} ${{ indexesUsed: planData.virtualIndexes.length }}`;

	const shardsToQuery = planData.shardsToQuery;

	// STEP 2: Log write if resharding is in progress
	await logWriteIfResharding(tableName, statement.type, query, params, context);

	// STEP 3: Prepare queries
	// SELECT statements only use WHERE clause parameters; UPDATE uses all parameters
	const selectStatement =
		planData.virtualIndexes.length > 0
			? buildSelectForIndexedColumns(statement, planData.virtualIndexes)
			: undefined;

	// Extract only WHERE clause params for the SELECT (skip SET clause params)
	// UPDATE params order: SET values first, then WHERE values
	const setPlaceholderCount = countPlaceholders(statement.set);
	const wherePlaceholderCount = countPlaceholders(statement.where);
	const selectParams =
		selectStatement && wherePlaceholderCount > 0
			? params.slice(
					setPlaceholderCount,
					setPlaceholderCount + wherePlaceholderCount,
				)
			: [];

	const queries = prepareIndexMaintenanceQueries(
		planData.virtualIndexes.length > 0,
		statement,
		selectStatement,
		params,
		selectParams,
	);

	// STEP 4: Execute on shards
	const execResult = await executeOnShards(context, shardsToQuery, queries);

	logger.info`Shard execution completed for UPDATE ${{ shardsQueried: shardsToQuery.length }}`;

	// STEP 5: Synchronous index maintenance
	if (planData.virtualIndexes.length > 0) {
		const { databaseId, topology } = context;
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);

		const resultsArray = execResult.results as QueryResult[][];
		for (let i = 0; i < shardsToQuery.length; i++) {
			const [selectBefore, , selectAfter] = resultsArray[i]!;
			const oldRows = (selectBefore?.rows || []) as Record<string, SqlParam>[];
			const newRows = (selectAfter?.rows || []) as Record<string, SqlParam>[];
			const shard = shardsToQuery[i];
			if (!shard) continue;
			const shardId = shard.shard_id;

			if (oldRows.length > 0 || newRows.length > 0) {
				await topologyStub.maintainIndexesForUpdate(
					oldRows,
					newRows,
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
		// Batch execution: extract update results (second in batch)
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

	logger.info`UPDATE query completed ${{ shardsQueried: shardsToQuery.length }} ${{ rowsAffected: result.rowsAffected }}`;

	return result;
}
