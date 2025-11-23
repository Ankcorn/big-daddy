import type { DeleteStatement, SelectStatement } from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import type { QueryResult, QueryHandlerContext, ShardInfo } from '../types';
import { mergeResultsSimple } from '../utils';
import {
	executeOnShards,
	logWriteIfResharding,
	invalidateCacheForWrite,
	getCachedQueryPlanData,
} from '../utils/write';
import {
	prepareIndexMaintenanceQueries,
	dispatchIndexSyncingFromQueryResults,
} from '../utils/index-maintenance';


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
		columns.forEach((col) => indexedColumns.add(col));
	}

	return {
		type: 'SelectStatement',
		select: [...indexedColumns].map((column) => ({
			type: 'SelectClause',
			expression: { type: 'Identifier', name: column },
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
	params: any[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
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

	// STEP 3: Prepare queries (SELECT + DELETE batch if indexes, else just DELETE)
	const selectStatement = planData.virtualIndexes.length > 0
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

	logger.info('Shard execution completed for DELETE', {
		shardsQueried: shardsToQuery.length,
	});

	// STEP 5: Dispatch index maintenance if needed
	if (planData.virtualIndexes.length > 0) {
		await dispatchIndexSyncingFromQueryResults(
			'DELETE',
			execResult.results as QueryResult[][],
			tableName,
			shardsToQuery,
			planData.virtualIndexes,
			context,
			(results) => {
				// Extract oldRows from SELECT results (first in batch)
				const oldRows = new Map<number, Record<string, any>[]>();
				const resultsArray = results as QueryResult[][];
				for (let i = 0; i < shardsToQuery.length; i++) {
					const [selectResult] = resultsArray[i]!;
					const rows = (selectResult as any).rows || [];
					oldRows.set(shardsToQuery[i].shard_id, rows);
				}
				return { oldRows };
			},
		);
	}

	// STEP 6: Invalidate cache entries for write operation
	invalidateCacheForWrite(context, tableName, statement, planData.virtualIndexes, params);

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

	const result = mergeResultsSimple(results, false);

	// Add shard statistics
	result.shardStats = shardsToQuery.map((s: ShardInfo, i: number) => ({
		shardId: s.shard_id,
		nodeId: s.node_id,
		rowsReturned: 0,
		rowsAffected: results[i]?.rowsAffected ?? 0,
		duration: 0,
	}));

	logger.info('DELETE query completed', {
		shardsQueried: shardsToQuery.length,
		rowsAffected: result.rowsAffected,
	});

	return result;
}
