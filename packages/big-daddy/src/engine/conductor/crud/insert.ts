import type { InsertStatement } from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import type { QueryResult, QueryHandlerContext, ShardInfo } from '../types';
import { mergeResultsSimple } from '../utils';
import {
	executeOnShards,
	logWriteIfResharding,
	invalidateCacheForWrite,
	getCachedQueryPlanData,
} from '../utils/write';
import { prepareIndexMaintenanceQueries, dispatchIndexSyncingFromQueryResults } from '../utils/index-maintenance';

/**
 * Extract inserted rows from INSERT statement
 *
 * Parses the INSERT AST to extract column names and values,
 * building a row map indexed by shard ID (all rows to all target shards in this simplified implementation)
 */
function extractInsertedRows(
	statement: InsertStatement,
	params: any[],
	shardsToQuery: ShardInfo[],
): Map<number, Record<string, any>[]> {
	const newRows = new Map<number, Record<string, any>[]>();

	// Initialize empty arrays for each shard
	for (const shard of shardsToQuery) {
		newRows.set(shard.shard_id, []);
	}

	const columns = statement.columns;
	if (!columns || columns.length === 0) {
		return newRows; // No columns specified, can't extract values
	}

	const values = statement.values;
	if (!values || values.length === 0) {
		return newRows; // No values to insert
	}

	// For each inserted row
	values.forEach((valueList) => {
		// Build a row object with column names mapped to values
		const rowData: Record<string, any> = {};
		valueList.forEach((value, colIndex) => {
			const colIdent = columns[colIndex];
			if (colIdent) {
				// Extract column name from Identifier
				const colName = typeof colIdent === 'string' ? colIdent : (colIdent as any).name;
				// Value is either a Literal or Placeholder
				if (typeof value === 'object' && value !== null) {
					if ('type' in value && value.type === 'Placeholder') {
						// It's a placeholder - get value from params
						const paramIndex = (value as any).parameterIndex;
						rowData[colName] = params[paramIndex] ?? null;
					} else if ('type' in value && value.type === 'Literal') {
						// It's a literal value
						rowData[colName] = (value as any).value;
					}
				} else {
					// Direct value (shouldn't happen with parsed AST, but handle it)
					rowData[colName] = value;
				}
			}
		});

		// Map this row to all target shards
		// TODO: In practice, each row goes to exactly one shard based on partition key,
		// but we don't know the exact mapping yet. The index handler deduplicates anyway.
		for (const shard of shardsToQuery) {
			const shardRows = newRows.get(shard.shard_id) || [];
			shardRows.push(rowData);
			newRows.set(shard.shard_id, shardRows);
		}
	});

	return newRows;
}

/**
 * Handle INSERT query
 *
 * This handler:
 * 1. Gets the query plan (which shards to insert to) from topology
 * 2. Logs the write if resharding is in progress
 * 3. Prepares queries (just INSERT since INSERT doesn't batch with SELECT)
 * 4. Executes the query on all target shards in parallel
 * 5. Dispatches index maintenance events if needed
 * 6. Invalidates relevant cache entries
 * 7. Merges and returns results
 */
export async function handleInsert(
	statement: InsertStatement,
	query: string,
	params: any[],
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const tableName = statement.table.name;
	logger.setTags({ table: tableName });

	// STEP 1: Get cached query plan data
	const { planData } = await getCachedQueryPlanData(context, tableName, statement, params);

	logger.info('Query plan determined for INSERT', {
		shardsSelected: planData.shardsToQuery.length,
		indexesUsed: planData.virtualIndexes.length,
	});

	const shardsToQuery = planData.shardsToQuery;

	// STEP 2: Log write if resharding is in progress
	await logWriteIfResharding(tableName, statement.type, query, params, context);

	// STEP 3: Prepare queries (INSERT only, no batching needed)
	const queries = prepareIndexMaintenanceQueries(
		planData.virtualIndexes.length > 0,
		statement,
		undefined, // No selectStatement for INSERT
		params,
	);

	// STEP 4: Execute query on all target shards in parallel
	const execResult = await executeOnShards(context, shardsToQuery, queries);

	logger.info('Shard execution completed for INSERT', {
		shardsQueried: shardsToQuery.length,
	});

	// STEP 5: Dispatch index maintenance if needed
	if (planData.virtualIndexes.length > 0) {
		// Extract inserted rows from statement
		const newRows = extractInsertedRows(statement, params, shardsToQuery);

		// Dispatch index maintenance with extracted rows
		await dispatchIndexSyncingFromQueryResults(
			'INSERT',
			execResult.results as QueryResult[][],
			tableName,
			shardsToQuery,
			planData.virtualIndexes,
			context,
			() => ({ newRows }), // Return extracted rows
		);
	}

	// STEP 6: Invalidate cache entries for write operation
	invalidateCacheForWrite(context, tableName, statement, planData.virtualIndexes, params);

	// STEP 7: Merge results from all shards
	const results = execResult.results as QueryResult[];
	const result = mergeResultsSimple(results, false);

	// Add shard statistics
	result.shardStats = execResult.shardStats;

	logger.info('INSERT query completed', {
		shardsQueried: shardsToQuery.length,
		rowsAffected: result.rowsAffected,
	});

	return result;
}
