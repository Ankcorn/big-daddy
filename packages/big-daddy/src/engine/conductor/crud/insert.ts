import type {
	Identifier,
	InsertStatement,
	Literal,
	Placeholder,
} from "@databases/sqlite-ast";
import { logger } from "../../../logger";
import type {
	QueryHandlerContext,
	QueryResult,
	ShardStats,
	SqlParam,
} from "../types";
import { mergeResultsSimple } from "../utils";
import { hashToShardId } from "../utils/helpers";
import { prepareIndexMaintenanceQueries } from "../utils/index-maintenance";
import {
	executeOnShards,
	getCachedQueryPlanData,
	invalidateCacheForWrite,
	logWriteIfResharding,
} from "../utils/write";

/**
 * Group INSERT rows by target shard and create per-shard INSERT statements
 *
 * For each row in the INSERT:
 * 1. Extract the shard key value
 * 2. Hash it to determine the target shard
 * 3. Group rows by shard
 * 4. Create a separate INSERT statement for each shard with only its rows
 * 5. Remap parameter indices for each shard's statement
 *
 * @param statement The original INSERT statement
 * @param shardKeyColumn The name of the shard key column
 * @param shardIds Array of actual shard IDs (may not be 0-indexed after resharding)
 * @param params Query parameters
 * @returns Map of shardId -> { statement: INSERT statement, params: filtered params }
 */
function groupInsertByShards(
	statement: InsertStatement,
	shardKeyColumn: string,
	shardIds: number[],
	params: SqlParam[],
): Map<number, { statement: InsertStatement; params: SqlParam[] }> {
	// Map of shardId -> { rows, paramIndices }
	const shardGroups = new Map<
		number,
		{ rows: typeof statement.values; paramIndices: number[] }
	>();

	const columns = statement.columns;
	if (!columns || columns.length === 0) {
		logger.warn`No columns in INSERT statement`;
		return new Map();
	}

	const values = statement.values;
	if (!values || values.length === 0) {
		logger.warn`No values in INSERT statement`;
		return new Map();
	}

	// Find the index of the shard key column
	const shardKeyIndex = columns.findIndex(
		(col: Identifier) => col.name === shardKeyColumn,
	);

	if (shardKeyIndex === -1) {
		logger.warn`Shard key column not found in INSERT, may not distribute optimally ${{ shardKey: shardKeyColumn }}`;
		// If shard key not in INSERT, can't determine target shard
		return new Map();
	}

	// Group each row by target shard, tracking which parameters belong to this row
	values.forEach((valueList, rowIndex) => {
		// Extract the shard key value from this row
		const shardKeyExpr = valueList[shardKeyIndex];
		let shardKeyValue: SqlParam = null;

		if (!shardKeyExpr) {
			logger.warn`Missing shard key value in INSERT row ${{ rowIndex }}`;
			return;
		}

		if (typeof shardKeyExpr === "object" && shardKeyExpr !== null) {
			if ("type" in shardKeyExpr && shardKeyExpr.type === "Placeholder") {
				shardKeyValue = params[(shardKeyExpr as Placeholder).parameterIndex]!;
			} else if ("type" in shardKeyExpr && shardKeyExpr.type === "Literal") {
				shardKeyValue = (shardKeyExpr as Literal).value as SqlParam;
			}
		} else {
			shardKeyValue = shardKeyExpr as SqlParam;
		}

		// Hash the shard key value to determine target shard
		// Map the hash index to the actual shard ID (handles non-0-indexed shards after resharding)
		const hashIndex = hashToShardId(shardKeyValue, shardIds.length);
		const shardId = shardIds[hashIndex]!;

		// Track parameters for this row
		const rowParamIndices = valueList
			.map((expr) => {
				if (
					typeof expr === "object" &&
					expr !== null &&
					"type" in expr &&
					expr.type === "Placeholder"
				) {
					return (expr as Placeholder).parameterIndex;
				}
				return -1; // Not a placeholder, skip
			})
			.filter((idx) => idx !== -1);

		// Group this row and its parameters by shard
		if (!shardGroups.has(shardId)) {
			shardGroups.set(shardId, { rows: [], paramIndices: [] });
		}
		shardGroups.get(shardId)?.rows.push(valueList);
		shardGroups.get(shardId)?.paramIndices.push(...rowParamIndices);
	});

	// Create INSERT statements and filtered parameter arrays for each shard
	const result = new Map<
		number,
		{ statement: InsertStatement; params: SqlParam[] }
	>();
	for (const [shardId, group] of shardGroups) {
		// Extract only the parameters needed for this shard's rows
		const shardParams = group.paramIndices.map((idx) => params[idx]!);

		// Remap the placeholder indices in the rows to the new parameter array
		// paramCounter tracks across all rows in this shard, not per row
		let paramCounter = 0;
		const remappedRows = group.rows.map((valueList) => {
			return valueList.map((expr) => {
				if (
					typeof expr === "object" &&
					expr !== null &&
					"type" in expr &&
					expr.type === "Placeholder"
				) {
					// Remap the placeholder index
					const remappedExpr: Placeholder = {
						...(expr as Placeholder),
						parameterIndex: paramCounter,
					};
					paramCounter++;
					return remappedExpr;
				}
				return expr;
			});
		});

		result.set(shardId, {
			statement: {
				...statement,
				values: remappedRows,
			},
			params: shardParams,
		});
	}

	return result;
}

/**
 * Handle INSERT query
 *
 * This handler:
 * 1. Gets the query plan and table metadata from topology
 * 2. Groups INSERT rows by target shard based on shard key hashing
 * 3. Logs the write if resharding is in progress
 * 4. Prepares and executes per-shard INSERT queries
 * 5. Dispatches index maintenance events if needed
 * 6. Invalidates relevant cache entries
 * 7. Merges and returns results
 */
export async function handleInsert(
	statement: InsertStatement,
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

	logger.info`Query plan determined for INSERT ${{ shardsSelected: planData.shardsToQuery.length }} ${{
		indexesUsed: planData.virtualIndexes.length,
	}} ${{ shardKey: planData.shardKey }}`;

	const allShards = planData.shardsToQuery;

	// STEP 2: Group INSERT rows by target shard based on shard key
	// Pass actual shard IDs (not just count) to handle non-0-indexed shards after resharding
	const shardIds = allShards.map((s) => s.shard_id).sort((a, b) => a - b);

	const perShardStatements = groupInsertByShards(
		statement,
		planData.shardKey,
		shardIds,
		params,
	);

	// If we couldn't group rows (shard key not in INSERT), execute on all shards
	const shardsToQuery =
		perShardStatements.size === 0
			? allShards
			: allShards.filter((s) => perShardStatements.has(s.shard_id));

	logger.info`INSERT rows grouped by shard ${{ shardsWithRows: perShardStatements.size }} ${{ totalShards: allShards.length }} ${{
		shardsToQueryLength: shardsToQuery.length,
	}}`;

	// STEP 3: Log write if resharding is in progress
	await logWriteIfResharding(tableName, statement.type, query, params, context);

	// STEP 4: Execute per-shard INSERTs
	// Track per-shard results for row count bumping (declared here for scope)
	const resultsPerShard = new Map<number, QueryResult>();
	let execResult:
		| {
				results:
					| QueryResult[]
					| QueryResult[][]
					| [QueryResult[], QueryResult[]]
					| [QueryResult[], QueryResult[], QueryResult[]];
				shardStats: ShardStats[];
		  }
		| undefined;
	if (perShardStatements.size > 0) {
		// Execute only the shards that have rows
		const shardStatsPerShard: ShardStats[] = [];

		for (const [
			shardId,
			{ statement: shardStatement, params: shardParams },
		] of perShardStatements) {
			const shard = allShards.find((s) => s.shard_id === shardId);
			if (!shard) {
				continue;
			}

			// For per-shard execution, use the filtered and remapped params for this shard
			const queries = prepareIndexMaintenanceQueries(
				planData.virtualIndexes.length > 0,
				shardStatement,
				undefined,
				shardParams,
			);

			const result = await executeOnShards(context, [shard], queries);
			const shardResult = (result.results as QueryResult[])[0] || {
				rows: [],
				rowsAffected: 0,
			};
			resultsPerShard.set(shardId, shardResult);
			if (result.shardStats) {
				shardStatsPerShard.push(...result.shardStats);
			}
		}

		execResult = {
			results: Array.from(resultsPerShard.values()),
			shardStats: shardStatsPerShard,
		};
	} else {
		// Fallback: execute on all shards if we can't determine distribution
		logger.warn`Could not determine shard distribution, executing on all shards ${{ tableName }}`;
		const queries = prepareIndexMaintenanceQueries(
			planData.virtualIndexes.length > 0,
			statement,
			undefined,
			params,
		);
		execResult = await executeOnShards(context, shardsToQuery, queries);
	}

	logger.info`Shard execution completed for INSERT ${{ shardsQueried: shardsToQuery.length }}`;

	// STEP 5: Synchronous index maintenance
	// For each shard that received rows, update the index entries
	if (planData.virtualIndexes.length > 0 && perShardStatements.size > 0) {
		const { databaseId, topology } = context;
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);

		for (const [
			shardId,
			{ statement: shardStatement, params: shardParams },
		] of perShardStatements) {
			// Extract the indexed column values from the INSERT statement
			for (const index of planData.virtualIndexes) {
				const indexColumns = JSON.parse(index.columns) as string[];

				// For each row in the INSERT
				for (const row of shardStatement.values) {
					const values: SqlParam[] = [];
					let hasNull = false;

					for (const colName of indexColumns) {
						const columnIndex =
							shardStatement.columns?.findIndex(
								(col: Identifier) => col.name === colName,
							) ?? -1;
						if (columnIndex === -1) {
							hasNull = true;
							break;
						}

						const valueExpr = row[columnIndex];
						let value: SqlParam = null;
						if (valueExpr?.type === "Literal") {
							value = (valueExpr as Literal).value as SqlParam;
						} else if (valueExpr?.type === "Placeholder") {
							value = shardParams[(valueExpr as Placeholder).parameterIndex]!;
						}

						if (value === null || value === undefined) {
							hasNull = true;
							break;
						}
						values.push(value);
					}

					if (!hasNull && values.length === indexColumns.length) {
						const keyValue =
							indexColumns.length === 1
								? String(values[0])
								: JSON.stringify(values);
						await topologyStub.addShardToIndexEntry(
							index.index_name,
							keyValue,
							shardId,
						);
					}
				}
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
	const results = execResult.results as QueryResult[];
	const result = mergeResultsSimple(results, statement);

	// Add shard statistics
	result.shardStats = execResult.shardStats;

	// STEP 8: Bump row counts for each shard
	// Use VALUES clause length (logical rows), not rowsAffected from SQLite
	// SQLite's rowsWritten includes B-tree writes for indexes (e.g., composite PK),
	// which would inflate counts by 2x for tables with indexes
	const { databaseId, topology } = context;
	const topologyId = topology.idFromName(databaseId);
	const topologyStub = topology.get(topologyId);

	if (perShardStatements.size > 0) {
		// Build delta map from VALUES clause length per shard
		const deltaByShard = new Map<number, number>();
		for (const [shardId, { statement: shardStatement }] of perShardStatements) {
			const rowsInserted = shardStatement.values?.length ?? 0;
			if (rowsInserted > 0) {
				deltaByShard.set(shardId, rowsInserted);
			}
		}

		if (deltaByShard.size > 0) {
			await topologyStub.batchBumpTableShardRowCounts(tableName, deltaByShard);
		}
	} else {
		// Fallback case: shard key not in INSERT, rows were inserted on all shards
		// Each shard gets all the rows from the original statement
		const rowsInserted = statement.values?.length ?? 0;
		if (rowsInserted > 0) {
			const deltaByShard = new Map<number, number>();
			for (const shard of shardsToQuery) {
				deltaByShard.set(shard.shard_id, rowsInserted);
			}
			await topologyStub.batchBumpTableShardRowCounts(tableName, deltaByShard);
		}
	}

	logger.info`INSERT query completed ${{ shardsQueried: shardsToQuery.length }} ${{ rowsAffected: result.rowsAffected }}`;

	return result;
}
