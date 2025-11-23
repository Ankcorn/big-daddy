import type { SelectStatement, InsertStatement, UpdateStatement, DeleteStatement } from '@databases/sqlite-ast';
import { generate } from '@databases/sqlite-ast';
import { Effect, Data } from 'effect';
import { logger } from '../../../logger';
import type { QueryResult, ShardStats, QueryHandlerContext, ShardInfo } from '../types';
import type { IndexMaintenanceJob, IndexJob } from '../../queue/types';
import { injectVirtualShard } from './helpers';
import { extractKeyValueFromRow } from './utils';
import type { QueryPlanData } from '../../topology';

/**
 * Error type for shard query execution failures
 */
class ShardQueryExecutionError extends Data.TaggedError('ShardQueryExecutionError')<{
	readonly shardId: number;
	readonly nodeId: string;
	readonly statementType: string;
	readonly originalError: Error;
}> {}

/**
 * Prepared queries for a single shard
 */
interface ShardQueries {
	shard: ShardInfo;
	queries: Array<{ query: string; params: any[] }>;
}

/**
 * Result from executing queries on a shard
 */
interface ShardExecutionResult {
	shard: ShardInfo;
	results: QueryResult | QueryResult[];
	stats: ShardStats;
	startTime: number;
}

/**
 * Statement with its associated parameters
 */
export interface StatementWithParams {
	statement: SelectStatement | InsertStatement | UpdateStatement | DeleteStatement;
	params: any[];
}

/**
 * Prepare queries for a shard by injecting virtual shard filters
 * This is a pure function that transforms statements into executable queries
 *
 * Supports two modes:
 * 1. Legacy: statements array + single params array (all statements share params)
 * 2. New: statementsWithParams array (each statement has its own params)
 */
function prepareShardQueries(
	shard: ShardInfo,
	statementsWithParams: StatementWithParams[],
): ShardQueries {
	const queries = statementsWithParams.map(({ statement, params }) => {
		const { modifiedStatement, modifiedParams } = injectVirtualShard(statement, params, shard.shard_id);
		return {
			query: generate(modifiedStatement),
			params: modifiedParams,
		};
	});

	return { shard, queries };
}

/**
 * Transform raw storage results into structured QueryResults
 * Pure function for data transformation
 */
function transformStorageResults(rawResults: any, isMultipleStatements: boolean): QueryResult[] {
	const rawResultsArray = isMultipleStatements ? (rawResults as any).results || rawResults : [rawResults];

	return rawResultsArray.map((result: any) => ({
		rows: result.rows || [],
		rowsAffected: result.rowsAffected ?? 0,
	}));
}

/**
 * Execute prepared queries on a single shard
 * Returns an Effect that handles the async storage call
 */
function executeSingleShardQueries(
	context: QueryHandlerContext,
	shardQueries: ShardQueries,
	isMultipleStatements: boolean,
	statementType: string,
): Effect.Effect<ShardExecutionResult, ShardQueryExecutionError> {
	const { storage } = context;
	const { shard, queries } = shardQueries;
	const shardStartTime = Date.now();
	const storageId = storage.idFromName(shard.node_id);
	const storageStub = storage.get(storageId);

	// Wrap the async call and transform results
	const executeQuery: Effect.Effect<any, ShardQueryExecutionError> = Effect.promise(
		() => storageStub.executeQuery(isMultipleStatements ? queries : queries[0]!) as any,
	).pipe(
		Effect.mapError(
			(error: unknown) =>
				new ShardQueryExecutionError({
					shardId: shard.shard_id,
					nodeId: shard.node_id,
					statementType,
					originalError: error instanceof Error ? error : new Error(String(error)),
				}),
		),
	);

	return executeQuery.pipe(
		Effect.andThen((rawResults) => {
			const statementResults = transformStorageResults(rawResults, isMultipleStatements);
			const shardDuration = Date.now() - shardStartTime;

			// Use stats from the last statement (typically the write operation)
			const lastResult = statementResults[statementResults.length - 1];
			const stats: ShardStats = {
				shardId: shard.shard_id,
				nodeId: shard.node_id,
				rowsReturned: lastResult?.rows?.length ?? 0,
				rowsAffected: lastResult?.rowsAffected ?? 0,
				duration: shardDuration,
			};

			const result: ShardExecutionResult = {
				shard,
				results: isMultipleStatements ? statementResults : statementResults[0]!,
				stats,
				startTime: shardStartTime,
			};

			return Effect.succeed(result);
		}),
		Effect.tapError(() => {
			logger.error(`${statementType} shard query failed`, {
				shardId: shard.shard_id,
			});
			return Effect.void;
		}),
	);
}

/**
 * Execute query/queries (SELECT/INSERT/UPDATE/DELETE) on shards in parallel batches
 * This function uses Effect for composing async operations with proper error handling
 *
 * Accepts statements+params in either format:
 * 1. Legacy: statements + shared params
 * 2. New: StatementWithParams[] (each statement has its own params)
 *
 * Implementation details:
 * 1. Processes shards in batches of 7 to avoid overwhelming the system
 * 2. Injects _virtualShard filter for shard-specific isolation
 * 3. Executes all statements in sequence per shard
 * 4. Tracks execution statistics for each shard
 * 5. Returns merged results and shard performance data
 */

// Overload 1: Single statement + shared params
function executeOnShardsEffect(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statement: SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
	params: any[],
): Effect.Effect<{ results: QueryResult[]; shardStats: ShardStats[] }, ShardQueryExecutionError>;

// Overload 2: Two statements + shared params
function executeOnShardsEffect(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statement: readonly [
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
	],
	params: any[],
): Effect.Effect<{ results: [QueryResult[], QueryResult[]]; shardStats: ShardStats[] }, ShardQueryExecutionError>;

// Overload 3: Three statements + shared params
function executeOnShardsEffect(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statement: readonly [
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
	],
	params: any[],
): Effect.Effect<
	{ results: [QueryResult[], QueryResult[], QueryResult[]]; shardStats: ShardStats[] },
	ShardQueryExecutionError
>;

// Overload 4: General array + shared params
function executeOnShardsEffect(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statement: readonly (SelectStatement | InsertStatement | UpdateStatement | DeleteStatement)[],
	params: any[],
): Effect.Effect<
	{ results: QueryResult[] | QueryResult[][] | [QueryResult[], QueryResult[]] | [QueryResult[], QueryResult[], QueryResult[]]; shardStats: ShardStats[] },
	ShardQueryExecutionError
>;

// Overload 5: StatementWithParams array (new format)
function executeOnShardsEffect(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statementsWithParams: readonly StatementWithParams[],
): Effect.Effect<
	{ results: QueryResult[] | QueryResult[][]; shardStats: ShardStats[] },
	ShardQueryExecutionError
>;

// Implementation
function executeOnShardsEffect(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statementsInput:
		| SelectStatement
		| InsertStatement
		| UpdateStatement
		| DeleteStatement
		| readonly (SelectStatement | InsertStatement | UpdateStatement | DeleteStatement)[]
		| readonly StatementWithParams[],
	params?: any[],
): Effect.Effect<
	{ results: QueryResult[] | QueryResult[][] | [QueryResult[], QueryResult[]] | [QueryResult[], QueryResult[], QueryResult[]]; shardStats: ShardStats[] },
	ShardQueryExecutionError
> {
	const BATCH_SIZE = 7;

	// Detect format and normalize to StatementWithParams[]
	const isNewFormat = Array.isArray(statementsInput) && statementsInput.length > 0 && 'statement' in statementsInput[0];
	let statementsWithParams: StatementWithParams[];
	let isMultipleStatements: boolean;
	let statementType: string;

	if (isNewFormat) {
		// New format: already have StatementWithParams[]
		statementsWithParams = statementsInput as StatementWithParams[];
		isMultipleStatements = statementsWithParams.length > 1;
		statementType = statementsWithParams[0]!.statement.type;
	} else {
		// Legacy format: statements + shared params
		const statements = Array.isArray(statementsInput) ? statementsInput : [statementsInput];
		isMultipleStatements = statements.length > 1;
		statementType = statements[0]!.type;

		// Convert to StatementWithParams format: each statement paired with params
		statementsWithParams = statements.map((stmt) => ({
			statement: stmt as SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
			params: params || [],
		}));
	}

	// Prepare queries for all shards (pure transformation)
	const allPreparedQueries = shardsToQuery.map((shard) => prepareShardQueries(shard, statementsWithParams));

	// Split into batches
	const batches = Array.from({ length: Math.ceil(allPreparedQueries.length / BATCH_SIZE) }, (_, i) =>
		allPreparedQueries.slice(i * BATCH_SIZE, (i + 1) * BATCH_SIZE),
	);

	// Execute all batches sequentially using Effect
	return Effect.gen(function* () {
		const allResults: (QueryResult | QueryResult[])[] = [];
		const allShardStats: ShardStats[] = [];

		// Process each batch sequentially
		for (const batch of batches) {
			// Execute all shards in a batch in parallel
			const batchResults = yield* Effect.all(
				batch.map((shardQueries) => executeSingleShardQueries(context, shardQueries, isMultipleStatements, statementType)),
				// Use concurrency mode for parallel execution within batch
				{ concurrency: 'unbounded' },
			);

			// Collect results from this batch
			for (const result of batchResults) {
				allResults.push(result.results);
				allShardStats.push(result.stats);
			}
		}

		// Return the final aggregated results
		return {
			results: isMultipleStatements ? (allResults as [QueryResult, QueryResult]) : (allResults as QueryResult[]),
			shardStats: allShardStats,
		};
	}).pipe(
		// Log any errors
		Effect.catchTag('ShardQueryExecutionError', (error) => Effect.fail(error)),
	);
}

/**
 * Wrapper for executeOnShardsEffect
 * Executes the Effect and returns a Promise for compatibility with existing code
 *
 * Type-safe overloads support two calling styles:
 *
 * Style 1: Statement(s) + params (legacy, backward compatible)
 * - executeOnShards(context, shards, statement, params)
 * - executeOnShards(context, shards, [stmt1, stmt2], params)
 *
 * Style 2: Statement+params pairs (new, for different params per statement)
 * - executeOnShards(context, shards, [{ statement, params }])
 * - executeOnShards(context, shards, [{ statement: s1, params: p1 }, { statement: s2, params: p2 }])
 */

// NEW: Overload for single statement+params pair
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statementsWithParams: readonly [StatementWithParams],
): Promise<{ results: QueryResult[]; shardStats: ShardStats[] }>;

// NEW: Overload for two statement+params pairs
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statementsWithParams: readonly [StatementWithParams, StatementWithParams],
): Promise<{ results: [QueryResult[], QueryResult[]]; shardStats: ShardStats[] }>;

// NEW: Overload for three statement+params pairs
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statementsWithParams: readonly [StatementWithParams, StatementWithParams, StatementWithParams],
): Promise<{ results: [QueryResult[], QueryResult[], QueryResult[]]; shardStats: ShardStats[] }>;

// NEW: Overload for general array of statement+params pairs
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statementsWithParams: readonly StatementWithParams[],
): Promise<{ results: QueryResult[] | QueryResult[][] | [QueryResult[], QueryResult[]] | [QueryResult[], QueryResult[], QueryResult[]]; shardStats: ShardStats[] }>;

// LEGACY: Overload 1: Single statement + shared params
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statement: SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
	params: any[],
): Promise<{ results: QueryResult[]; shardStats: ShardStats[] }>;

// Overload 2: Two statements
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statement: readonly [
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
	],
	params: any[],
): Promise<{ results: [QueryResult[], QueryResult[]]; shardStats: ShardStats[] }>;

// Overload 3: Three statements
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statement: readonly [
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
		SelectStatement | InsertStatement | UpdateStatement | DeleteStatement,
	],
	params: any[],
): Promise<{ results: [QueryResult[], QueryResult[], QueryResult[]]; shardStats: ShardStats[] }>;

// Overload 4: General array (catch-all for other cases)
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statement: readonly (SelectStatement | InsertStatement | UpdateStatement | DeleteStatement)[],
	params: any[],
): Promise<{ results: QueryResult[] | QueryResult[][] | [QueryResult[], QueryResult[]] | [QueryResult[], QueryResult[], QueryResult[]]; shardStats: ShardStats[] }>;

// Implementation
export async function executeOnShards(
	context: QueryHandlerContext,
	shardsToQuery: ShardInfo[],
	statementsInput:
		| SelectStatement
		| InsertStatement
		| UpdateStatement
		| DeleteStatement
		| readonly (SelectStatement | InsertStatement | UpdateStatement | DeleteStatement)[]
		| readonly StatementWithParams[],
	params?: any[],
): Promise<{ results: QueryResult[] | QueryResult[][] | [QueryResult[], QueryResult[]] | [QueryResult[], QueryResult[], QueryResult[]]; shardStats: ShardStats[] }> {
	// Pass through to executeOnShardsEffect, which handles both formats
	const effect = executeOnShardsEffect(context, shardsToQuery, statementsInput as any, params);
	return Effect.runPromise(effect);
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
): Promise<{ planData: QueryPlanData; cacheHit: boolean }> {
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
