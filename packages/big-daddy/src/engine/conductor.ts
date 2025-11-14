import { parse, generate } from '@databases/sqlite-ast';
import type {
	Statement,
	SelectStatement,
	InsertStatement,
	UpdateStatement,
	DeleteStatement,
	CreateTableStatement,
	CreateIndexStatement,
	PragmaStatement,
	BinaryExpression,
} from '@databases/sqlite-ast';
import { withLogTags } from 'workers-tagged-logger';
import { logger } from '../logger';
import type { Storage, QueryResult as StorageQueryResult, QueryType } from './storage';
import type { Topology, TableMetadata, QueryPlanData } from './topology/index';
import { extractTableName, getQueryType, buildQuery } from './utils/ast-utils';
import { extractTableMetadata } from './utils/schema-utils';
import type { IndexJob, IndexMaintenanceJob, ReshardTableJob } from './queue/types';
import { TopologyCache, type CacheStats } from './utils/topology-cache';

/**
 * Cache statistics for a query
 */
export interface QueryCacheStats {
	cacheHit: boolean; // Whether this specific query was a cache hit
	totalHits: number; // Total cache hits for this conductor instance
	totalMisses: number; // Total cache misses for this conductor instance
	cacheSize: number; // Number of entries currently in cache
}

/**
 * Statistics for a single shard query
 */
export interface ShardStats {
	shardId: number;
	nodeId: string;
	rowsReturned: number;
	rowsAffected?: number;
	duration: number; // in milliseconds
}

/**
 * Result from a SQL query execution
 */
export interface QueryResult {
	rows: Record<string, any>[];
	rowsAffected?: number;
	cacheStats?: QueryCacheStats; // Cache statistics (only for SELECT queries)
	shardStats?: ShardStats[]; // Statistics for each shard queried
}

/**
 * Conductor - Routes SQL queries to the appropriate storage shards
 *
 * The Conductor sits between the client and the distributed storage layer,
 * parsing queries, determining which shards to target, and coordinating
 * execution across multiple storage nodes.
 */
export class ConductorClient {
	private cache: TopologyCache;
	private correlationId: string;

	constructor(
		private databaseId: string,
		correlationId: string,
		private storage: DurableObjectNamespace<Storage>,
		private topology: DurableObjectNamespace<Topology>,
		private indexQueue?: Queue,
		private env?: Env, // For test environment queue processing
	) {
		this.cache = new TopologyCache();
		this.correlationId = correlationId;
	}

	/**
	 * Get cache statistics (for testing and monitoring)
	 */
	getCacheStats() {
		return this.cache.getStats();
	}

	/**
	 * Clear the topology cache (for testing)
	 */
	clearCache() {
		this.cache.clear();
	}

	/**
	 * Execute a SQL query using tagged template literals
	 *
	 * @example
	 * const result = await conductor.sql`SELECT * FROM users WHERE id = ${userId}`;
	 * await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`;
	 */
	sql = async (strings: TemplateStringsArray, ...values: any[]): Promise<QueryResult> => {
		return withLogTags({ source: 'Conductor' }, async () => {
			const startTime = Date.now();
			const cid = this.correlationId;

			logger.setTags({
				correlationId: cid,
				requestId: cid,
				component: 'Conductor',
				operation: 'sql',
				databaseId: this.databaseId,
			});

			logger.debug('Starting SQL query execution');

			// STEP 1: Parse - Build and parse the SQL query
			const { query, params } = buildQuery(strings, values);
			const statement = parse(query);

			logger.debug('Query parsed', {
				queryType: statement.type,
			});

			// Handle CREATE TABLE statements (special case - affects all nodes)
			if (statement.type === 'CreateTableStatement') {
				logger.info('Handling CREATE TABLE', {
					table: (statement as CreateTableStatement).table.name,
				});
				const result = await this.handleCreateTable(statement as CreateTableStatement, query, cid);
				const duration = Date.now() - startTime;
				logger.info('CREATE TABLE completed', {
					duration,
					status: 'success',
				});
				return result;
			}

			// Handle CREATE INDEX statements (special case - creates virtual index)
			if (statement.type === 'CreateIndexStatement') {
				logger.info('Handling CREATE INDEX', {
					indexName: (statement as CreateIndexStatement).name.name,
					table: (statement as CreateIndexStatement).table.name,
				});
				const result = await this.handleCreateIndex(statement as CreateIndexStatement, cid);
				const duration = Date.now() - startTime;
				logger.info('CREATE INDEX completed', {
					duration,
					status: 'success',
				});
				return result;
			}

			// Handle PRAGMA statements (special case - controls topology behavior)
			if (statement.type === 'PragmaStatement') {
				logger.info('Handling PRAGMA', {
					pragmaName: (statement as PragmaStatement).name,
				});
				const result = await this.handlePragma(statement as PragmaStatement, query, cid);
				const duration = Date.now() - startTime;
				logger.info('PRAGMA completed', {
					duration,
					status: 'success',
				});
				return result;
			}

			// STEP 2: Route - Get ALL topology data (with caching)
			const tableName = extractTableName(statement);
			if (!tableName) {
				logger.error('Failed to extract table name from query', {
					queryType: statement.type,
				});
				throw new Error('Could not determine table name from query');
			}

			logger.setTags({ table: tableName });

			// **CACHED TOPOLOGY CALL** - Get all topology data AND determine shard targets
			// All planning logic (including index usage) happens inside the Topology DO
			// Cache eliminates this call for repeated queries with same predicates
			const { planData, cacheHit } = await this.getCachedQueryPlanData(tableName, statement, params, cid);

			logger.info('Query plan determined', {
				cacheHit,
				shardsSelected: planData.shardsToQuery.length,
				indexesUsed: planData.virtualIndexes.length,
			});

			// Shard targets are already determined by Topology!
			const shardsToQuery = planData.shardsToQuery;

			// STEP 2.5: Write Logging - Log writes during resharding (Phase 3A)
			// If a table is being resharded, log all INSERT/UPDATE/DELETE operations to the queue
			// These will be replayed to new shards after the copy phase completes
			if (statement.type === 'InsertStatement' || statement.type === 'UpdateStatement' || statement.type === 'DeleteStatement') {
				await this.logWriteIfResharding(tableName, statement.type, query, params, cid);
			}

			// STEP 3: Execute - Run query on all target shards in parallel
			const queryType = getQueryType(statement);
			const { results, shardStats } = await this.executeOnShards(shardsToQuery, query, params, queryType, cid);

			logger.info('Shard execution completed', {
				shardsQueried: shardsToQuery.length,
			});

			// STEP 3.5: Index Maintenance - Enqueue async maintenance for UPDATE/DELETE
			// INSERT: Already handled in getQueryPlanData() (before query execution)
			// UPDATE/DELETE: Enqueue to queue for async processing (no blocking!)
			if ((statement.type === 'UpdateStatement' || statement.type === 'DeleteStatement') && planData.virtualIndexes.length > 0) {
				await this.enqueueIndexMaintenanceJob(
					tableName,
					statement,
					shardsToQuery.map((s) => s.shard_id),
					planData.virtualIndexes,
					cid,
				);
				logger.debug('Index maintenance job enqueued', {
					indexCount: planData.virtualIndexes.length,
				});
			}

			// STEP 3.6: Cache Invalidation - Invalidate cache for write operations
			// This ensures the cache stays fresh after data modifications
			if (statement.type === 'InsertStatement' || statement.type === 'UpdateStatement' || statement.type === 'DeleteStatement') {
				this.invalidateCacheForWrite(tableName, statement, planData.virtualIndexes, params);
				logger.debug('Cache invalidated for write operation');
			}

			// STEP 4: Merge - Combine results from all shards
			const result = this.mergeResults(results, statement);

			// STEP 5: Add cache statistics to result (for SELECT queries)
			if (statement.type === 'SelectStatement') {
				const stats = this.cache.getStats();
				result.cacheStats = {
					cacheHit,
					totalHits: stats.hits,
					totalMisses: stats.misses,
					cacheSize: stats.size,
				};
			}

			// STEP 6: Add shard statistics to result (for all queries)
			result.shardStats = shardStats;

			const duration = Date.now() - startTime;
			logger.info('SQL query execution completed', {
				duration,
				queryType: statement.type,
				rowCount: result.rows.length,
				rowsAffected: result.rowsAffected,
				status: 'success',
			});

			return result;
		});
	};

	/**
	 * Handle CREATE TABLE statement execution
	 */
	private async handleCreateTable(statement: CreateTableStatement, query: string, correlationId?: string): Promise<QueryResult> {
		const tableName = statement.table.name;

		// Get topology stub
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Check if table already exists in topology
		const existingTable = topologyData.tables.find((t) => t.table_name === tableName);

		// If IF NOT EXISTS is specified and table exists, skip
		if (statement.ifNotExists && existingTable) {
			return {
				rows: [],
				rowsAffected: 0,
			};
		}

		// If table exists and IF NOT EXISTS was not specified, throw error
		if (existingTable) {
			throw new Error(`Table '${tableName}' already exists in topology`);
		}

		// Extract metadata from the CREATE TABLE statement
		const metadata = extractTableMetadata(statement, query);

		// Add table to topology
		await topologyStub.updateTopology({
			tables: {
				add: [metadata],
			},
		});

		// Inject _virtualShard column into the CREATE TABLE query
		// This hidden column ensures shard isolation at the storage level
		const modifiedQuery = this.injectVirtualShardColumn(query);

		// Execute CREATE TABLE on all storage nodes in parallel (use actual node IDs from topology)
		await Promise.all(
			topologyData.storage_nodes.map(async (node) => {
				const storageId = this.storage.idFromName(node.node_id);
				const storageStub = this.storage.get(storageId);

				await storageStub.executeQuery({
					query: modifiedQuery,
					params: [],
					queryType: 'CREATE',
				});
			}),
		);

		return {
			rows: [],
			rowsAffected: 0,
		};
	}

	/**
	 * Handle CREATE INDEX statement execution
	 *
	 * This method:
	 * 1. Validates the index and table
	 * 2. Creates SQLite indexes on all storage shards (for query performance)
	 * 3. Creates the virtual index definition in Topology with status 'building'
	 * 4. Enqueues an IndexBuildJob to the queue for async processing (builds virtual index metadata)
	 * 5. Returns immediately to the client (non-blocking)
	 */
	private async handleCreateIndex(statement: CreateIndexStatement, correlationId?: string): Promise<QueryResult> {
		const indexName = statement.name.name;
		const tableName = statement.table.name;

		// Extract column names from the CREATE INDEX statement
		const columns = statement.columns.map((col) => col.name);

		// Get topology stub
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);

		// Get all shards for this table to create SQLite indexes
		const { tableShards } = await this.getTableTopologyInfo(tableName);

		// Build the CREATE INDEX SQL statement
		const uniqueClause = statement.unique ? 'UNIQUE ' : '';
		const ifNotExistsClause = statement.ifNotExists ? 'IF NOT EXISTS ' : '';
		const columnList = columns.join(', ');
		const createIndexSQL = `CREATE ${uniqueClause}INDEX ${ifNotExistsClause}${indexName} ON ${tableName}(${columnList})`;

		// Create SQLite indexes on all storage shards in parallel
		// This ensures queries on individual shards are fast (reduces rows scanned)
		await Promise.all(
			tableShards.map(async (shard) => {
				const storageId = this.storage.idFromName(shard.node_id);
				const storageStub = this.storage.get(storageId);

				try {
					await storageStub.executeQuery({
						query: createIndexSQL,
						params: [],
						queryType: 'CREATE',
					});
				} catch (error) {
					// If IF NOT EXISTS is specified, ignore "already exists" errors
					if (statement.ifNotExists && (error as Error).message.includes('already exists')) {
						return;
					}
					throw error;
				}
			}),
		);

		// Create the virtual index in Topology with 'building' status
		// Virtual index provides shard-level routing optimization
		const indexType = statement.unique ? 'unique' : 'hash';
		const result = await topologyStub.createVirtualIndex(indexName, tableName, columns, indexType);

		if (!result.success) {
			// If IF NOT EXISTS is specified and index already exists, return success
			if (statement.ifNotExists && result.error?.includes('already exists')) {
				return {
					rows: [],
					rowsAffected: 0,
				};
			}

			throw new Error(result.error || 'Failed to create index');
		}

		// Enqueue index build job for background processing
		// This populates the virtual index metadata with existing data
		await this.enqueueIndexJob({
			type: 'build_index',
			database_id: this.databaseId,
			table_name: tableName,
			columns: columns,
			index_name: indexName,
			created_at: new Date().toISOString(),
			correlation_id: correlationId,
		});

		return {
			rows: [],
			rowsAffected: 0,
		};
	}

	/**
	 * Handle PRAGMA statements
	 */
	private async handlePragma(statement: PragmaStatement, query: string, cid: string): Promise<QueryResult> {
		const pragmaName = statement.name.toLowerCase();

		if (pragmaName === 'reshardtable') {
			return this.handleReshardTable(statement, cid);
		}

		throw new Error(`Unsupported PRAGMA: ${statement.name}`);
	}

	/**
	 * Handle PRAGMA reshardTable(table_name, shard_count)
	 * Initiates a resharding operation for a table
	 */
	private async handleReshardTable(stmt: PragmaStatement, cid: string): Promise<QueryResult> {
		if (!stmt.arguments || stmt.arguments.length !== 2) {
			throw new Error('PRAGMA reshardTable requires exactly 2 arguments: table name and shard count');
		}

		// Extract table name from first argument
		const tableNameArg = stmt.arguments[0];
		let tableName: string;
		if (tableNameArg.type === 'Literal') {
			tableName = (tableNameArg as any).value as string;
		} else if (tableNameArg.type === 'Identifier') {
			tableName = (tableNameArg as any).name as string;
		} else {
			throw new Error('First argument to PRAGMA reshardTable must be a table name');
		}

		// Extract shard count from second argument
		const shardCountArg = stmt.arguments[1];
		let shardCount: number;
		if (shardCountArg.type === 'Literal') {
			shardCount = (shardCountArg as any).value as number;
		} else if (shardCountArg.type === 'Identifier') {
			shardCount = parseInt((shardCountArg as any).name as string, 10);
		} else {
			throw new Error('Second argument to PRAGMA reshardTable must be a number');
		}

		if (!Number.isInteger(shardCount) || shardCount < 1 || shardCount > 256) {
			throw new Error(`Invalid shard count: ${shardCount}. Must be an integer between 1 and 256`);
		}

		// Get topology stub
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);

		// Phase 1: Create pending shards and resharding state
		const changeLogId = crypto.randomUUID();
		const newShards = await topologyStub.createPendingShards(tableName, shardCount, changeLogId);

		logger.info('Pending shards created for resharding', {
			table: tableName,
			shardCount: newShards.length,
			changeLogId,
		});

		// Get table metadata for the resharding job
		const topology = await topologyStub.getTopology();
		const tableMetadata = topology.tables.find((t) => t.table_name === tableName);
		if (!tableMetadata) {
			throw new Error(`Table '${tableName}' not found in topology`);
		}

		// Phase 2: Enqueue ReshardTableJob for each active source shard
		// When resharding from multiple shards to a different number, we need to handle each source shard
		const sourceShardIds = topology.table_shards
			.filter((s) => s.table_name === tableName && s.status === 'active')
			.sort((a, b) => a.shard_id - b.shard_id)
			.map((s) => s.shard_id);

		if (sourceShardIds.length === 0) {
			throw new Error(`No active shards found for table '${tableName}'`);
		}

		// Enqueue a resharding job for each source shard
		for (const sourceShardId of sourceShardIds) {
			const jobId = crypto.randomUUID();

			await this.enqueueIndexJob({
				type: 'reshard_table',
				database_id: this.databaseId,
				table_name: tableName,
				source_shard_id: sourceShardId,
				target_shard_ids: newShards.map((s) => s.shard_id),
				shard_key: tableMetadata.shard_key,
				shard_strategy: tableMetadata.shard_strategy,
				change_log_id: changeLogId,
				created_at: new Date().toISOString(),
				correlation_id: cid,
			} as ReshardTableJob);

			logger.info('Resharding job enqueued for source shard', {
				table: tableName,
				jobId,
				sourceShardId,
				targetShardIds: newShards.map((s) => s.shard_id),
			});
		}

		logger.info('All resharding jobs enqueued', {
			table: tableName,
			sourceShardCount: sourceShardIds.length,
			targetShardCount: newShards.length,
		});

		return {
			rows: [
				{
					change_log_id: changeLogId,
					status: 'queued',
					message: `Resharding ${tableName} from ${sourceShardIds.length} shards to ${shardCount} shards`,
					table_name: tableName,
					source_shard_count: sourceShardIds.length,
					target_shard_count: shardCount,
				},
			],
		};
	}

	/**
	 * Fetch topology information for a table
	 */
	private async getTableTopologyInfo(tableName: string): Promise<{
		tableMetadata: TableMetadata;
		tableShards: Array<{ table_name: string; shard_id: number; node_id: string }>;
	}> {
		// Get topology information
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find the table metadata
		const tableMetadata = topologyData.tables.find((t) => t.table_name === tableName);
		if (!tableMetadata) {
			throw new Error(`Table '${tableName}' not found in topology`);
		}

		// Get table shards for this table
		const tableShards = topologyData.table_shards.filter((s) => s.table_name === tableName);

		if (tableShards.length === 0) {
			throw new Error(`No shards found for table '${tableName}'`);
		}

		return { tableMetadata, tableShards };
	}

	/**
	 * Get query plan data with caching
	 *
	 * This method checks the cache first before calling the Topology DO.
	 * For cacheable queries (SELECT with WHERE clause), this can eliminate
	 * the Topology DO call entirely.
	 *
	 * @returns Query plan data and whether it was a cache hit
	 */
	private async getCachedQueryPlanData(tableName: string, statement: Statement, params: any[], correlationId?: string): Promise<{ planData: QueryPlanData; cacheHit: boolean }> {
		// Try to build a cache key for this query
		const cacheKey = this.cache.buildQueryPlanCacheKey(tableName, statement, params);

		// Check cache if we have a valid cache key
		if (cacheKey) {
			const cached = this.cache.getQueryPlanData(cacheKey);
			if (cached) {
				logger.debug('Query plan cache hit', { cacheKey });
				return { planData: cached, cacheHit: true };
			}
		}

		logger.debug('Query plan cache miss, fetching from Topology', { cacheKey });

		// Cache miss - fetch from Topology DO
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const planData = await topologyStub.getQueryPlanData(tableName, statement, params, correlationId);

		// Store in cache if cacheable
		if (cacheKey) {
			const ttl = this.cache.getTTLForQuery(statement);
			this.cache.setQueryPlanData(cacheKey, planData, ttl);
			logger.debug('Query plan cached', { cacheKey, ttl });
		}

		return { planData, cacheHit: false };
	}

	/**
	 * Invalidate cache entries for write operations
	 *
	 * This method invalidates the appropriate cache entries based on the write operation:
	 * - INSERT: Invalidate query plan cache + index caches for inserted values
	 * - UPDATE: Invalidate query plan cache + index caches for updated values
	 * - DELETE: Invalidate query plan cache + index caches for deleted values
	 */
	private invalidateCacheForWrite(
		tableName: string,
		statement: InsertStatement | UpdateStatement | DeleteStatement,
		virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
		params: any[],
	): void {
		// Always invalidate the query plan cache for this table
		// This ensures queries see fresh data distribution
		this.cache.invalidateTable(tableName);

		// Invalidate index caches for affected index values
		if (virtualIndexes.length === 0) {
			return; // No indexes to invalidate
		}

		if (statement.type === 'InsertStatement') {
			this.invalidateIndexCacheForInsert(statement, virtualIndexes, params);
		} else if (statement.type === 'UpdateStatement') {
			this.invalidateIndexCacheForUpdate(statement, virtualIndexes, params);
		} else if (statement.type === 'DeleteStatement') {
			this.invalidateIndexCacheForDelete(statement, virtualIndexes, params);
		}
	}

	/**
	 * Invalidate index cache entries for INSERT operation
	 */
	private invalidateIndexCacheForInsert(
		statement: InsertStatement,
		virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
		params: any[],
	): void {
		if (!statement.columns || statement.values.length === 0) {
			return;
		}

		const columns = statement.columns; // Narrow the type
		const row = statement.values[0]!; // Only handle single-row inserts for now

		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns);
			const keyValue = this.extractKeyValueFromRow(columns, row, indexColumns, params);
			if (keyValue) {
				this.cache.invalidateIndexKeys(index.index_name, [keyValue]);
			}
		}
	}

	/**
	 * Invalidate index cache entries for UPDATE operation
	 */
	private invalidateIndexCacheForUpdate(
		statement: UpdateStatement,
		virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
		params: any[],
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
			this.cache.invalidateIndex(index.index_name);
		}
	}

	/**
	 * Invalidate index cache entries for DELETE operation
	 */
	private invalidateIndexCacheForDelete(
		statement: DeleteStatement,
		virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
		params: any[],
	): void {
		// For DELETE, we can't easily determine which values are being deleted without querying
		// So we invalidate the entire index cache for all indexes
		for (const index of virtualIndexes) {
			this.cache.invalidateIndex(index.index_name);
		}
	}

	/**
	 * Extract key value from a row for index invalidation
	 */
	private extractKeyValueFromRow(
		columns: Array<{ name: string }>,
		row: any[],
		indexColumns: string[],
		params: any[],
	): string | null {
		const values: any[] = [];

		for (const colName of indexColumns) {
			const columnIndex = columns.findIndex((col) => col.name === colName);
			if (columnIndex === -1) {
				return null; // Column not in INSERT
			}

			const valueExpression = row[columnIndex];
			const value = this.extractValueFromExpression(valueExpression, params);

			if (value === null || value === undefined) {
				return null; // NULL values are not indexed
			}

			values.push(value);
		}

		if (values.length !== indexColumns.length) {
			return null;
		}

		// Build the key value (same format as topology uses)
		return indexColumns.length === 1 ? String(values[0]) : JSON.stringify(values);
	}

	/**
	 * Extract value from an expression node (for cache invalidation)
	 */
	private extractValueFromExpression(expression: any, params: any[]): any {
		if (!expression) {
			return null;
		}
		if (expression.type === 'Literal') {
			return expression.value;
		} else if (expression.type === 'Placeholder') {
			return params[expression.parameterIndex];
		}
		return null;
	}

	/**
	 * Execute a query on multiple shards in parallel, with batching
	 *
	 * Cloudflare has a limit of 6 subrequests in parallel (we use 7 to be safe with the limit).
	 * This method batches shard queries into groups of 7 to respect this constraint.
	 *
	 * Returns both the results and metadata about which shards were queried and timing info.
	 */
	private async executeOnShards(
		shardsToQuery: Array<{ table_name: string; shard_id: number; node_id: string }>,
		query: string,
		params: any[],
		queryType: QueryType,
		correlationId?: string,
	): Promise<{ results: QueryResult[]; shardStats: ShardStats[] }> {
		const BATCH_SIZE = 7;
		const allResults: QueryResult[] = [];
		const allShardStats: ShardStats[] = [];

		logger.debug('Executing query on shards', {
			shardCount: shardsToQuery.length,
			batchSize: BATCH_SIZE,
			batchCount: Math.ceil(shardsToQuery.length / BATCH_SIZE),
		});

		// Process shards in batches of 7
		for (let i = 0; i < shardsToQuery.length; i += BATCH_SIZE) {
			const batch = shardsToQuery.slice(i, i + BATCH_SIZE);
			const batchNum = Math.floor(i / BATCH_SIZE) + 1;
			const startTime = Date.now();

			logger.debug('Processing shard batch', {
				batchNumber: batchNum,
				batchSize: batch.length,
			});

			const batchResults = await Promise.all(
				batch.map(async (shard) => {
					const shardStartTime = Date.now();
					// Get the storage stub using the node_id from table_shards mapping
					const storageId = this.storage.idFromName(shard.node_id);
					const storageStub = this.storage.get(storageId);

					try {
						// Inject _virtualShard filter into the query for this specific shard
						// This is critical because during resharding, a physical storage node can have
						// data from multiple virtual shards, so we need to filter at the SQL level
						const { modifiedQuery, modifiedParams } = this.injectVirtualShardFilter(
							query,
							params,
							shard.shard_id,
							shard.table_name,
							queryType,
						);

						// Execute the query (single query always returns StorageQueryResult)
						const rawResult = await storageStub.executeQuery({
							query: modifiedQuery,
							params: modifiedParams,
							queryType,
							correlationId,
						});

						const shardDuration = Date.now() - shardStartTime;

						// Convert StorageQueryResult to QueryResult
						const result = rawResult as unknown as StorageQueryResult;

						// Track shard stats
						const shardStat: ShardStats = {
							shardId: shard.shard_id,
							nodeId: shard.node_id,
							rowsReturned: result.rows.length,
							rowsAffected: result.rowsAffected,
							duration: shardDuration,
						};

						allShardStats.push(shardStat);

						logger.debug('Shard query completed', {
							shardId: shard.shard_id,
							nodeId: shard.node_id,
							duration: shardDuration,
							rowCount: result.rows.length,
						});
						return {
							rows: result.rows,
							rowsAffected: result.rowsAffected,
						};
					} catch (error) {
						logger.error('Shard query failed', {
							shardId: shard.shard_id,
							nodeId: shard.node_id,
							error: error instanceof Error ? error.message : String(error),
							duration: Date.now() - shardStartTime,
						});
						throw error;
					}
				}),
			);

			const batchDuration = Date.now() - startTime;
			logger.debug('Shard batch completed', {
				batchNumber: batchNum,
				duration: batchDuration,
				resultsCount: batchResults.length,
			});

			allResults.push(...batchResults);
		}

		return { results: allResults, shardStats: allShardStats };
	}

	/**
	 * Merge results from multiple shards
	 */
	private mergeResults(results: QueryResult[], statement: Statement): QueryResult {
		if (results.length === 1) {
			return results[0]!;
		}

		const isSelect = statement.type === 'SelectStatement';
		return this.mergeResultsSimple(results, isSelect);
	}

	/**
	 * Enqueue index maintenance job for UPDATE/DELETE operations
	 * This is async - index updates happen in the background
	 */
	private async enqueueIndexMaintenanceJob(
		tableName: string,
		statement: UpdateStatement | DeleteStatement,
		shardIds: number[],
		virtualIndexes: Array<{ index_name: string; columns: string }>,
		correlationId?: string,
	): Promise<void> {
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
			database_id: this.databaseId,
			table_name: tableName,
			operation: statement.type === 'UpdateStatement' ? 'UPDATE' : 'DELETE',
			shard_ids: shardIds,
			affected_indexes: affectedIndexes.map((idx) => idx.index_name),
			updated_columns: updatedColumns,
			created_at: new Date().toISOString(),
			correlation_id: correlationId,
		};

		logger.debug('Enqueuing index maintenance job', {
			operation: job.operation,
			affectedIndexes: job.affected_indexes.length,
			shardCount: shardIds.length,
		});

		await this.enqueueIndexJob(job);
	}

	/**
	 * Enqueue an index job to the queue
	 *
	 * In test environments, this also triggers immediate processing of the queue.
	 * This is necessary because:
	 * 1. Tests need synchronous behavior to verify index maintenance worked
	 * 2. Cloudflare's test environment doesn't automatically process queues
	 * 3. Without this, tests would need to manually call queueHandler after every write
	 *
	 * In production, queue processing happens automatically via Cloudflare's infrastructure.
	 */
	private async logWriteIfResharding(tableName: string, operationType: string, query: string, params: any[], correlationId?: string): Promise<void> {
		// Get the current resharding state for this table
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const reshardingState = await topologyStub.getReshardingState(tableName);

		// Only log if resharding is active and in copying phase
		if (!reshardingState || reshardingState.status !== 'copying') {
			return;
		}

		// Log the write operation to the queue for later replay
		const operation = (operationType === 'InsertStatement' ? 'INSERT' :
		                  operationType === 'UpdateStatement' ? 'UPDATE' : 'DELETE') as 'INSERT' | 'UPDATE' | 'DELETE';

		const changeLogEntry = {
			type: 'resharding_change_log' as const,
			resharding_id: reshardingState.change_log_id,
			database_id: this.databaseId,
			table_name: tableName,
			operation,
			query,
			params,
			timestamp: Date.now(),
			correlation_id: correlationId,
		};

		try {
			await this.enqueueIndexJob(changeLogEntry);
			logger.debug('Write operation logged for resharding', {
				table: tableName,
				operation,
				reshardingId: reshardingState.change_log_id,
			});
		} catch (error) {
			// Log but don't fail - write logging should not block query execution
			logger.warn('Failed to log write during resharding', {
				table: tableName,
				operation,
				error: (error as Error).message,
			});
		}
	}

	private async enqueueIndexJob(job: IndexJob): Promise<void> {
		if (!this.indexQueue) {
			console.warn('INDEX_QUEUE not available, skipping index job:', job.type);
			return;
		}

		try {
			await this.indexQueue.send(job);
			console.log(`Enqueued ${job.type} job for ${job.table_name}`);

			// In test environment, trigger queue processing immediately
			// This is detected by checking if env was provided (only happens in tests)
			if (this.env) {
				// Dynamically import queueHandler to avoid circular dependencies
				const { queueHandler } = await import('../queue-consumer');

				// Simulate queue batch with this single message
				const jobCorrelationId = job.correlation_id || crypto.randomUUID();
				await queueHandler(
					{
						queue: 'vitess-index-jobs',
						messages: [
							{
								id: `test-${Date.now()}-${Math.random()}`,
								timestamp: new Date(),
								body: job,
								attempts: 1,
							},
						],
					},
					this.env,
					jobCorrelationId,
				);
			}
		} catch (error) {
			console.error(`Failed to enqueue index job:`, error);
			// Don't throw - index operations should not block queries
		}
	}

	/**
	 * Merge results from multiple shards (simple version)
	 */
	private mergeResultsSimple(results: QueryResult[], isSelect: boolean): QueryResult {
		if (isSelect) {
			// Merge rows from all shards
			const mergedRows = results.flatMap((r) => r.rows);

			// Strip _virtualShard from result rows (hidden column should not be visible to user)
			const cleanedRows = mergedRows.map((row) => {
				const cleaned = { ...row };
				delete (cleaned as any)._virtualShard;
				return cleaned;
			});

			return {
				rows: cleanedRows,
				rowsAffected: cleanedRows.length,
			};
		} else {
			// For INSERT/UPDATE/DELETE, sum the rowsAffected
			const totalAffected = results.reduce((sum, r) => sum + (r.rowsAffected || 0), 0);
			return {
				rows: [],
				rowsAffected: totalAffected,
			};
		}
	}

	/**
	 * Inject _virtualShard filter into a query for a specific shard
	 *
	 * Uses the SQL AST parser to safely inject _virtualShard WHERE clause filtering
	 * for SELECT/UPDATE/DELETE queries. For CREATE TABLE, adds _virtualShard column.
	 *
	 * @param query - The original query
	 * @param params - Original query parameters
	 * @param shardId - The virtual shard ID to filter by
	 * @param tableName - The table name (for CREATE operations)
	 * @param queryType - The type of query (SELECT, INSERT, etc.)
	 * @returns Modified query and params with _virtualShard filter injected
	 */
	private injectVirtualShardFilter(
		query: string,
		params: any[],
		shardId: number,
		tableName: string,
		queryType: QueryType,
	): { modifiedQuery: string; modifiedParams: any[] } {
		// Don't filter certain operations - they don't need shard isolation
		if (queryType === 'DROP' || queryType === 'ALTER' || queryType === 'PRAGMA') {
			return { modifiedQuery: query, modifiedParams: params };
		}

		// For CREATE, also skip filtering (tables are created on all shards)
		if (queryType === 'CREATE') {
			return { modifiedQuery: query, modifiedParams: params };
		}

		try {
			// Parse the query into AST
			const ast = parse(query);
			const statement = ast.statements[0];

			if (!statement) {
				return { modifiedQuery: query, modifiedParams: params };
			}

			let modifiedStatement = statement;

			if (queryType === 'SELECT') {
				modifiedStatement = this.injectWhereFilterToSelect(
					statement as SelectStatement,
					shardId,
					params,
				);
			} else if (queryType === 'UPDATE') {
				modifiedStatement = this.injectWhereFilterToUpdate(
					statement as UpdateStatement,
					shardId,
					params,
				);
			} else if (queryType === 'DELETE') {
				modifiedStatement = this.injectWhereFilterToDelete(
					statement as DeleteStatement,
					shardId,
					params,
				);
			} else if (queryType === 'INSERT') {
				// Storage nodes handle INSERT modifications, no filtering needed here
				return { modifiedQuery: query, modifiedParams: params };
			}

			const modifiedQuery = generate(modifiedStatement);
			const modifiedParams = [...params, shardId];

			return { modifiedQuery, modifiedParams };
		} catch (error) {
			logger.warn('Failed to parse query for _virtualShard injection, using original query', {
				query,
				error: error instanceof Error ? error.message : String(error),
			});
			// Fallback: return original query
			return { modifiedQuery: query, modifiedParams: params };
		}
	}

	/**
	 * Inject _virtualShard filter into a SELECT statement using AST manipulation
	 */
	private injectWhereFilterToSelect(
		stmt: SelectStatement,
		shardId: number,
		params: any[],
	): SelectStatement {
		const virtualShardFilter: BinaryExpression = {
			type: 'BinaryExpression',
			operator: '=',
			left: {
				type: 'Identifier',
				name: '_virtualShard',
			},
			right: {
				type: 'Placeholder',
				parameterIndex: params.length,
			},
		};

		if (stmt.where) {
			// AND existing WHERE with new filter
			stmt.where = {
				type: 'BinaryExpression',
				operator: 'AND',
				left: virtualShardFilter,
				right: stmt.where,
			};
		} else {
			// No WHERE clause, just add the filter
			stmt.where = virtualShardFilter;
		}

		return stmt;
	}

	/**
	 * Inject _virtualShard filter into an UPDATE statement using AST manipulation
	 */
	private injectWhereFilterToUpdate(
		stmt: UpdateStatement,
		shardId: number,
		params: any[],
	): UpdateStatement {
		const virtualShardFilter: BinaryExpression = {
			type: 'BinaryExpression',
			operator: '=',
			left: {
				type: 'Identifier',
				name: '_virtualShard',
			},
			right: {
				type: 'Placeholder',
				parameterIndex: params.length,
			},
		};

		if (stmt.where) {
			// AND existing WHERE with new filter
			stmt.where = {
				type: 'BinaryExpression',
				operator: 'AND',
				left: virtualShardFilter,
				right: stmt.where,
			};
		} else {
			// No WHERE clause, just add the filter
			stmt.where = virtualShardFilter;
		}

		return stmt;
	}

	/**
	 * Inject _virtualShard filter into a DELETE statement using AST manipulation
	 */
	private injectWhereFilterToDelete(
		stmt: DeleteStatement,
		shardId: number,
		params: any[],
	): DeleteStatement {
		const virtualShardFilter: BinaryExpression = {
			type: 'BinaryExpression',
			operator: '=',
			left: {
				type: 'Identifier',
				name: '_virtualShard',
			},
			right: {
				type: 'Placeholder',
				parameterIndex: params.length,
			},
		};

		if (stmt.where) {
			// AND existing WHERE with new filter
			stmt.where = {
				type: 'BinaryExpression',
				operator: 'AND',
				left: virtualShardFilter,
				right: stmt.where,
			};
		} else {
			// No WHERE clause, just add the filter
			stmt.where = virtualShardFilter;
		}

		return stmt;
	}

	/**
	 * Inject _virtualShard column into CREATE TABLE query
	 *
	 * This adds a hidden column to every table that stores the virtual shard ID.
	 * This is critical for query isolation at the storage level.
	 *
	 * @param query - Original CREATE TABLE query from user
	 * @returns Modified query with _virtualShard INTEGER NOT NULL DEFAULT 0 column
	 */
	private injectVirtualShardColumn(query: string): string {
		// Match CREATE [TEMP] TABLE [IF NOT EXISTS] table_name (
		const tableDefRegex = /^(CREATE\s+(?:TEMP|TEMPORARY)?\s*TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\w+\s*\()([\s\S]*)(\);?\s*$)/i;

		const match = query.match(tableDefRegex);
		if (!match) {
			// If we can't parse it, return original query (shouldn't happen in normal cases)
			logger.warn('Could not parse CREATE TABLE query for _virtualShard injection', { query });
			return query;
		}

		const [, prefix, columnDefs, suffix] = match;

		// Add _virtualShard column before the closing parenthesis
		// Use a comma if there are existing columns
		const hasColumns = columnDefs.trim().length > 0;
		const virtualShardDef = '_virtualShard INTEGER NOT NULL DEFAULT 0';
		const joinChar = hasColumns ? ',' : '';

		const modifiedQuery = `${prefix}${columnDefs}${joinChar}\n\t${virtualShardDef}${suffix}`;

		return modifiedQuery;
	}
}

/**
 * Conductor API interface with sql execution and cache management
 */
export interface ConductorAPI {
	sql: (strings: TemplateStringsArray, ...values: any[]) => Promise<QueryResult>;
	getCacheStats: () => CacheStats;
	clearCache: () => void;
}

/**
 * Create a Conductor client for a specific database
 *
 * @param databaseId - Unique identifier for the database
 * @param cid - Correlation ID for request tracking
 * @param env - Worker environment with Durable Object bindings
 * @returns A Conductor API with sql method and cache management
 *
 * @example
 * const conductor = createConductor('my-database', correlationId, env);
 * const result = await conductor.sql`SELECT * FROM users WHERE id = ${123}`;
 * const stats = conductor.getCacheStats();
 * conductor.clearCache();
 */
export function createConductor(databaseId: string, cid: string, env: Env): ConductorAPI {
	const client = new ConductorClient(databaseId, cid, env.STORAGE, env.TOPOLOGY, env.INDEX_QUEUE, env);

	return {
		sql: (strings: TemplateStringsArray, ...values: any[]) => client.sql(strings, ...values),
		getCacheStats: () => client.getCacheStats(),
		clearCache: () => client.clearCache(),
	};
}
