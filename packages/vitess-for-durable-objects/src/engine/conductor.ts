import { parse } from '@databases/sqlite-ast';
import type {
	Statement,
	SelectStatement,
	InsertStatement,
	UpdateStatement,
	DeleteStatement,
	CreateTableStatement,
	CreateIndexStatement,
} from '@databases/sqlite-ast';
import type { Storage, QueryResult as StorageQueryResult, QueryType } from './storage';
import type { Topology, TableMetadata, QueryPlanData } from './topology';
import { extractTableName, getQueryType, buildQuery } from './utils/ast-utils';
import { extractTableMetadata } from './utils/schema-utils';
import type { IndexJob, IndexMaintenanceJob } from './queue/types';
import { TopologyCache } from './utils/topology-cache';

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
 * Result from a SQL query execution
 */
export interface QueryResult {
	rows: Record<string, any>[];
	rowsAffected?: number;
	cacheStats?: QueryCacheStats; // Cache statistics (only for SELECT queries)
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

	constructor(
		private databaseId: string,
		private storage: DurableObjectNamespace<Storage>,
		private topology: DurableObjectNamespace<Topology>,
		private indexQueue?: Queue,
		private env?: Env, // For test environment queue processing
	) {
		this.cache = new TopologyCache();
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
		// STEP 1: Parse - Build and parse the SQL query
		const { query, params } = buildQuery(strings, values);
		const statement = parse(query);

		// Handle CREATE TABLE statements (special case - affects all nodes)
		if (statement.type === 'CreateTableStatement') {
			return await this.handleCreateTable(statement as CreateTableStatement, query);
		}

		// Handle CREATE INDEX statements (special case - creates virtual index)
		if (statement.type === 'CreateIndexStatement') {
			return await this.handleCreateIndex(statement as CreateIndexStatement);
		}

		// STEP 2: Route - Get ALL topology data (with caching)
		const tableName = extractTableName(statement);
		if (!tableName) {
			throw new Error('Could not determine table name from query');
		}

		// **CACHED TOPOLOGY CALL** - Get all topology data AND determine shard targets
		// All planning logic (including index usage) happens inside the Topology DO
		// Cache eliminates this call for repeated queries with same predicates
		const { planData, cacheHit } = await this.getCachedQueryPlanData(tableName, statement, params);

		// Shard targets are already determined by Topology!
		const shardsToQuery = planData.shardsToQuery;

		// STEP 3: Execute - Run query on all target shards in parallel
		const queryType = getQueryType(statement);
		const results = await this.executeOnShards(shardsToQuery, query, params, queryType);

		// STEP 3.5: Index Maintenance - Enqueue async maintenance for UPDATE/DELETE
		// INSERT: Already handled in getQueryPlanData() (before query execution)
		// UPDATE/DELETE: Enqueue to queue for async processing (no blocking!)
		if ((statement.type === 'UpdateStatement' || statement.type === 'DeleteStatement') && planData.virtualIndexes.length > 0) {
			await this.enqueueIndexMaintenanceJob(
				tableName,
				statement,
				shardsToQuery.map((s) => s.shard_id),
				planData.virtualIndexes,
			);
		}

		// STEP 3.6: Cache Invalidation - Invalidate cache for write operations
		// This ensures the cache stays fresh after data modifications
		if (statement.type === 'InsertStatement' || statement.type === 'UpdateStatement' || statement.type === 'DeleteStatement') {
			this.invalidateCacheForWrite(tableName, statement, planData.virtualIndexes, params);
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

		return result;
	};

	/**
	 * Handle CREATE TABLE statement execution
	 */
	private async handleCreateTable(statement: CreateTableStatement, query: string): Promise<QueryResult> {
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
		const metadata = extractTableMetadata(statement);

		// Add table to topology
		await topologyStub.updateTopology({
			tables: {
				add: [metadata],
			},
		});

		// Execute CREATE TABLE on all storage nodes in parallel (use actual node IDs from topology)
		await Promise.all(
			topologyData.storage_nodes.map(async (node) => {
				const storageId = this.storage.idFromName(node.node_id);
				const storageStub = this.storage.get(storageId);

				await storageStub.executeQuery({
					query,
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
	private async handleCreateIndex(statement: CreateIndexStatement): Promise<QueryResult> {
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
		});

		return {
			rows: [],
			rowsAffected: 0,
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
	private async getCachedQueryPlanData(tableName: string, statement: Statement, params: any[]): Promise<{ planData: QueryPlanData; cacheHit: boolean }> {
		// Try to build a cache key for this query
		const cacheKey = this.cache.buildQueryPlanCacheKey(tableName, statement, params);

		// Check cache if we have a valid cache key
		if (cacheKey) {
			const cached = this.cache.getQueryPlanData(cacheKey);
			if (cached) {
				return { planData: cached, cacheHit: true };
			}
		}

		// Cache miss - fetch from Topology DO
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const planData = await topologyStub.getQueryPlanData(tableName, statement, params);

		// Store in cache if cacheable
		if (cacheKey) {
			const ttl = this.cache.getTTLForQuery(statement);
			this.cache.setQueryPlanData(cacheKey, planData, ttl);
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

		const row = statement.values[0]; // Only handle single-row inserts for now

		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns);
			const keyValue = this.extractKeyValueFromRow(statement.columns, row, indexColumns, params);
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
	 */
	private async executeOnShards(
		shardsToQuery: Array<{ table_name: string; shard_id: number; node_id: string }>,
		query: string,
		params: any[],
		queryType: QueryType,
	): Promise<QueryResult[]> {
		const BATCH_SIZE = 7;
		const allResults: QueryResult[] = [];

		// Process shards in batches of 7
		for (let i = 0; i < shardsToQuery.length; i += BATCH_SIZE) {
			const batch = shardsToQuery.slice(i, i + BATCH_SIZE);

			const batchResults = await Promise.all(
				batch.map(async (shard) => {
					// Get the storage stub using the node_id from table_shards mapping
					const storageId = this.storage.idFromName(shard.node_id);
					const storageStub = this.storage.get(storageId);

					// Execute the query (single query always returns StorageQueryResult)
					const rawResult = await storageStub.executeQuery({
						query,
						params,
						queryType,
					});

					// Convert StorageQueryResult to QueryResult
					const result = rawResult as unknown as StorageQueryResult;
					return {
						rows: result.rows,
						rowsAffected: result.rowsAffected,
					};
				}),
			);

			allResults.push(...batchResults);
		}

		return allResults;
	}

	/**
	 * Merge results from multiple shards
	 */
	private mergeResults(results: QueryResult[], statement: Statement): QueryResult {
		if (results.length === 1) {
			return results[0];
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
		};

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
			return {
				rows: mergedRows,
				rowsAffected: mergedRows.length,
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
}

/**
 * Create a Conductor client for a specific database
 *
 * @param databaseId - Unique identifier for the database
 * @param env - Worker environment with Durable Object bindings
 * @returns A Conductor client with sql method for executing queries
 *
 * @example
 * const conductor = createConductor('my-database', env);
 * const result = await conductor.sql`SELECT * FROM users WHERE id = ${123}`;
 */
export function createConductor(databaseId: string, env: Env): ConductorClient {
	return new ConductorClient(databaseId, env.STORAGE, env.TOPOLOGY, env.INDEX_QUEUE, env);
}
