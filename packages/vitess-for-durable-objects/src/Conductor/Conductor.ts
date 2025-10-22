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
import type { Storage, QueryResult as StorageQueryResult, QueryType } from '../Storage/Storage';
import type { Topology, TableMetadata } from '../Topology/Topology';
import { hashToShard } from './utils/sharding';
import { extractTableName, extractWhereClause, extractValueFromExpression, getQueryType, buildQuery } from './utils/ast-utils';
import { extractTableMetadata } from './utils/schema-utils';
import type { IndexJob } from '../Queue/types';

/**
 * Result from a SQL query execution
 */
export interface QueryResult {
	rows: Record<string, any>[];
	rowsAffected?: number;
}

/**
 * Conductor - Routes SQL queries to the appropriate storage shards
 *
 * The Conductor sits between the client and the distributed storage layer,
 * parsing queries, determining which shards to target, and coordinating
 * execution across multiple storage nodes.
 */
export class ConductorClient {
	constructor(
		private databaseId: string,
		private storage: DurableObjectNamespace<Storage>,
		private topology: DurableObjectNamespace<Topology>,
		private indexQueue?: Queue,
	) {}

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

		// STEP 2: Route - Determine which shards to target
		const tableName = extractTableName(statement);
		if (!tableName) {
			throw new Error('Could not determine table name from query');
		}

		const { tableMetadata, tableShards } = await this.getTableTopologyInfo(tableName);
		const shardsToQuery = this.determineShardTargets(statement, tableMetadata, tableShards, params, tableName);

		// STEP 3: Execute - Run query on all target shards in parallel
		const queryType = getQueryType(statement);
		const results = await this.executeOnShards(shardsToQuery, query, params, queryType);

		// STEP 4: Merge - Combine results from all shards
		return this.mergeResults(results, statement);
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
	 * 2. Creates the index definition in Topology with status 'building'
	 * 3. Enqueues an IndexBuildJob to the queue for async processing
	 * 4. Returns immediately to the client (non-blocking)
	 */
	private async handleCreateIndex(statement: CreateIndexStatement): Promise<QueryResult> {
		const indexName = statement.name.name;
		const tableName = statement.table.name;

		// Only support single-column indexes for now
		if (statement.columns.length !== 1) {
			throw new Error('Multi-column indexes are not yet supported. Only single-column indexes are allowed.');
		}

		const columnName = statement.columns[0].name;

		// Get topology stub
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);

		// Create the index in Topology with 'building' status
		const indexType = statement.unique ? 'unique' : 'hash';
		const result = await topologyStub.createVirtualIndex(indexName, tableName, columnName, indexType);

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
		await this.enqueueIndexJob({
			type: 'build_index',
			database_id: this.databaseId,
			table_name: tableName,
			column_name: columnName,
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
	 * Determine which shards should be targeted for a query
	 *
	 * This method handles routing logic for all statement types:
	 * - INSERT: Routes to a single shard based on shard key value
	 * - SELECT/UPDATE/DELETE: Analyzes WHERE clause to determine if we can route to a single shard
	 *   or need to query all shards
	 */
	private determineShardTargets(
		statement: Statement,
		tableMetadata: TableMetadata,
		tableShards: Array<{ table_name: string; shard_id: number; node_id: string }>,
		params: any[],
		tableName: string,
	): Array<{ table_name: string; shard_id: number; node_id: string }> {
		// Handle INSERT statements - route to specific shard based on shard key value
		if (statement.type === 'InsertStatement') {
			const shardId = this.getShardIdForInsert(statement as InsertStatement, tableMetadata, tableShards.length, params);
			const shard = tableShards.find((s) => s.shard_id === shardId);
			if (!shard) {
				throw new Error(`Shard ${shardId} not found for table '${tableName}'`);
			}
			return [shard];
		}

		// Handle SELECT/UPDATE/DELETE statements - check WHERE clause
		const whereClause = extractWhereClause(statement);

		if (whereClause) {
			// WHERE clause exists - check if it filters on shard key
			const shardId = this.getShardIdFromWhere(whereClause, tableMetadata, tableShards.length, params);
			if (shardId !== null) {
				// WHERE clause filters on shard key with equality - route to specific shard
				const shard = tableShards.find((s) => s.shard_id === shardId);
				if (!shard) {
					throw new Error(`Shard ${shardId} not found for table '${tableName}'`);
				}
				return [shard];
			}
		}

		// No WHERE clause, or WHERE clause doesn't filter on shard key - query all shards
		return tableShards;
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
	 * Calculate which shard an INSERT should go to based on the shard key value
	 */
	private getShardIdForInsert(statement: InsertStatement, tableMetadata: TableMetadata, numShards: number, params: any[]): number {
		// Find the index of the shard key column
		const shardKeyIndex = statement.columns?.findIndex((col) => col.name === tableMetadata.shard_key);

		if (shardKeyIndex === undefined || shardKeyIndex === -1) {
			throw new Error(`Shard key '${tableMetadata.shard_key}' not found in INSERT columns`);
		}

		// Get the shard key value from the first row of values
		// For now, only support single-row inserts
		if (statement.values.length === 0) {
			throw new Error('INSERT statement has no values');
		}

		const shardKeyExpression = statement.values[0][shardKeyIndex];

		// Extract the actual value from the Expression
		const value = extractValueFromExpression(shardKeyExpression, params);

		if (value === null || value === undefined) {
			throw new Error('Could not resolve shard key value from parameters');
		}

		// Hash the value to determine the shard
		return hashToShard(value, numShards);
	}


	/**
	 * Extract shard ID from WHERE clause if it filters on the shard key with equality
	 * Returns null if the WHERE clause doesn't provide a specific shard key value
	 */
	private getShardIdFromWhere(where: any, tableMetadata: TableMetadata, numShards: number, params: any[]): number | null {
		// For MVP, we only support simple equality comparisons: WHERE shard_key = value
		// We check if the WHERE clause is a binary operation with '=' operator
		if (where.type === 'BinaryOperation' && where.operator === '=') {
			// Check if one side is the shard key column
			const leftIsShardKey = where.left.type === 'ColumnReference' && where.left.name === tableMetadata.shard_key;
			const rightIsShardKey = where.right.type === 'ColumnReference' && where.right.name === tableMetadata.shard_key;

			let value: any = null;

			if (leftIsShardKey) {
				// Shard key is on the left, value is on the right
				value = extractValueFromExpression(where.right, params);
			} else if (rightIsShardKey) {
				// Shard key is on the right, value is on the left
				value = extractValueFromExpression(where.left, params);
			}

			if (value !== null) {
				return hashToShard(value, numShards);
			}
		}

		// WHERE clause doesn't provide a specific shard key value
		// This includes cases like:
		// - Complex conditions (AND/OR)
		// - Range queries (>, <, >=, <=)
		// - Non-equality operators
		// - Filters on non-shard-key columns
		return null;
	}


	/**
	 * Enqueue an index job to the queue
	 */
	private async enqueueIndexJob(job: IndexJob): Promise<void> {
		if (!this.indexQueue) {
			console.warn('INDEX_QUEUE not available, skipping index job:', job.type);
			return;
		}

		try {
			await this.indexQueue.send(job);
			console.log(`Enqueued ${job.type} job for ${job.table_name}`);
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
	return new ConductorClient(databaseId, env.STORAGE, env.TOPOLOGY, env.INDEX_QUEUE);
}
