import { parse } from '@databases/sqlite-ast';
import type { Statement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement, CreateTableStatement } from '@databases/sqlite-ast';
import type { Storage, QueryResult as StorageQueryResult, QueryType } from '../Storage/Storage';
import type { Topology, TableMetadata } from '../Topology/Topology';

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
	) {}

	/**
	 * Execute a SQL query using tagged template literals
	 *
	 * @example
	 * const result = await conductor.sql`SELECT * FROM users WHERE id = ${userId}`;
	 * await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`;
	 */
	sql = async (strings: TemplateStringsArray, ...values: any[]): Promise<QueryResult> => {
		// Build the SQL query and extract parameters
		const { query, params } = this.buildQuery(strings, values);

		// Parse the query to determine operation type and affected tables
		const statement = parse(query);

		// Handle CREATE TABLE statements
		if (statement.type === 'CreateTableStatement') {
			return await this.handleCreateTable(statement as CreateTableStatement, query);
		}

		const tableName = this.extractTableName(statement);

		if (!tableName) {
			throw new Error('Could not determine table name from query');
		}

		// Get topology information
		const topologyId = this.topology.idFromName(this.databaseId);
		const topologyStub = this.topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find the table metadata
		const tableMetadata = topologyData.tables.find((t) => t.table_name === tableName);
		if (!tableMetadata) {
			throw new Error(`Table '${tableName}' not found in topology`);
		}

		// For MVP: query all shards for SELECT, first shard for everything else
		const isSelect = statement.type === 'SelectStatement';

		// Get table shards for this table
		const tableShards = topologyData.table_shards.filter((s) => s.table_name === tableName);

		if (tableShards.length === 0) {
			throw new Error(`No shards found for table '${tableName}'`);
		}

		// Determine which shards to query
		const shardsToQuery = isSelect
			? tableShards // All shards for SELECT
			: [tableShards[0]]; // First shard for INSERT/UPDATE/DELETE (for now)

		// Execute query on appropriate shard(s)
		const results: QueryResult[] = [];
		for (const shard of shardsToQuery) {
			// Get the storage stub using the node_id from table_shards mapping
			const storageId = this.storage.idFromName(shard.node_id);
			const storageStub = this.storage.get(storageId);

			// Execute the query (single query always returns StorageQueryResult)
			const rawResult = await storageStub.executeQuery({
				query,
				params,
				queryType: this.getQueryType(statement),
			});

			// Convert StorageQueryResult to QueryResult
			const result = rawResult as unknown as StorageQueryResult;
			results.push({
				rows: result.rows,
				rowsAffected: result.rowsAffected,
			});
		}

		// Merge results if multiple shards are involved
		if (results.length === 1) {
			return results[0];
		} else {
			return this.mergeResultsSimple(results, isSelect);
		}
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
		const metadata = this.extractTableMetadata(statement);

		// Add table to topology
		await topologyStub.updateTopology({
			tables: {
				add: [metadata],
			},
		});

		// Execute CREATE TABLE on all storage nodes
		const numStorageNodes = topologyData.storage_nodes.length;
		for (let nodeIndex = 0; nodeIndex < numStorageNodes; nodeIndex++) {
			const nodeId = `node-${nodeIndex}`;
			const storageId = this.storage.idFromName(nodeId);
			const storageStub = this.storage.get(storageId);

			await storageStub.executeQuery({
				query,
				params: [],
				queryType: 'CREATE',
			});
		}

		return {
			rows: [],
			rowsAffected: 0,
		};
	}

	/**
	 * Build a parameterized query from template literal parts
	 */
	private buildQuery(strings: TemplateStringsArray, values: any[]): { query: string; params: any[] } {
		let query = '';
		const params: any[] = [];

		for (let i = 0; i < strings.length; i++) {
			query += strings[i];

			if (i < values.length) {
				// Add parameter placeholder
				query += '?';
				params.push(values[i]);
			}
		}

		return { query, params };
	}

	/**
	 * Extract table metadata from a CREATE TABLE statement
	 * Infers all sharding configuration from the table structure
	 */
	private extractTableMetadata(statement: CreateTableStatement): Omit<TableMetadata, 'created_at' | 'updated_at'> {
		const tableName = statement.table.name;

		// Find the primary key column
		const primaryKeyColumn = statement.columns.find((col) =>
			col.constraints?.some((c) => c.constraint === 'PRIMARY KEY')
		);

		if (!primaryKeyColumn) {
			throw new Error(`CREATE TABLE ${tableName} must have a PRIMARY KEY column`);
		}

		const primaryKey = primaryKeyColumn.name.name;
		const primaryKeyType = primaryKeyColumn.dataType;

		// Default sharding configuration
		// - shard_key: use the primary key
		// - num_shards: 1 (single shard, can be manually updated later via topology)
		// - shard_strategy: hash (standard approach)
		// - block_size: 500 rows per block
		return {
			table_name: tableName,
			primary_key: primaryKey,
			primary_key_type: primaryKeyType,
			shard_strategy: 'hash',
			shard_key: primaryKey,
			num_shards: 1,
			block_size: 500,
		};
	}

	/**
	 * Extract the table name from a parsed SQL statement
	 */
	private extractTableName(statement: Statement): string | null {
		switch (statement.type) {
			case 'SelectStatement':
				return (statement as SelectStatement).from?.name || null;
			case 'InsertStatement':
				return (statement as InsertStatement).table.name;
			case 'UpdateStatement':
				return (statement as UpdateStatement).table.name;
			case 'DeleteStatement':
				return (statement as DeleteStatement).table.name;
			default:
				return null;
		}
	}

	/**
	 * Determine the query type from a parsed SQL statement
	 */
	private getQueryType(statement: Statement): QueryType {
		switch (statement.type) {
			case 'SelectStatement':
				return 'SELECT';
			case 'InsertStatement':
				return 'INSERT';
			case 'UpdateStatement':
				return 'UPDATE';
			case 'DeleteStatement':
				return 'DELETE';
			case 'CreateTableStatement':
				return 'CREATE';
			case 'AlterTableStatement':
				return 'ALTER';
			case 'CreateIndexStatement':
				return 'CREATE';
			default:
				return 'UNKNOWN';
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
	return new ConductorClient(databaseId, env.STORAGE, env.TOPOLOGY);
}
