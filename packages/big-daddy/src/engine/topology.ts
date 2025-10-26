import { DurableObject } from 'cloudflare:workers';
import { withLogTags } from 'workers-tagged-logger';
import { logger } from '../logger';
import type { Statement, UpdateStatement, DeleteStatement, SelectStatement } from '@databases/sqlite-ast';

// Type definitions for topology data structures
export interface StorageNode {
	node_id: string;
	created_at: number;
	updated_at: number;
	status: 'active' | 'down' | 'draining';
	capacity_used: number;
	error: string | null;
}

export interface TableMetadata {
	table_name: string;
	primary_key: string;
	primary_key_type: string;
	shard_strategy: 'hash' | 'range';
	shard_key: string;
	num_shards: number;
	block_size: number;
	created_at: number;
	updated_at: number;
}

export interface TableShard {
	table_name: string;
	shard_id: number;
	node_id: string;
	created_at: number;
	updated_at: number;
}

export type IndexStatus = 'building' | 'ready' | 'failed' | 'rebuilding';

export interface VirtualIndex {
	index_name: string;
	table_name: string;
	columns: string; // Stored as JSON array of column names for composite indexes
	index_type: 'hash' | 'unique';
	status: IndexStatus;
	error_message: string | null;
	created_at: number;
	updated_at: number;
}

export interface VirtualIndexEntry {
	index_name: string;
	key_value: string;
	shard_ids: string; // Stored as JSON array string
	updated_at: number;
}

export interface TopologyData {
	storage_nodes: StorageNode[];
	tables: TableMetadata[];
	table_shards: TableShard[];
	virtual_indexes: VirtualIndex[];
	virtual_index_entries: VirtualIndexEntry[];
}

export interface StorageNodeUpdate {
	node_id: string;
	status?: 'active' | 'down' | 'draining';
	capacity_used?: number;
	error?: string | null;
}

export interface TableMetadataUpdate {
	table_name: string;
	num_shards?: number;
	block_size?: number;
}

export interface TopologyUpdates {
	// Storage nodes are immutable
	storage_nodes?: never;
	tables?: {
		add?: Omit<TableMetadata, 'created_at' | 'updated_at'>[];
		update?: TableMetadataUpdate[];
		remove?: string[];
	};
}

export interface QueryPlanData {
	// Table metadata
	tableMetadata: TableMetadata;

	// Shards to query (already determined using indexes if possible)
	shardsToQuery: Array<{ table_name: string; shard_id: number; node_id: string }>;

	// Ready virtual indexes for this table (for index maintenance)
	virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>;
}

/** Topology Durable Object for managing cluster topology */
export class Topology extends DurableObject<Env> {
	/**
	 * The constructor is invoked once upon creation of the Durable Object
	 *
	 * @param ctx - The interface for interacting with Durable Object state
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 */
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.initializeSchema();
		// Set up recurring alarm to check capacity every 5 minutes
		this.ctx.storage.setAlarm(Date.now() + 5 * 60 * 1000);
	}

	/**
	 * Initialize the SQLite schema for topology metadata
	 */
	private initializeSchema(): void {
		// Cluster metadata - stores if cluster is created
		this.ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS cluster_metadata (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL
			)
		`);

		// Storage nodes table - tracks all available storage DOs
		this.ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS storage_nodes (
				node_id TEXT PRIMARY KEY,
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL,
				status TEXT NOT NULL DEFAULT 'active',
				capacity_used INTEGER DEFAULT 0,
				error TEXT
			)
		`);

		// Tables metadata - stores info about user-created tables
		this.ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS tables (
				table_name TEXT PRIMARY KEY,
				primary_key TEXT NOT NULL,
				primary_key_type TEXT NOT NULL,
				shard_strategy TEXT NOT NULL CHECK(shard_strategy IN ('hash', 'range')),
				shard_key TEXT NOT NULL,
				num_shards INTEGER NOT NULL DEFAULT 1,
				block_size INTEGER DEFAULT 500,
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL
			)
		`);

		// Table shards - maps virtual shards to physical storage nodes
		this.ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS table_shards (
				table_name TEXT NOT NULL,
				shard_id INTEGER NOT NULL,
				node_id TEXT NOT NULL,
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL,
				PRIMARY KEY (table_name, shard_id),
				FOREIGN KEY (table_name) REFERENCES tables(table_name) ON DELETE CASCADE,
				FOREIGN KEY (node_id) REFERENCES storage_nodes(node_id)
			)
		`);

		// Virtual indexes - metadata for indexes that enable query optimization
		this.ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS virtual_indexes (
				index_name TEXT PRIMARY KEY,
				table_name TEXT NOT NULL,
				columns TEXT NOT NULL,
				index_type TEXT NOT NULL CHECK(index_type IN ('hash', 'unique')),
				status TEXT NOT NULL DEFAULT 'building' CHECK(status IN ('building', 'ready', 'failed', 'rebuilding')),
				error_message TEXT,
				created_at INTEGER NOT NULL,
				updated_at INTEGER NOT NULL,
				FOREIGN KEY (table_name) REFERENCES tables(table_name) ON DELETE CASCADE
			)
		`);

		// Virtual index entries - maps indexed values to shard IDs
		this.ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS virtual_index_entries (
				index_name TEXT NOT NULL,
				key_value TEXT NOT NULL,
				shard_ids TEXT NOT NULL,
				updated_at INTEGER NOT NULL,
				PRIMARY KEY (index_name, key_value),
				FOREIGN KEY (index_name) REFERENCES virtual_indexes(index_name) ON DELETE CASCADE
			)
		`);
	}

	/**
	 * Check if the topology has been created
	 */
	private isCreated(): boolean {
		const result = this.ctx.storage.sql.exec(`SELECT value FROM cluster_metadata WHERE key = 'created'`).toArray() as unknown as {
			value: string;
		}[];

		return result.length > 0 && result[0].value === 'true';
	}

	/**
	 * Ensure the topology has been created
	 */
	private ensureCreated(): void {
		if (!this.isCreated()) {
			throw new Error('Topology not created. Call create() first to initialize the cluster.');
		}
	}

	/**
	 * Create the cluster topology with a fixed number of storage nodes
	 * This can only be called once - storage nodes cannot be modified after creation
	 *
	 * @param numNodes - Number of storage nodes in the cluster
	 * @returns Success status or error
	 */
	async create(numNodes: number): Promise<{ success: boolean; error?: string }> {
		if (this.isCreated()) {
			throw new Error('Topology already created. Storage nodes cannot be modified after creation.');
		}

		if (numNodes < 1) {
			return { success: false, error: 'Number of nodes must be at least 1' };
		}

		const now = Date.now();

		// Mark as created
		this.ctx.storage.sql.exec(`INSERT INTO cluster_metadata (key, value) VALUES ('created', 'true')`);

		// Store number of nodes
		this.ctx.storage.sql.exec(`INSERT INTO cluster_metadata (key, value) VALUES ('num_nodes', ?)`, String(numNodes));

		// Create storage nodes with unique, unguessable IDs
		for (let i = 0; i < numNodes; i++) {
			// Generate a unique ID using crypto.randomUUID()
			const nodeId = crypto.randomUUID();
			this.ctx.storage.sql.exec(
				`INSERT INTO storage_nodes (node_id, created_at, updated_at, status, capacity_used, error)
				 VALUES (?, ?, ?, 'active', 0, NULL)`,
				nodeId,
				now,
				now,
			);
		}

		return { success: true };
	}

	/**
	 * Read all topology information in a single operation
	 *
	 * @returns Complete topology data including storage nodes, tables, and virtual indexes
	 */
	async getTopology(): Promise<TopologyData> {
		this.ensureCreated();
		const storage_nodes = this.ctx.storage.sql.exec(`SELECT * FROM storage_nodes`).toArray() as unknown as StorageNode[];

		const tables = this.ctx.storage.sql.exec(`SELECT * FROM tables`).toArray() as unknown as TableMetadata[];

		const table_shards = this.ctx.storage.sql.exec(`SELECT * FROM table_shards ORDER BY table_name, shard_id`).toArray() as unknown as TableShard[];

		const virtual_indexes = this.ctx.storage.sql.exec(`SELECT * FROM virtual_indexes`).toArray() as unknown as VirtualIndex[];

		const virtual_index_entries = this.ctx.storage.sql.exec(`SELECT * FROM virtual_index_entries`).toArray() as unknown as VirtualIndexEntry[];

		return {
			storage_nodes,
			tables,
			table_shards,
			virtual_indexes,
			virtual_index_entries,
		};
	}

	/**
	 * Get all topology data needed for query planning in a single call
	 * This eliminates multiple round-trips to the Topology DO
	 *
	 * This method determines shard targets using indexes if possible, all within the DO
	 *
	 * @param tableName - Table name being queried
	 * @param statement - Parsed SQL statement
	 * @param params - Query parameters
	 * @returns Complete query plan with shard targets determined
	 */
	async getQueryPlanData(tableName: string, statement: Statement, params: any[], correlationId?: string): Promise<QueryPlanData> {
		return withLogTags({ source: 'Topology' }, async () => {
			if (correlationId) {
				logger.setTags({
					correlationId,
					requestId: correlationId,
					component: 'Topology',
					operation: 'getQueryPlanData',
					table: tableName,
				});
			}

			const startTime = Date.now();
			logger.debug('Getting query plan data', {
				table: tableName,
				queryType: statement.type,
			});

			this.ensureCreated();

			// Fetch ALL topology data for this table in a single pass
			const tables = this.ctx.storage.sql.exec(
				`SELECT * FROM tables WHERE table_name = ?`,
				tableName
			).toArray() as unknown as TableMetadata[];

			const tableMetadata = tables[0];
			if (!tableMetadata) {
				logger.error('Table not found in topology', { table: tableName });
				throw new Error(`Table '${tableName}' not found in topology`);
			}

			const tableShards = this.ctx.storage.sql.exec(
				`SELECT * FROM table_shards WHERE table_name = ? ORDER BY shard_id`,
				tableName
			).toArray() as unknown as TableShard[];

			if (tableShards.length === 0) {
				logger.error('No shards found for table', { table: tableName });
				throw new Error(`No shards found for table '${tableName}'`);
			}

			// Only get ready indexes
			const virtual_indexes = this.ctx.storage.sql.exec(
				`SELECT index_name, columns, index_type FROM virtual_indexes WHERE table_name = ? AND status = 'ready'`,
				tableName
			).toArray() as unknown as Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>;

			logger.debug('Topology data fetched', {
				shardCount: tableShards.length,
				indexCount: virtual_indexes.length,
			});

			// Determine which shards to query (using indexes if possible)
			const shardsToQuery = await this.determineShardTargets(
				statement,
				tableMetadata,
				tableShards,
				virtual_indexes,
				params
			);

			logger.info('Shard targets determined', {
				shardsSelected: shardsToQuery.length,
				totalShards: tableShards.length,
				strategy: shardsToQuery.length === 1 ? 'single' : shardsToQuery.length === tableShards.length ? 'all' : 'subset',
			});

			// For INSERT: Handle index maintenance NOW (before query executes)
			// We know the target shard and have all the data we need
			if (statement.type === 'InsertStatement' && virtual_indexes.length > 0) {
				logger.debug('Maintaining indexes for INSERT', {
					indexCount: virtual_indexes.length,
				});
				await this.maintainIndexesForInsert(
					statement as any,
					virtual_indexes,
					shardsToQuery[0], // INSERT always goes to single shard
					params
				);
			}

			const duration = Date.now() - startTime;
			logger.debug('Query plan data completed', { duration });

			return {
				tableMetadata,
				shardsToQuery,
				virtualIndexes: virtual_indexes,
			};
		});
	}

	/**
	 * Determine which shards to query based on the statement and available indexes
	 * This is called internally by getQueryPlanData
	 */
	private async determineShardTargets(
		statement: Statement,
		tableMetadata: TableMetadata,
		tableShards: TableShard[],
		virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
		params: any[]
	): Promise<Array<{ table_name: string; shard_id: number; node_id: string }>> {
		// For INSERT, route to specific shard based on shard key value
		if (statement.type === 'InsertStatement') {
			try {
				const shardId = this.getShardIdForInsert(statement as any, tableMetadata, tableShards.length, params);
				const shard = tableShards.find((s) => s.shard_id === shardId);
				if (!shard) {
					throw new Error(`Shard ${shardId} not found for table '${tableMetadata.table_name}'`);
				}
				return [shard];
			} catch (error) {
				// If we can't determine shard (e.g., bulk inserts), use first shard as fallback
				// This matches the Conductor's behavior
				return [tableShards[0]];
			}
		}

		// For SELECT/UPDATE/DELETE, check WHERE clause
		const where = (statement as any).where;

		if (where) {
			// Check if it filters on shard key
			const shardId = this.getShardIdFromWhere(where, tableMetadata, tableShards.length, params);
			if (shardId !== null) {
				const shard = tableShards.find((s) => s.shard_id === shardId);
				if (!shard) {
					throw new Error(`Shard ${shardId} not found for table '${tableMetadata.table_name}'`);
				}
				return [shard];
			}

			// Shard key not used - check if we can use a virtual index
			const indexedShards = await this.getShardsFromIndexedWhere(where, tableMetadata.table_name, tableShards, virtualIndexes, params);
			if (indexedShards !== null) {
				return indexedShards;
			}
		}

		// No WHERE clause or couldn't optimize - query all shards
		return tableShards;
	}

	/**
	 * Calculate shard ID for INSERT based on shard key value
	 */
	private getShardIdForInsert(statement: any, tableMetadata: TableMetadata, numShards: number, params: any[]): number {
		// Find the shard key column in the INSERT
		const columnIndex = statement.columns?.findIndex((col: any) => col.name === tableMetadata.shard_key);

		if (columnIndex === undefined || columnIndex === -1) {
			throw new Error(`Shard key '${tableMetadata.shard_key}' not found in INSERT statement`);
		}

		// Get the value for the shard key
		// statement.values[0] is an array of expressions, one for each column
		if (statement.values.length === 0) {
			throw new Error('INSERT statement has no values');
		}

		const valueExpression = statement.values[0][columnIndex];
		const value = this.extractValueFromExpression(valueExpression, params);

		if (value === null || value === undefined) {
			throw new Error(`Could not resolve shard key value from parameters`);
		}

		// Hash the value to get shard ID
		return this.hashToShardId(String(value), numShards);
	}

	/**
	 * Try to determine shard ID from WHERE clause
	 */
	private getShardIdFromWhere(where: any, tableMetadata: TableMetadata, numShards: number, params: any[]): number | null {
		if (where.type === 'BinaryExpression' && where.operator === '=') {
			let columnName: string | null = null;
			let valueExpression: any | null = null;

			if (where.left.type === 'Identifier') {
				columnName = where.left.name;
				valueExpression = where.right;
			} else if (where.right.type === 'Identifier') {
				columnName = where.right.name;
				valueExpression = where.left;
			}

			if (columnName === tableMetadata.shard_key && valueExpression) {
				const value = this.extractValueFromExpression(valueExpression, params);
				if (value !== null && value !== undefined) {
					return this.hashToShardId(String(value), numShards);
				}
			}
		}

		return null;
	}

	/**
	 * Check if WHERE clause can use a virtual index to reduce shard fan-out
	 */
	private async getShardsFromIndexedWhere(
		where: any,
		tableName: string,
		tableShards: TableShard[],
		virtualIndexes: Array<{ index_name: string; columns: string; index_type: 'hash' | 'unique' }>,
		params: any[]
	): Promise<Array<{ table_name: string; shard_id: number; node_id: string }> | null> {
		// Try to match composite index from AND conditions
		if (where.type === 'BinaryExpression' && where.operator === 'AND') {
			const columnValues = this.extractColumnValuesFromAnd(where, params);

			if (columnValues.size > 0) {
				const index = virtualIndexes.find((idx) => {
					const indexColumns = JSON.parse(idx.columns);

					if (indexColumns.length === 1) {
						return columnValues.has(indexColumns[0]);
					}

					// Multi-column index - check leftmost prefix
					let matchCount = 0;
					for (let i = 0; i < indexColumns.length; i++) {
						if (columnValues.has(indexColumns[i])) {
							matchCount++;
						} else {
							break;
						}
					}

					return matchCount > 0 && matchCount === Math.min(columnValues.size, indexColumns.length);
				});

				if (index) {
					const indexColumns = JSON.parse(index.columns);
					let keyValue: string;

					if (indexColumns.length === 1) {
						const value = columnValues.get(indexColumns[0])!;
						if (value === null || value === undefined) {
							return null;
						}
						keyValue = String(value);
					} else {
						const values: any[] = [];
						for (const col of indexColumns) {
							const value = columnValues.get(col);
							if (value === undefined) break;
							if (value === null) return null;
							values.push(value);
						}
						if (values.length === 0) return null;
						keyValue = values.length === 1 ? String(values[0]) : JSON.stringify(values);
					}

					const shardIds = await this.getIndexedShards(index.index_name, keyValue);
					if (shardIds === null || shardIds.length === 0) {
						return [];
					}

					return tableShards.filter((s) => shardIds.includes(s.shard_id));
				}
			}
		}

		// Try to match single equality condition
		if (where.type === 'BinaryExpression' && where.operator === '=') {
			let columnName: string | null = null;
			let valueExpression: any | null = null;

			if (where.left.type === 'Identifier') {
				columnName = where.left.name;
				valueExpression = where.right;
			} else if (where.right.type === 'Identifier') {
				columnName = where.right.name;
				valueExpression = where.left;
			}

			if (!columnName || !valueExpression) {
				return null;
			}

			const index = virtualIndexes.find((idx) => {
				const columns = JSON.parse(idx.columns);
				return columns.length === 1 && columns[0] === columnName;
			});

			if (!index) {
				return null;
			}

			const value = this.extractValueFromExpression(valueExpression, params);
			if (value === null || value === undefined) {
				return null;
			}

			const keyValue = String(value);
			const shardIds = await this.getIndexedShards(index.index_name, keyValue);

			if (shardIds === null || shardIds.length === 0) {
				return [];
			}

			return tableShards.filter((s) => shardIds.includes(s.shard_id));
		}

		// Try to match IN query
		if (where.type === 'InExpression') {
			const columnName = where.expression.type === 'Identifier' ? where.expression.name : null;

			if (!columnName) {
				return null;
			}

			const index = virtualIndexes.find((idx) => {
				const columns = JSON.parse(idx.columns);
				return columns.length === 1 && columns[0] === columnName;
			});

			if (!index) {
				return null;
			}

			const values: any[] = [];
			for (const item of where.values) {
				const value = this.extractValueFromExpression(item, params);
				if (value !== null && value !== undefined) {
					values.push(value);
				}
			}

			if (values.length === 0) {
				return null;
			}

			const allShardIds = new Set<number>();
			for (const value of values) {
				const keyValue = String(value);
				const shardIds = await this.getIndexedShards(index.index_name, keyValue);

				if (shardIds && shardIds.length > 0) {
					shardIds.forEach(id => allShardIds.add(id));
				}
			}

			if (allShardIds.size === 0) {
				return [];
			}

			return tableShards.filter((s) => allShardIds.has(s.shard_id));
		}

		return null;
	}

	/**
	 * Extract column-value pairs from AND conditions
	 */
	private extractColumnValuesFromAnd(where: any, params: any[]): Map<string, any> {
		const columnValues = new Map<string, any>();

		const extract = (node: any) => {
			if (node.type === 'BinaryExpression') {
				if (node.operator === 'AND') {
					extract(node.left);
					extract(node.right);
				} else if (node.operator === '=') {
					let columnName: string | null = null;
					let valueExpression: any | null = null;

					if (node.left.type === 'Identifier') {
						columnName = node.left.name;
						valueExpression = node.right;
					} else if (node.right.type === 'Identifier') {
						columnName = node.right.name;
						valueExpression = node.left;
					}

					if (columnName && valueExpression) {
						const value = this.extractValueFromExpression(valueExpression, params);
						columnValues.set(columnName, value);
					}
				}
			}
		};

		extract(where);
		return columnValues;
	}

	/**
	 * Extract value from an expression node
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
	 * Hash a value to a shard ID
	 */
	private hashToShardId(value: string, numShards: number): number {
		let hash = 0;
		for (let i = 0; i < value.length; i++) {
			hash = ((hash << 5) - hash) + value.charCodeAt(i);
			hash = hash & hash;
		}
		return Math.abs(hash) % numShards;
	}

	/**
	 * Maintain indexes for INSERT operation
	 * Called from getQueryPlanData before the INSERT executes
	 */
	private async maintainIndexesForInsert(
		statement: any,
		indexes: Array<{ index_name: string; columns: string }>,
		shard: { table_name: string; shard_id: number; node_id: string },
		params: any[]
	): Promise<void> {
		// Extract values from the INSERT statement
		if (!statement.columns || statement.values.length === 0) {
			return;
		}

		const row = statement.values[0]; // Only handle single-row inserts for now

		// For each index, extract the value(s) and update the index entry
		for (const index of indexes) {
			const indexColumns = JSON.parse(index.columns);

			// Build composite key from all indexed columns
			const values: any[] = [];
			let hasNull = false;

			for (const colName of indexColumns) {
				const columnIndex = statement.columns.findIndex((col: any) => col.name === colName);
				if (columnIndex === -1) {
					// This column is not in the INSERT - skip this index
					hasNull = true;
					break;
				}

				const valueExpression = row[columnIndex];
				const value = this.extractValueFromExpression(valueExpression, params);

				if (value === null || value === undefined) {
					// NULL values are not indexed
					hasNull = true;
					break;
				}

				values.push(value);
			}

			if (hasNull || values.length !== indexColumns.length) {
				continue;
			}

			// Build the key value
			const keyValue = indexColumns.length === 1 ? String(values[0]) : JSON.stringify(values);

			// Add this shard to the index entry for this value
			await this.addShardToIndexEntry(index.index_name, keyValue, shard.shard_id);
		}
	}

	/**
	 * Batch update topology information in a single transaction
	 * Note: Storage nodes cannot be modified - they are immutable after creation
	 *
	 * @param updates - Object containing all changes to apply
	 * @returns Success status
	 */
	async updateTopology(updates: TopologyUpdates): Promise<{ success: boolean }> {
		this.ensureCreated();

		const now = Date.now();

		// Add new tables
		if (updates.tables?.add) {
			// Get storage nodes for shard distribution
			const nodes = this.ctx.storage.sql.exec(`SELECT node_id FROM storage_nodes WHERE status = 'active'`).toArray() as unknown as {
				node_id: string;
			}[];

			if (nodes.length === 0) {
				throw new Error('No active storage nodes available');
			}

			for (const table of updates.tables.add) {
				// Insert table metadata
				this.ctx.storage.sql.exec(
					`INSERT INTO tables (table_name, primary_key, primary_key_type, shard_strategy, shard_key, num_shards, block_size, created_at, updated_at)
					 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					table.table_name,
					table.primary_key,
					table.primary_key_type,
					table.shard_strategy,
					table.shard_key,
					table.num_shards,
					table.block_size ?? 500,
					now,
					now,
				);

				// Create table_shards mappings - distribute shards across storage nodes
				for (let shardId = 0; shardId < table.num_shards; shardId++) {
					// Simple modulo distribution across available nodes
					const nodeIndex = shardId % nodes.length;
					const nodeId = nodes[nodeIndex].node_id;

					this.ctx.storage.sql.exec(
						`INSERT INTO table_shards (table_name, shard_id, node_id, created_at, updated_at)
						 VALUES (?, ?, ?, ?, ?)`,
						table.table_name,
						shardId,
						nodeId,
						now,
						now,
					);
				}
			}
		}

		// Update existing tables
		if (updates.tables?.update) {
			for (const table of updates.tables.update) {
				const setClauses: string[] = [];
				const values: any[] = [];

				if (table.num_shards !== undefined) {
					setClauses.push('num_shards = ?');
					values.push(table.num_shards);
				}
				if (table.block_size !== undefined) {
					setClauses.push('block_size = ?');
					values.push(table.block_size);
				}

				if (setClauses.length > 0) {
					// Always update the updated_at timestamp
					setClauses.push('updated_at = ?');
					values.push(now);
					values.push(table.table_name);
					this.ctx.storage.sql.exec(`UPDATE tables SET ${setClauses.join(', ')} WHERE table_name = ?`, ...values);
				}
			}
		}

		// Remove tables
		if (updates.tables?.remove) {
			for (const table_name of updates.tables.remove) {
				this.ctx.storage.sql.exec(`DELETE FROM tables WHERE table_name = ?`, table_name);
			}
		}

		return { success: true };
	}

	/**
	 * Create a virtual index
	 *
	 * @param indexName - Unique name for the index
	 * @param tableName - Table to index
	 * @param columnName - Column to index
	 * @param indexType - Type of index ('hash' or 'unique')
	 * @returns Success status or error
	 */
	async createVirtualIndex(
		indexName: string,
		tableName: string,
		columns: string[],
		indexType: 'hash' | 'unique',
	): Promise<{ success: boolean; error?: string }> {
		this.ensureCreated();

		// Check if index already exists
		const existing = this.ctx.storage.sql
			.exec(`SELECT index_name FROM virtual_indexes WHERE index_name = ?`, indexName)
			.toArray() as unknown as { index_name: string }[];

		if (existing.length > 0) {
			return { success: false, error: `Index '${indexName}' already exists` };
		}

		// Check if table exists
		const tableExists = this.ctx.storage.sql
			.exec(`SELECT table_name FROM tables WHERE table_name = ?`, tableName)
			.toArray() as unknown as { table_name: string }[];

		if (tableExists.length === 0) {
			return { success: false, error: `Table '${tableName}' does not exist` };
		}

		const now = Date.now();

		// Create index with 'building' status
		// Store columns as JSON array
		this.ctx.storage.sql.exec(
			`INSERT INTO virtual_indexes (index_name, table_name, columns, index_type, status, error_message, created_at, updated_at)
			 VALUES (?, ?, ?, ?, 'building', NULL, ?, ?)`,
			indexName,
			tableName,
			JSON.stringify(columns),
			indexType,
			now,
			now,
		);

		return { success: true };
	}

	/**
	 * Update virtual index status
	 *
	 * @param indexName - Index to update
	 * @param status - New status
	 * @param errorMessage - Optional error message for failed status
	 */
	async updateIndexStatus(indexName: string, status: IndexStatus, errorMessage?: string): Promise<void> {
		this.ensureCreated();

		const now = Date.now();

		this.ctx.storage.sql.exec(
			`UPDATE virtual_indexes SET status = ?, error_message = ?, updated_at = ? WHERE index_name = ?`,
			status,
			errorMessage ?? null,
			now,
			indexName,
		);
	}

	/**
	 * Batch upsert multiple virtual index entries
	 *
	 * Writes entries one at a time to avoid SQLite variable limits and handle variable-length values.
	 * This is safe and efficient because Durable Object writes are extremely fast (single-digit ms).
	 *
	 * @param indexName - Index name
	 * @param entries - Array of entries to upsert, each with keyValue and shardIds
	 * @returns Number of entries upserted
	 */
	async batchUpsertIndexEntries(
		indexName: string,
		entries: Array<{ keyValue: string; shardIds: number[] }>,
	): Promise<{ count: number }> {
		this.ensureCreated();

		if (entries.length === 0) {
			return { count: 0 };
		}

		const now = Date.now();

		// Write entries one at a time - DO writes are fast enough that this is not a bottleneck
		for (const entry of entries) {
			this.ctx.storage.sql.exec(
				`INSERT OR REPLACE INTO virtual_index_entries (index_name, key_value, shard_ids, updated_at)
				 VALUES (?, ?, ?, ?)`,
				indexName,
				entry.keyValue,
				JSON.stringify(entry.shardIds),
				now,
			);
		}

		return { count: entries.length };
	}

	/**
	 * Get shard IDs for a specific indexed value
	 *
	 * @param indexName - Index name
	 * @param keyValue - The indexed value
	 * @returns Array of shard IDs, or null if not found
	 */
	async getIndexedShards(indexName: string, keyValue: string): Promise<number[] | null> {
		this.ensureCreated();

		const result = this.ctx.storage.sql
			.exec(`SELECT shard_ids FROM virtual_index_entries WHERE index_name = ? AND key_value = ?`, indexName, keyValue)
			.toArray() as unknown as { shard_ids: string }[];

		if (result.length === 0) {
			return null;
		}

		return JSON.parse(result[0].shard_ids) as number[];
	}

	/**
	 * Add a shard ID to an index entry for a specific value
	 * Used for synchronous index maintenance during INSERT operations
	 *
	 * @param indexName - Index name
	 * @param keyValue - The indexed value
	 * @param shardId - Shard ID to add
	 */
	async addShardToIndexEntry(indexName: string, keyValue: string, shardId: number): Promise<void> {
		this.ensureCreated();

		const now = Date.now();

		// Get existing entry
		const existing = await this.getIndexedShards(indexName, keyValue);

		if (existing === null) {
			// Create new entry with this shard
			this.ctx.storage.sql.exec(
				`INSERT INTO virtual_index_entries (index_name, key_value, shard_ids, updated_at)
				 VALUES (?, ?, ?, ?)`,
				indexName,
				keyValue,
				JSON.stringify([shardId]),
				now,
			);
		} else {
			// Add shard to existing entry if not already present
			if (!existing.includes(shardId)) {
				const updatedShardIds = [...existing, shardId].sort((a, b) => a - b);
				this.ctx.storage.sql.exec(
					`UPDATE virtual_index_entries SET shard_ids = ?, updated_at = ?
					 WHERE index_name = ? AND key_value = ?`,
					JSON.stringify(updatedShardIds),
					now,
					indexName,
					keyValue,
				);
			}
		}
	}

	/**
	 * Remove a shard ID from an index entry for a specific value
	 * Used for synchronous index maintenance during DELETE and UPDATE operations
	 * Deletes the entry if it becomes empty
	 *
	 * @param indexName - Index name
	 * @param keyValue - The indexed value
	 * @param shardId - Shard ID to remove
	 */
	async removeShardFromIndexEntry(indexName: string, keyValue: string, shardId: number): Promise<void> {
		this.ensureCreated();

		const now = Date.now();

		// Get existing entry
		const existing = await this.getIndexedShards(indexName, keyValue);

		if (existing === null || !existing.includes(shardId)) {
			// Entry doesn't exist or shard not present - nothing to do
			return;
		}

		// Remove shard from list
		const updatedShardIds = existing.filter((id) => id !== shardId);

		if (updatedShardIds.length === 0) {
			// No more shards - delete the entry
			this.ctx.storage.sql.exec(
				`DELETE FROM virtual_index_entries WHERE index_name = ? AND key_value = ?`,
				indexName,
				keyValue,
			);
		} else {
			// Update with remaining shards
			this.ctx.storage.sql.exec(
				`UPDATE virtual_index_entries SET shard_ids = ?, updated_at = ?
				 WHERE index_name = ? AND key_value = ?`,
				JSON.stringify(updatedShardIds),
				now,
				indexName,
				keyValue,
			);
		}
	}

	/**
	 * Delete a virtual index and all its entries
	 *
	 * @param indexName - Index to delete
	 */
	async dropVirtualIndex(indexName: string): Promise<{ success: boolean }> {
		this.ensureCreated();

		// CASCADE will automatically delete index entries
		this.ctx.storage.sql.exec(`DELETE FROM virtual_indexes WHERE index_name = ?`, indexName);

		return { success: true };
	}

	/**
	 * Get all index entries that include a specific shard
	 * Used by async index maintenance to rebuild index state
	 */
	async getIndexEntriesForShard(indexName: string, shardId: number): Promise<Array<{ key_value: string }>> {
		this.ensureCreated();

		const entries = this.ctx.storage.sql
			.exec(
				`SELECT key_value, shard_ids FROM virtual_index_entries
		     WHERE index_name = ?`,
				indexName
			)
			.toArray() as Array<{ key_value: string; shard_ids: string }>;

		return entries
			.filter((entry) => {
				const shardIds = JSON.parse(entry.shard_ids);
				return shardIds.includes(shardId);
			})
			.map((entry) => ({ key_value: entry.key_value }));
	}

	/**
	 * Apply a batch of index changes in a single call
	 * Used by async index maintenance queue consumer
	 */
	async batchMaintainIndexes(
		changes: Array<{
			operation: 'add' | 'remove';
			index_name: string;
			key_value: string;
			shard_id: number;
		}>
	): Promise<void> {
		this.ensureCreated();

		for (const change of changes) {
			if (change.operation === 'add') {
				await this.addShardToIndexEntry(change.index_name, change.key_value, change.shard_id);
			} else {
				await this.removeShardFromIndexEntry(change.index_name, change.key_value, change.shard_id);
			}
		}
	}

	/**
	 * Alarm handler - runs every 5 minutes to check storage capacity and status
	 */
	async alarm(): Promise<void> {
		// Get all storage nodes
		const nodes = this.ctx.storage.sql.exec(`SELECT node_id FROM storage_nodes`).toArray() as unknown as { node_id: string }[];

		const now = Date.now();

		// Check each storage node's capacity and availability
		for (const node of nodes) {
			try {
				// Get the Storage DO stub
				const storageId = this.env.STORAGE.idFromName(node.node_id);
				const storageStub = this.env.STORAGE.get(storageId);

				// Fetch the database size
				const size = await storageStub.getDatabaseSize();

				// Update capacity, set status to active, clear any previous error, and update timestamp
				this.ctx.storage.sql.exec(
					`UPDATE storage_nodes SET capacity_used = ?, status = ?, error = NULL, updated_at = ? WHERE node_id = ?`,
					size,
					'active',
					now,
					node.node_id,
				);
			} catch (error) {
				// If we couldn't reach the storage node, mark it as down, store the error, and update timestamp
				const errorMessage = error instanceof Error ? error.message : String(error);
				this.ctx.storage.sql.exec(
					`UPDATE storage_nodes SET status = ?, error = ?, updated_at = ? WHERE node_id = ?`,
					'down',
					errorMessage,
					now,
					node.node_id,
				);
			}
		}

		// Schedule the next alarm in 5 minutes
		this.ctx.storage.setAlarm(Date.now() + 5 * 60 * 1000);
	}
}
