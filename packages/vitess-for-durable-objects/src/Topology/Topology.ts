import { DurableObject } from 'cloudflare:workers';

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
	column_name: string;
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
				column_name TEXT NOT NULL,
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
		columnName: string,
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
		this.ctx.storage.sql.exec(
			`INSERT INTO virtual_indexes (index_name, table_name, column_name, index_type, status, error_message, created_at, updated_at)
			 VALUES (?, ?, ?, ?, 'building', NULL, ?, ?)`,
			indexName,
			tableName,
			columnName,
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
