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

export interface TopologyData {
	storage_nodes: StorageNode[];
	tables: TableMetadata[];
	table_shards: TableShard[];
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

		// Create storage nodes
		for (let i = 0; i < numNodes; i++) {
			const nodeId = `node-${i}`;
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
	 * @returns Complete topology data including storage nodes and tables
	 */
	async getTopology(): Promise<TopologyData> {
		this.ensureCreated();
		const storage_nodes = this.ctx.storage.sql.exec(`SELECT * FROM storage_nodes`).toArray() as unknown as StorageNode[];

		const tables = this.ctx.storage.sql.exec(`SELECT * FROM tables`).toArray() as unknown as TableMetadata[];

		const table_shards = this.ctx.storage.sql.exec(`SELECT * FROM table_shards ORDER BY table_name, shard_id`).toArray() as unknown as TableShard[];

		return {
			storage_nodes,
			tables,
			table_shards,
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
