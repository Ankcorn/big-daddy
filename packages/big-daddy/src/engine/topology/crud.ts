/**
 * CRUD operations for Topology
 *
 * Handles basic create, read, update operations for topology:
 * - create: Initialize cluster topology
 * - getTopology: Read complete topology data
 * - updateTopology: Batch update topology metadata
 */

import type {
	AsyncJob,
	ReshardingState,
	ShardRowCount,
	SqlParam,
	StorageNode,
	TableMetadata,
	TableShard,
	TopologyData,
	TopologyUpdates,
	VirtualIndex,
	VirtualIndexEntry,
} from "./types";

export class CRUDOperations {
	constructor(private storage: DurableObjectStorage) {}

	/**
	 * Create the cluster topology with a fixed number of storage nodes
	 * This can only be called once - storage nodes cannot be modified after creation
	 *
	 * @param numNodes - Number of storage nodes in the cluster
	 * @returns Success status or error
	 */
	async create(
		numNodes: number,
	): Promise<{ success: boolean; error?: string }> {
		// Check if already created
		const result = this.storage.sql
			.exec(`SELECT value FROM cluster_metadata WHERE key = 'created'`)
			.toArray() as unknown as {
			value: string;
		}[];

		if (result.length > 0 && result[0]?.value === "true") {
			throw new Error(
				"Topology already created. Storage nodes cannot be modified after creation.",
			);
		}

		if (numNodes < 1) {
			return { success: false, error: "Number of nodes must be at least 1" };
		}

		const now = Date.now();

		// Mark as created
		this.storage.sql.exec(
			`INSERT INTO cluster_metadata (key, value) VALUES ('created', 'true')`,
		);

		// Store number of nodes
		this.storage.sql.exec(
			`INSERT INTO cluster_metadata (key, value) VALUES ('num_nodes', ?)`,
			String(numNodes),
		);

		// Create storage nodes with unique, unguessable IDs
		for (let i = 0; i < numNodes; i++) {
			// Generate a unique ID using crypto.randomUUID()
			const nodeId = crypto.randomUUID();
			this.storage.sql.exec(
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
		const storage_nodes = this.storage.sql
			.exec(`SELECT * FROM storage_nodes`)
			.toArray() as unknown as StorageNode[];

		const tables = this.storage.sql
			.exec(`SELECT * FROM tables`)
			.toArray() as unknown as TableMetadata[];

		const table_shards = this.storage.sql
			.exec(`SELECT * FROM table_shards ORDER BY table_name, shard_id`)
			.toArray() as unknown as TableShard[];

		const virtual_indexes = this.storage.sql
			.exec(`SELECT * FROM virtual_indexes`)
			.toArray() as unknown as VirtualIndex[];

		const virtual_index_entries = this.storage.sql
			.exec(`SELECT * FROM virtual_index_entries`)
			.toArray() as unknown as VirtualIndexEntry[];

		const resharding_states = this.storage.sql
			.exec(`SELECT * FROM resharding_states`)
			.toArray() as unknown as ReshardingState[];

		const async_jobs = this.storage.sql
			.exec(`SELECT * FROM async_jobs ORDER BY created_at DESC`)
			.toArray() as unknown as AsyncJob[];

		return {
			storage_nodes,
			tables,
			table_shards,
			virtual_indexes,
			virtual_index_entries,
			resharding_states,
			async_jobs,
		};
	}

	/**
	 * Batch update topology information in a single transaction
	 * Note: Storage nodes cannot be modified - they are immutable after creation
	 *
	 * @param updates - Object containing all changes to apply
	 * @returns Success status
	 */
	async updateTopology(
		updates: TopologyUpdates,
	): Promise<{ success: boolean }> {
		const now = Date.now();

		// Add new tables
		if (updates.tables?.add) {
			// Get storage nodes for shard distribution
			const nodes = this.storage.sql
				.exec(`SELECT node_id FROM storage_nodes WHERE status = 'active'`)
				.toArray() as unknown as {
				node_id: string;
			}[];

			if (nodes.length === 0) {
				throw new Error("No active storage nodes available");
			}

			for (const table of updates.tables.add) {
				// Insert table metadata
				this.storage.sql.exec(
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
					const nodeId = nodes[nodeIndex]?.node_id;

					this.storage.sql.exec(
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
				const values: SqlParam[] = [];

				if (table.num_shards !== undefined) {
					setClauses.push("num_shards = ?");
					values.push(table.num_shards);
				}
				if (table.block_size !== undefined) {
					setClauses.push("block_size = ?");
					values.push(table.block_size);
				}

				if (setClauses.length > 0) {
					// Always update the updated_at timestamp
					setClauses.push("updated_at = ?");
					values.push(now);
					values.push(table.table_name);
					this.storage.sql.exec(
						`UPDATE tables SET ${setClauses.join(", ")} WHERE table_name = ?`,
						...values,
					);
				}
			}
		}

		// Remove tables
		if (updates.tables?.remove) {
			for (const table_name of updates.tables.remove) {
				this.storage.sql.exec(
					`DELETE FROM tables WHERE table_name = ?`,
					table_name,
				);
			}
		}

		return { success: true };
	}

	// ============================================================================
	// Row Count Operations
	// ============================================================================

	/**
	 * Get row counts for all shards of a table
	 *
	 * @param tableName - Name of the table
	 * @returns Array of per-shard row counts
	 */
	getTableShardRowCounts(tableName: string): ShardRowCount[] {
		return this.storage.sql
			.exec(
				`SELECT table_name, shard_id, row_count, updated_at 
				 FROM table_shards 
				 WHERE table_name = ? AND status = 'active'
				 ORDER BY shard_id`,
				tableName,
			)
			.toArray() as unknown as ShardRowCount[];
	}

	/**
	 * Bump (increment or decrement) the row count for a specific shard
	 * Clamps at 0 to prevent negative counts
	 *
	 * @param tableName - Name of the table
	 * @param shardId - ID of the shard
	 * @param delta - Amount to change (positive for inserts, negative for deletes)
	 */
	bumpTableShardRowCount(
		tableName: string,
		shardId: number,
		delta: number,
	): void {
		const now = Date.now();
		// Use MAX(0, ...) to clamp at 0
		this.storage.sql.exec(
			`UPDATE table_shards 
			 SET row_count = MAX(0, row_count + ?), updated_at = ?
			 WHERE table_name = ? AND shard_id = ?`,
			delta,
			now,
			tableName,
			shardId,
		);
	}

	/**
	 * Batch bump row counts for multiple shards of a table
	 * More efficient than calling bumpTableShardRowCount multiple times
	 *
	 * @param tableName - Name of the table
	 * @param deltaByShard - Map of shard_id to delta (amount to change)
	 */
	batchBumpTableShardRowCounts(
		tableName: string,
		deltaByShard: Map<number, number>,
	): void {
		const now = Date.now();
		for (const [shardId, delta] of deltaByShard) {
			this.storage.sql.exec(
				`UPDATE table_shards 
				 SET row_count = MAX(0, row_count + ?), updated_at = ?
				 WHERE table_name = ? AND shard_id = ?`,
				delta,
				now,
				tableName,
				shardId,
			);
		}
	}

	/**
	 * Set row counts for shards (used after resharding to set exact counts)
	 *
	 * @param tableName - Name of the table
	 * @param countsByShardId - Map of shard_id to row_count
	 */
	setTableShardRowCounts(
		tableName: string,
		countsByShardId: Map<number, number>,
	): void {
		const now = Date.now();
		for (const [shardId, rowCount] of countsByShardId) {
			this.storage.sql.exec(
				`UPDATE table_shards 
				 SET row_count = ?, updated_at = ?
				 WHERE table_name = ? AND shard_id = ?`,
				rowCount,
				now,
				tableName,
				shardId,
			);
		}
	}
}
