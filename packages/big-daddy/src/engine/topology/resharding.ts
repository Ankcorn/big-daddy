/**
 * Resharding operations for Topology
 *
 * Handles all resharding-related operations:
 * - createPendingShards: Create new shards for resharding
 * - getReshardingState: Get current resharding status
 * - startResharding: Mark resharding as in progress
 * - markReshardingComplete: Complete resharding operation
 * - markReshardingFailed: Mark resharding as failed
 * - atomicStatusSwitch: Atomic switch from pending to active shards
 * - deleteVirtualShard: Delete old shards after resharding complete
 */

import { logger } from "../../logger";
import type { Storage } from "../storage";
import type { ReshardingState, TableMetadata, TableShard } from "./types";

export class ReshardingOperations {
	constructor(
		private storage: DurableObjectStorage,
		private env?: Env,
	) {}

	/**
	 * Create pending shards for resharding operation
	 *
	 * Creates new shards with status='pending' and initializes resharding state.
	 * The new shards won't be used by the conductor until marked 'active'.
	 *
	 * @param tableName - Table to reshard
	 * @param newShardCount - Number of new shards to create
	 * @param changeLogId - ID for tracking changes during resharding
	 * @returns Array of newly created pending shards
	 */
	async createPendingShards(
		tableName: string,
		newShardCount: number,
		changeLogId: string,
	): Promise<TableShard[]> {
		// Validate table exists
		const tables = this.storage.sql
			.exec(`SELECT * FROM tables WHERE table_name = ?`, tableName)
			.toArray() as unknown as TableMetadata[];

		if (!tables.length) {
			throw new Error(`Table '${tableName}' not found`);
		}

		// Validate shard count
		if (newShardCount < 1 || newShardCount > 256) {
			throw new Error(
				`Invalid shard count: ${newShardCount}. Must be between 1 and 256`,
			);
		}

		// Get all active nodes for distribution
		const nodes = this.storage.sql
			.exec(`SELECT node_id FROM storage_nodes WHERE status = 'active'`)
			.toArray() as unknown as Array<{ node_id: string }>;

		if (!nodes.length) {
			throw new Error("No active storage nodes available");
		}

		// Get the current max shard_id for this table
		const maxShardResult = this.storage.sql
			.exec(
				`SELECT MAX(shard_id) as max_id FROM table_shards WHERE table_name = ?`,
				tableName,
			)
			.toArray() as unknown as { max_id: number | null }[];

		const maxShardId = maxShardResult[0]?.max_id ?? -1;

		// Create new pending shards, distributed across nodes
		const now = Date.now();
		const newShards: TableShard[] = [];

		for (let i = 0; i < newShardCount; i++) {
			const shardId = maxShardId + 1 + i;
			const nodeIndex = shardId % nodes.length;
			const nodeId = nodes[nodeIndex]!.node_id;

			this.storage.sql.exec(
				`INSERT INTO table_shards (table_name, shard_id, node_id, status, row_count, created_at, updated_at)
				 VALUES (?, ?, ?, 'pending', 0, ?, ?)`,
				tableName,
				shardId,
				nodeId,
				now,
				now,
			);

			newShards.push({
				table_name: tableName,
				shard_id: shardId,
				node_id: nodeId,
				status: "pending",
				row_count: 0,
				created_at: now,
				updated_at: now,
			});
		}

		// Get the source (old) shard - assume shard_id = 0 (default single shard)
		const sourceShards = this.storage.sql
			.exec(
				`SELECT shard_id FROM table_shards WHERE table_name = ? AND status = 'active' ORDER BY shard_id LIMIT 1`,
				tableName,
			)
			.toArray() as unknown as { shard_id: number }[];

		const sourceShardId = sourceShards[0]?.shard_id ?? 0;

		// Create resharding state (or replace if previous attempt failed)
		const targetShardIds = newShards.map((s) => s.shard_id);
		// First, delete any existing resharding state for this table (allows retry after failure)
		this.storage.sql.exec(
			`DELETE FROM resharding_states WHERE table_name = ?`,
			tableName,
		);
		// Now insert the new resharding state
		this.storage.sql.exec(
			`INSERT INTO resharding_states (table_name, source_shard_id, target_shard_ids, change_log_id, status, error_message, created_at, updated_at)
			 VALUES (?, ?, ?, ?, 'pending_shards', NULL, ?, ?)`,
			tableName,
			sourceShardId,
			JSON.stringify(targetShardIds),
			changeLogId,
			now,
			now,
		);

		return newShards;
	}

	/**
	 * Get the current resharding state for a table
	 * Returns null if no resharding is in progress
	 */
	async getReshardingState(tableName: string): Promise<ReshardingState | null> {
		const result = this.storage.sql
			.exec(`SELECT * FROM resharding_states WHERE table_name = ?`, tableName)
			.toArray() as unknown as ReshardingState[];

		return result.length > 0 ? result[0]! : null;
	}

	/**
	 * Start resharding: transition from 'pending_shards' to 'copying' status
	 * This is called when the queue worker begins Phase 3B (copy data)
	 */
	async startResharding(tableName: string): Promise<void> {
		const now = Date.now();
		this.storage.sql.exec(
			`UPDATE resharding_states SET status = 'copying', updated_at = ? WHERE table_name = ?`,
			now,
			tableName,
		);
	}

	/**
	 * Mark resharding as complete: transition to 'complete' status
	 * This is called after atomic status switch completes
	 */
	async markReshardingComplete(tableName: string): Promise<void> {
		const now = Date.now();
		this.storage.sql.exec(
			`UPDATE resharding_states SET status = 'complete', updated_at = ? WHERE table_name = ?`,
			now,
			tableName,
		);
	}

	/**
	 * Mark resharding as failed: transition to 'failed' status with error message
	 * This is called if any phase fails
	 */
	async markReshardingFailed(
		tableName: string,
		errorMessage: string,
	): Promise<void> {
		const now = Date.now();
		this.storage.sql.exec(
			`UPDATE resharding_states SET status = 'failed', error_message = ?, updated_at = ? WHERE table_name = ?`,
			errorMessage,
			now,
			tableName,
		);
	}

	/**
	 * Perform atomic status switch: mark target shards as 'active' and source shard as 'to_be_deleted'
	 * This is called after data verification passes
	 * Must be atomic to prevent concurrent writes during the switch
	 */
	async atomicStatusSwitch(tableName: string): Promise<void> {
		// Get resharding state
		const reshardingState = await this.getReshardingState(tableName);
		if (!reshardingState) {
			throw new Error(`No resharding in progress for table '${tableName}'`);
		}

		const targetShardIds: number[] = JSON.parse(
			reshardingState.target_shard_ids,
		);
		const sourceShardId = reshardingState.source_shard_id;
		const now = Date.now();

		// Note: Cloudflare Durable Objects automatically coalesces all SQL operations
		// within a single request into atomic writes, so explicit BEGIN/COMMIT is not needed
		// and not supported. The multiple updates below are executed atomically.

		// Mark all target shards as active
		for (const shardId of targetShardIds) {
			this.storage.sql.exec(
				`UPDATE table_shards SET status = 'active', updated_at = ? WHERE table_name = ? AND shard_id = ?`,
				now,
				tableName,
				shardId,
			);
		}

		// Mark source shard as to_be_deleted
		this.storage.sql.exec(
			`UPDATE table_shards SET status = 'to_be_deleted', updated_at = ? WHERE table_name = ? AND shard_id = ?`,
			now,
			tableName,
			sourceShardId,
		);

		// Update resharding state to 'verifying' (will be set to 'complete' after cleanup)
		this.storage.sql.exec(
			`UPDATE resharding_states SET status = 'verifying', updated_at = ? WHERE table_name = ?`,
			now,
			tableName,
		);
	}

	/**
	 * Delete a virtual shard
	 * Only allows deletion of shards with status 'to_be_deleted', 'pending', or 'failed'
	 * Throws error if attempting to delete an active shard
	 *
	 * Used after resharding to clean up old shards, or to remove failed/pending shards
	 *
	 * @param tableName - Table that owns the shard
	 * @param shardId - ID of the shard to delete
	 * @returns Success status
	 */
	async deleteVirtualShard(
		tableName: string,
		shardId: number,
	): Promise<{ success: boolean; error?: string }> {
		// Get the shard to check its status and node location
		const shards = this.storage.sql
			.exec(
				`SELECT status, node_id FROM table_shards WHERE table_name = ? AND shard_id = ?`,
				tableName,
				shardId,
			)
			.toArray() as unknown as { status: string; node_id: string }[];

		if (shards.length === 0) {
			return {
				success: false,
				error: `Shard ${shardId} not found for table '${tableName}'`,
			};
		}

		const shard = shards[0]!;

		// Throw error if attempting to delete an active shard
		if (shard.status === "active") {
			throw new Error(
				`Cannot delete active shard ${shardId} from table '${tableName}'. ` +
					`Only shards with status 'to_be_deleted', 'pending', or 'failed' can be deleted. ` +
					`Use resharding to migrate data before deleting.`,
			);
		}

		// Delete shard data from physical storage
		if (this.env) {
			try {
				const storageId = this.env.STORAGE.idFromName(shard.node_id);
				const storageStub = this.env.STORAGE.get(
					storageId,
				) as DurableObjectStub<Storage>;

				// Delete all rows for this shard from the table
				await storageStub.executeQuery({
					query: `DELETE FROM ${tableName} WHERE _virtualShard = ?`,
					params: [shardId],
				});

				logger.info`Virtual shard data deleted from storage ${{ tableName }} ${{ shardId }} ${{ nodeId: shard.node_id }}`;
			} catch (error) {
				logger.error`Failed to delete virtual shard data from storage ${{ tableName }} ${{ shardId }} ${{ nodeId: shard.node_id }} ${{ error: error instanceof Error ? error.message : String(error) }}`;
				// Continue to delete from topology even if storage deletion fails
			}
		}

		// Delete the shard metadata from topology
		this.storage.sql.exec(
			`DELETE FROM table_shards WHERE table_name = ? AND shard_id = ?`,
			tableName,
			shardId,
		);

		logger.info`Virtual shard deleted ${{ tableName }} ${{ shardId }} ${{ previousStatus: shard.status }}`;

		return { success: true };
	}

	/**
	 * Mark all target shards as 'failed' status
	 * Called when resharding fails to clean up pending shards
	 *
	 * @param tableName - Table being resharded
	 * @param targetShardIds - Array of target shard IDs to mark as failed
	 */
	async markTargetShardsAsFailed(
		tableName: string,
		targetShardIds: number[],
	): Promise<void> {
		const now = Date.now();

		for (const shardId of targetShardIds) {
			this.storage.sql.exec(
				`UPDATE table_shards SET status = 'failed', updated_at = ? WHERE table_name = ? AND shard_id = ?`,
				now,
				tableName,
				shardId,
			);
		}

		logger.info`Target shards marked as failed ${{ tableName }} ${{ targetShardIds }}`;
	}
}
