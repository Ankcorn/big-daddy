import { logger } from "../../../logger";
import type { QueryHandlerContext, QueryResult } from "../types";

/**
 * Handle ALTER TABLE statement execution
 *
 * Supports:
 * - RENAME TO: Rename table
 * - MODIFY block_size: Change block size per shard
 *
 * Does NOT support (would require migration):
 * - ADD COLUMN / DROP COLUMN
 * - MODIFY data types
 * - CHANGE PRIMARY KEY
 */
export async function handleAlterTable(
	tableName: string,
	alterationType: string,
	alterationValue: string | number,
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, topology } = context;

	try {
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find table
		const tableMetadata = topologyData.tables.find(
			(t) => t.table_name === tableName,
		);
		if (!tableMetadata) {
			throw new Error(`Table '${tableName}' not found`);
		}

		let updateData: {
			tables: {
				update?: typeof topologyData.tables;
				add?: typeof topologyData.tables;
				remove?: string[];
			};
		} = { tables: { update: [] } };

		if (alterationType === "RENAME") {
			const newTableName = alterationValue as string;

			// Check if new name already exists
			const existingTable = topologyData.tables.find(
				(t) => t.table_name === newTableName,
			);
			if (existingTable) {
				throw new Error(`Table '${newTableName}' already exists`);
			}

			// For rename, we need to:
			// 1. Remove old table metadata
			// 2. Add new table metadata with updated name
			// This is a simplified approach - full rename would also need to update shards/indexes

			const renamedMetadata = {
				...tableMetadata,
				table_name: newTableName,
				updated_at: Date.now(),
			};

			updateData = {
				tables: {
					remove: [tableName],
					add: [renamedMetadata],
				},
			};

			logger.info`ALTER TABLE RENAME initiated ${{ oldName: tableName }} ${{ newName: newTableName }}`;
		} else if (alterationType === "MODIFY_BLOCK_SIZE") {
			const newBlockSize = alterationValue as number;

			if (
				!Number.isInteger(newBlockSize) ||
				newBlockSize < 1 ||
				newBlockSize > 10000
			) {
				throw new Error(
					`Invalid block size: ${newBlockSize}. Must be between 1 and 10000`,
				);
			}

			const updatedMetadata = {
				...tableMetadata,
				block_size: newBlockSize,
				updated_at: Date.now(),
			};

			updateData = {
				tables: {
					update: [updatedMetadata],
				},
			};

			logger.info`ALTER TABLE MODIFY block_size ${{ table: tableName }} ${{ oldBlockSize: tableMetadata.block_size }} ${{ newBlockSize }}`;
		} else {
			throw new Error(`Unsupported ALTER operation: ${alterationType}`);
		}

		// Update topology
		await topologyStub.updateTopology(updateData);

		logger.info`ALTER TABLE completed successfully ${{ table: tableName }} ${{ operation: alterationType }}`;

		return {
			rows: [],
			rowsAffected: 0,
		};
	} catch (error) {
		throw new Error(`Failed to alter table: ${(error as Error).message}`);
	}
}

/**
 * Handle adding/removing shards (resharding wrapper)
 *
 * This is a convenience function that wraps the PRAGMA reshardTable operation
 * to provide a more SQL-like interface
 */
export async function handleReshardTable(
	tableName: string,
	newShardCount: number,
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, topology, indexQueue } = context;

	try {
		if (
			!Number.isInteger(newShardCount) ||
			newShardCount < 1 ||
			newShardCount > 256
		) {
			throw new Error(
				`Invalid shard count: ${newShardCount}. Must be between 1 and 256`,
			);
		}

		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find table
		const tableMetadata = topologyData.tables.find(
			(t) => t.table_name === tableName,
		);
		if (!tableMetadata) {
			throw new Error(`Table '${tableName}' not found`);
		}

		// Check if already resharding
		const activeResharding = topologyData.resharding_states.find(
			(s) =>
				s.table_name === tableName &&
				(s.status === "pending_shards" || s.status === "copying"),
		);

		if (activeResharding) {
			throw new Error(`Table '${tableName}' is already being resharded`);
		}

		// Check if shard count is the same
		if (newShardCount === tableMetadata.num_shards) {
			logger.warn`Requested shard count is same as current ${{ table: tableName }} ${{ currentShards: tableMetadata.num_shards }}`;
			return {
				rows: [],
				rowsAffected: 0,
			};
		}

		// Phase 1: Create pending shards and resharding state
		const changeLogId = crypto.randomUUID();
		const newShards = await topologyStub.createPendingShards(
			tableName,
			newShardCount,
			changeLogId,
		);

		logger.info`Pending shards created for resharding ${{ table: tableName }} ${{ oldShardCount: tableMetadata.num_shards }} ${{ newShardCount: newShards.length }} ${{ changeLogId }}`;

		// Phase 2: Enqueue resharding jobs for each active source shard
		if (indexQueue) {
			const sourceShardIds = topologyData.table_shards
				.filter((s) => s.table_name === tableName && s.status === "active")
				.sort((a, b) => a.shard_id - b.shard_id)
				.map((s) => s.shard_id);

			for (const sourceShardId of sourceShardIds) {
				await indexQueue.send({
					type: "reshard_table",
					database_id: databaseId,
					table_name: tableName,
					source_shard_id: sourceShardId,
					target_shard_ids: newShards.map((s) => s.shard_id),
					shard_key: tableMetadata.shard_key,
					shard_strategy: tableMetadata.shard_strategy,
					change_log_id: changeLogId,
					created_at: new Date().toISOString(),
					correlation_id: context.correlationId,
				});
			}
		}

		logger.info`Resharding jobs enqueued ${{ table: tableName }} ${{ changeLogId }}`;

		return {
			rows: [
				{
					change_log_id: changeLogId,
					status: "queued",
					message: `Resharding ${tableName} from ${tableMetadata.num_shards} shards to ${newShardCount} shards`,
					table_name: tableName,
					source_shard_count: tableMetadata.num_shards,
					target_shard_count: newShardCount,
				},
			],
		};
	} catch (error) {
		throw new Error(`Failed to reshard table: ${(error as Error).message}`);
	}
}
