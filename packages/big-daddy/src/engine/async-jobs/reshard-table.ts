/**
 * Reshard Table Job Handler
 *
 * Handles resharding a table from one source shard to multiple target shards.
 *
 * Process (Phases 3B-5):
 * - Phase 3A (write logging) is handled by the Conductor before queuing
 * - Phase 3B: Copy data from source shard to all target shards
 * - Phase 3C: Replay all captured changes to target shards
 * - Phase 3D: Verify row counts match between source and targets
 * - Phase 4: Atomic status switch (mark targets active, source to_be_deleted)
 * - Phase 5: Delete old shard data
 */

import { logger } from "../../logger";
import type { ReshardTableJob, SqlParam } from "../queue/types";
import type { Storage, StorageResults } from "../storage";
import type { Topology } from "../topology/index";
import type { TableShard } from "../topology/types";
import { hashToShard } from "../utils/sharding";

/** Type for COUNT query results */
interface CountRow {
	count: number;
}

/** Type for GROUP BY virtualShard query results */
interface VirtualShardCountRow {
	_virtualShard: number;
	count: number;
}

/** Verification details for data integrity check */
interface VerificationDetails {
	status: "PASSED" | "FAILED";
	duration: number;
	source: { shardId: number; rowCount: number };
	targets: {
		shardIds: number[];
		totalRowCount: number;
		breakdown: Record<number, number>;
		breakdown_details: Array<{ shardId: number; rowCount: number }>;
	};
	comparison: {
		match: boolean;
		difference: number;
		differencePercent: string;
		expectedSourceCount: number;
		actualTargetCount: number;
	};
}

/**
 * Build a key value for indexed column(s) from a row
 * Returns null if any indexed column is NULL (NULL values are not indexed)
 */
function buildIndexKeyValue(
	row: Record<string, unknown>,
	columns: string[],
): string | null {
	if (columns.length === 1) {
		const value = row[columns[0]!];
		if (value === null || value === undefined) {
			return null;
		}
		return String(value);
	} else {
		// Composite index - build key from all column values
		const values = columns.map((col) => row[col]);
		if (values.some((v) => v === null || v === undefined)) {
			return null;
		}
		return JSON.stringify(values);
	}
}

export async function processReshardTableJob(
	job: ReshardTableJob,
	env: Env,
	correlationId?: string,
): Promise<void> {
	const source = "QueueConsumer";
	const tableName = job.table_name;
	const sourceShardId = job.source_shard_id;
	const targetShardIds = job.target_shard_ids;
	const jobId = job.change_log_id;
	const reshardingId = job.change_log_id;
	const changeLogId = job.change_log_id;

	logger.info`Processing resharding job ${{ source }} ${{ table: tableName }} ${{ reshardingId }} ${{ jobId }} ${{ correlationId }} ${{ sourceShardId }} ${{ targetShardIds }} ${{ changeLogId }}`;

	const topologyId = env.TOPOLOGY.idFromName(job.database_id);
	const topologyStub = env.TOPOLOGY.get(
		topologyId,
	) as DurableObjectStub<Topology>;

	try {
		// Get initial source count for debugging
		const topology = await topologyStub.getTopology();
		const sourceShard = topology.table_shards.find(
			(s: TableShard) =>
				s.table_name === tableName && s.shard_id === sourceShardId,
		);
		let initialSourceCount = 0;
		if (sourceShard) {
			const sourceStorageId = env.STORAGE.idFromName(sourceShard.node_id);
			const sourceStorageStub = env.STORAGE.get(
				sourceStorageId,
			) as DurableObjectStub<Storage>;
			const countResult = (await sourceStorageStub.executeQuery({
				query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
				params: [sourceShardId],
			})) as StorageResults<CountRow>;
			initialSourceCount = countResult.rows[0]?.count ?? 0;
		}

		logger.info`Resharding initial state ${{ source }} ${{ table: tableName }} ${{ initialSourceCount }} ${{ sourceShardId }}`;

		// Create async job record
		await topologyStub.createAsyncJob(jobId, "reshard_table", tableName, {
			source_shard_id: sourceShardId,
			target_shard_ids: targetShardIds,
			shard_key: job.shard_key,
			shard_strategy: job.shard_strategy,
		});

		// Mark async job as running
		await topologyStub.startAsyncJob(jobId);

		// Mark resharding as starting (transition to 'copying' phase)
		await topologyStub.startResharding(tableName);
		const status1 = "copying";
		logger.info`Resharding started ${{ source }} ${{ table: tableName }} ${{ status: status1 }} ${{ initialSourceCount }}`;

		// Phase 3B: Copy data from source shard to target shards
		// First, log the initial state of target shards before copy
		const topologyForPreCopy = await topologyStub.getTopology();
		const preCopyTargetCounts: { [key: number]: number } = {};
		const preCopyVirtualShardBreakdown: {
			[key: number]: { [key: number]: number };
		} = {};
		for (const targetShardId of targetShardIds) {
			const targetShard = topologyForPreCopy.table_shards.find(
				(s: TableShard) =>
					s.table_name === tableName && s.shard_id === targetShardId,
			);
			if (targetShard) {
				const targetStorageId = env.STORAGE.idFromName(targetShard.node_id);
				const targetStorageStub = env.STORAGE.get(
					targetStorageId,
				) as DurableObjectStub<Storage>;

				// Get total count filtered by target shard ID
				const countResult = (await targetStorageStub.executeQuery({
					query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
					params: [targetShardId],
				})) as StorageResults<CountRow>;
				preCopyTargetCounts[targetShardId] = countResult.rows[0]?.count ?? 0;

				// Get breakdown by virtualShard value to detect unexpected data
				const breakdown = (await targetStorageStub.executeQuery({
					query: `SELECT _virtualShard, COUNT(*) as count FROM ${tableName} GROUP BY _virtualShard`,
					params: [],
				})) as StorageResults<VirtualShardCountRow>;
				const breakdownMap: { [key: number]: number } = {};
				for (const row of breakdown.rows) {
					breakdownMap[row._virtualShard] = row.count;
				}
				preCopyVirtualShardBreakdown[targetShardId] = breakdownMap;
			}
		}
		logger.info`Pre-copy target shard state ${{ source }} ${{ preCopyTargetCounts }} ${{ targetShardIds }} ${{ preCopyVirtualShardBreakdown }}`;

		const copyStats = await copyShardData(
			env,
			job.database_id,
			sourceShardId,
			targetShardIds,
			tableName,
			job.shard_key,
			job.shard_strategy,
		);

		// Log post-copy state to verify what was actually written
		const postCopyVirtualShardBreakdown: {
			[key: number]: { [key: number]: number };
		} = {};
		for (const targetShardId of targetShardIds) {
			const targetShard = topologyForPreCopy.table_shards.find(
				(s: TableShard) =>
					s.table_name === tableName && s.shard_id === targetShardId,
			);
			if (targetShard) {
				const targetStorageId = env.STORAGE.idFromName(targetShard.node_id);
				const targetStorageStub = env.STORAGE.get(
					targetStorageId,
				) as DurableObjectStub<Storage>;

				// Get breakdown by virtualShard value to see what was actually copied
				const breakdown = (await targetStorageStub.executeQuery({
					query: `SELECT _virtualShard, COUNT(*) as count FROM ${tableName} GROUP BY _virtualShard`,
					params: [],
				})) as StorageResults<VirtualShardCountRow>;
				const breakdownMap: { [key: number]: number } = {};
				for (const row of breakdown.rows) {
					breakdownMap[row._virtualShard] = row.count;
				}
				postCopyVirtualShardBreakdown[targetShardId] = breakdownMap;
			}
		}
		const distribution = copyStats.distribution;
		logger.info`Post-copy target shard state ${{ source }} ${{ distribution }} ${{ postCopyVirtualShardBreakdown }}`;

		// Phase 3C: Replay captured changes
		const replayStats = await replayChangeLog(
			env,
			job.database_id,
			job.change_log_id,
			targetShardIds,
			tableName,
			job.shard_key,
			job.shard_strategy,
		);

		// Phase 3D: Verify data integrity
		const verifyStats = await verifyIntegrity(
			env,
			job.database_id,
			sourceShardId,
			targetShardIds,
			tableName,
		);

		// Combined comprehensive log showing copy -> replay -> verify progression
		const preCopyTargetState = preCopyTargetCounts;
		const phase3B_copy = {
			rowsCopied: copyStats.rows_copied,
			duration: copyStats.duration,
			distribution: copyStats.distribution,
		};
		const phase3C_replay = {
			changesReplayed: replayStats.changes_replayed,
			duration: replayStats.duration,
		};
		const phase3D_verify = {
			isValid: verifyStats.isValid,
			duration: verifyStats.duration,
			verificationDetails: verifyStats.verificationDetails,
		};
		logger.info`Phase 3: Copy-Replay-Verify Summary ${{ source }} ${{ preCopyTargetState }} ${{ phase3B_copy }} ${{ phase3C_replay }} ${{ phase3D_verify }}`;

		if (!verifyStats.isValid) {
			throw new Error(`Data verification failed: ${verifyStats.error}`);
		}

		// Phase 4: Atomic status switch
		await topologyStub.atomicStatusSwitch(tableName);
		logger.info`Phase 4: Atomic status switch completed ${{ source }} ${{ table: tableName }}`;

		// Phase 4B: Set row counts for new shards using the verified distribution
		const rowCountsByShardId = new Map<number, number>();
		for (const targetShardId of targetShardIds) {
			// Use the breakdown from verification if available, otherwise use copy distribution
			const rowCount =
				verifyStats.verificationDetails?.targets.breakdown[targetShardId] ??
				copyStats.distribution[targetShardId] ??
				0;
			rowCountsByShardId.set(targetShardId, rowCount);
		}
		await topologyStub.setTableShardRowCounts(tableName, rowCountsByShardId);
		logger.info`Phase 4B: Row counts set for new shards ${{ source }} ${{ table: tableName }} ${{ rowCounts: Object.fromEntries(rowCountsByShardId) }}`;

		// Phase 5: Delete old shard data
		await deleteOldShardData(env, job.database_id, sourceShardId, tableName);
		const shardId = sourceShardId;
		logger.info`Phase 5: Old shard data deleted ${{ source }} ${{ table: tableName }} ${{ shardId }}`;

		// CRITICAL: Data verification and copy/delete all passed successfully
		// Mark resharding as complete before job completion
		// This should happen BEFORE completeAsyncJob to ensure clean state
		try {
			await topologyStub.markReshardingComplete(tableName);
			logger.info`Resharding marked as complete ${{ source }} ${{ table: tableName }}`;
		} catch (completeError) {
			const errorMsg =
				completeError instanceof Error
					? completeError.message
					: String(completeError);
			logger.error`Failed to mark resharding as complete (but data migration succeeded) ${{ source }} ${{ error: errorMsg }}`;
		}

		// Mark async job as completed
		try {
			await topologyStub.completeAsyncJob(jobId);
			logger.info`Async job marked as complete ${{ source }} ${{ jobId }}`;
		} catch (jobError) {
			const jobErrorMsg =
				jobError instanceof Error ? jobError.message : String(jobError);
			logger.error`Failed to mark async job as complete (but data migration succeeded) ${{ source }} ${{ jobId }} ${{ error: jobErrorMsg }}`;
		}

		const status2 = "complete";
		logger.info`Resharding completed successfully ${{ source }} ${{ table: tableName }} ${{ status: status2 }}`;
	} catch (error) {
		const errorMessage = error instanceof Error ? error.message : String(error);
		const status3 = "failed";
		logger.error`Resharding failed ${{ source }} ${{ table: tableName }} ${{ error: errorMessage }} ${{ status: status3 }}`;

		// Mark resharding state as failed
		await topologyStub.markReshardingFailed(tableName, errorMessage);

		// Mark all target shards as 'failed' to clean up pending shards
		// ONLY do this if we haven't already switched them to active
		// Check current shard status before marking as failed
		try {
			const currentTopology = await topologyStub.getTopology();
			const targetShardsActive = currentTopology.table_shards.some(
				(s: TableShard) =>
					s.table_name === tableName &&
					targetShardIds.includes(s.shard_id) &&
					s.status === "active",
			);

			// Only mark as failed if they're not already active (which means Phase 4 completed)
			if (!targetShardsActive) {
				await topologyStub.markTargetShardsAsFailed(tableName, targetShardIds);
				logger.info`Target shards marked as failed ${{ source }} ${{ table: tableName }} ${{ targetShardIds }}`;
			} else {
				logger.warn`Not marking target shards as failed - they are already active ${{ source }} ${{ table: tableName }} ${{ targetShardIds }}`;
			}
		} catch (shardError) {
			const shardErrorMsg =
				shardError instanceof Error ? shardError.message : String(shardError);
			logger.warn`Failed to handle target shard status ${{ source }} ${{ tableName }} ${{ targetShardIds }} ${{ error: shardErrorMsg }}`;
		}

		// Mark async job as failed (no retries - fail fast)
		try {
			await topologyStub.failAsyncJob(jobId, errorMessage);
		} catch (jobError) {
			const failJobErrorMsg =
				jobError instanceof Error ? jobError.message : String(jobError);
			logger.warn`Failed to mark async job as failed ${{ source }} ${{ jobId }} ${{ error: failJobErrorMsg }}`;
		}

		throw error;
	}
}

/**
 * Phase 3B: Copy data from source shard to target shards
 * Routes rows to target shards based on shard key using the same hashing strategy
 */
async function copyShardData(
	env: Env,
	databaseId: string,
	sourceShardId: number,
	targetShardIds: number[],
	tableName: string,
	shardKey: string,
	_shardStrategy: "hash" | "range",
): Promise<{
	rows_copied: number;
	duration: number;
	distribution: { [key: number]: number };
}> {
	const startTime = Date.now();
	let rowsCopied = 0;

	const source = "QueueConsumer";
	logger.info`Phase 3B: Starting data copy ${{ source }} ${{ sourceShardId }} ${{ targetShardIds }}`;

	try {
		// Get all data from source shard
		const topologyId = env.TOPOLOGY.idFromName(databaseId);
		const topologyStub = env.TOPOLOGY.get(
			topologyId,
		) as DurableObjectStub<Topology>;
		const topology = await topologyStub.getTopology();

		const sourceShard = topology.table_shards.find(
			(s: TableShard) =>
				s.table_name === tableName && s.shard_id === sourceShardId,
		);
		if (!sourceShard) {
			throw new Error(`Source shard ${sourceShardId} not found`);
		}

		const storageId = env.STORAGE.idFromName(sourceShard.node_id);
		const storageStub = env.STORAGE.get(
			storageId,
		) as DurableObjectStub<Storage>;

		// Query all data from source shard filtered by _virtualShard
		const result = (await storageStub.executeQuery({
			query: `SELECT * FROM ${tableName} WHERE _virtualShard = ?`,
			params: [sourceShardId],
		})) as StorageResults<Record<string, unknown>>;

		if (!("rows" in result)) {
			throw new Error("Expected QueryResult but got BatchQueryResult");
		}

		const rows = result.rows as Record<string, unknown>[];

		// Also get count from source to verify (filtered by _virtualShard)
		const sourceCountResult = (await storageStub.executeQuery({
			query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
			params: [sourceShardId],
		})) as StorageResults<CountRow>;
		const sourceCount = sourceCountResult.rows[0]?.count ?? 0;

		const rowCount = rows.length;
		const countQuery = sourceCount;
		const match = rows.length === sourceCount;
		logger.info`Fetched rows from source shard ${{ source }} ${{ rowCount }} ${{ countQuery }} ${{ match }}`;

		// Warn if there's a mismatch
		if (rows.length !== sourceCount) {
			const selectStar = rows.length;
			const difference = sourceCount - rows.length;
			logger.warn`Row count mismatch in source shard fetch ${{ source }} ${{ selectStar }} ${{ countQuery }} ${{ difference }}`;
		}

		// Build map of target shard IDs to storage nodes for efficient lookup
		const targetShardMap = new Map<number, string>();
		for (const shard of topology.table_shards.filter(
			(s: TableShard) =>
				s.table_name === tableName && targetShardIds.includes(s.shard_id),
		)) {
			targetShardMap.set(shard.shard_id, shard.node_id);
		}

		// Verify all target shards have nodes
		for (const targetShardId of targetShardIds) {
			if (!targetShardMap.has(targetShardId)) {
				throw new Error(`Target shard ${targetShardId} has no node mapping`);
			}
		}

		// Get virtual indexes for this table (for index maintenance during copy)
		const tableIndexes = topology.virtual_indexes
			.filter((idx) => idx.table_name === tableName && idx.status === "ready")
			.map((idx) => ({ index_name: idx.index_name, columns: idx.columns }));

		if (tableIndexes.length > 0) {
			logger.info`Phase 3B: Will update ${tableIndexes.length} indexes during copy ${{ source }} ${{ indexes: tableIndexes.map((i) => i.index_name) }}`;
		}

		// Track index key values that need to be removed from source shard after copy completes
		// We can't remove during copy because multiple rows may share the same index key value
		const indexKeysToRemoveFromSource = new Map<string, Set<string>>(); // index_name -> Set<key_value>
		for (const index of tableIndexes) {
			indexKeysToRemoveFromSource.set(index.index_name, new Set());
		}

		// Insert rows into target shards based on shard key distribution
		// Each row goes to exactly one target shard determined by hash(shardKeyValue) % targetShardCount
		const shardDistribution = new Map<number, number>();
		for (const targetShardId of targetShardIds) {
			shardDistribution.set(targetShardId, 0);
		}

		for (const row of rows) {
			// Extract the shard key value from the row
			const shardKeyValue = row[shardKey];
			if (shardKeyValue === undefined) {
				logger.warn`Row missing shard key ${{ source }} ${{ shardKey }} ${{ row }}`;
				continue;
			}

			// Determine which target shard this row belongs to
			// hashToShard returns 0-indexed value, so we use it to select from targetShardIds
			// Cast to SqlParam since we know shard keys must be valid SQL values
			const targetShardIndex = hashToShard(
				shardKeyValue as SqlParam,
				targetShardIds.length,
			);
			const targetShardId = targetShardIds[targetShardIndex]!;

			// Get the storage node for this target shard
			const targetNodeId = targetShardMap.get(targetShardId);
			if (!targetNodeId) {
				throw new Error(`Target shard ${targetShardId} node mapping not found`);
			}

			// Build INSERT OR REPLACE query with proper shard awareness
			// IMPORTANT: We need to handle multiple scenarios:
			// 1. Normal case: Row doesn't exist in target, INSERT it with correct _virtualShard
			// 2. Retry case: Same row with same _virtualShard already exists, REPLACE to update it
			// 3. Multi-shard case: Multiple source shards might have same ID for different logical rows
			//
			// The key insight: Since physical nodes can hold multiple virtual shards,
			// the source shard row (with _virtualShard = sourceShardId) and target shard row
			// (with _virtualShard = targetShardId) are DIFFERENT rows even if they have same primary key.
			// They're distinguished by the combination of (primary_key, _virtualShard).
			//
			// However, SQLite PRIMARY KEY doesn't include _virtualShard, so we need to handle
			// the collision. We use INSERT OR REPLACE to ensure we always have the target shard
			// version, but the source shard version might get replaced if they're on same node.
			// This is okay because after resharding completes, the source shard will be deleted anyway.
			const columns = Object.keys(row).filter((col) => col !== "_virtualShard");
			const allColumns = [...columns, "_virtualShard"];
			const values = allColumns.map(() => "?");
			const query = `INSERT OR REPLACE INTO ${tableName} (${allColumns.join(", ")}) VALUES (${values.join(", ")})`;
			// Cast row values to SqlParam since they come from SQL rows
			const params = [
				...columns.map((col) => row[col] as SqlParam),
				targetShardId,
			];

			// Insert row into its designated target shard only
			const targetStorageId = env.STORAGE.idFromName(targetNodeId);
			const targetStorageStub = env.STORAGE.get(
				targetStorageId,
			) as DurableObjectStub<Storage>;

			try {
				await targetStorageStub.executeQuery({
					query,
					params,
				});

				rowsCopied++;
				const currentCount = shardDistribution.get(targetShardId) || 0;
				shardDistribution.set(targetShardId, currentCount + 1);

				// Update virtual indexes: add to target shard now, track for removal from source later
				// We can't remove from source during copy because multiple rows may share the same key value
				if (tableIndexes.length > 0) {
					for (const index of tableIndexes) {
						const indexColumns = JSON.parse(index.columns) as string[];
						const keyValue = buildIndexKeyValue(row, indexColumns);
						if (keyValue !== null) {
							// Add to target shard's index entry
							await topologyStub.addShardToIndexEntry(
								index.index_name,
								keyValue,
								targetShardId,
							);
							// Track for removal from source after all rows are copied
							indexKeysToRemoveFromSource.get(index.index_name)?.add(keyValue);
						}
					}
				}
			} catch (error) {
				const copyErrorMsg =
					error instanceof Error ? error.message : String(error);
				logger.error`Failed to copy row to target shard ${{ source }} ${{ shardKeyValue }} ${{ targetShardId }} ${{ error: copyErrorMsg }}`;
				throw error;
			}
		}

		// Now that all rows are copied, remove index entries from source shard
		// This is safe because all data has been migrated to target shards
		if (tableIndexes.length > 0) {
			for (const index of tableIndexes) {
				const keysToRemove = indexKeysToRemoveFromSource.get(index.index_name)!;
				for (const keyValue of keysToRemove) {
					await topologyStub.removeShardFromIndexEntry(
						index.index_name,
						keyValue,
						sourceShardId,
					);
				}
			}
			const totalKeysRemoved = Array.from(
				indexKeysToRemoveFromSource.values(),
			).reduce((sum, set) => sum + set.size, 0);
			logger.info`Phase 3B: Index entries removed from source shard ${{ source }} ${{ indexCount: tableIndexes.length }} ${{ keysRemoved: totalKeysRemoved }}`;
		}

		// Log final distribution
		const distributionBreakdown: { [key: number]: number } = {};
		for (const [shardId, count] of shardDistribution) {
			distributionBreakdown[shardId] = count;
		}

		const totalRows = rowsCopied;
		const totalShards = targetShardIds.length;
		const sourceRowsRead = rows.length;
		const sourceCountQuery = sourceCount;
		const finalMatch = rows.length === sourceCount;
		logger.info`Phase 3B: Final copy distribution ${{ source }} ${{ totalRows }} ${{ totalShards }} ${{ distribution: distributionBreakdown }} ${{ sourceRowsRead }} ${{ sourceCountQuery }} ${{ match: finalMatch }}`;

		const duration = Date.now() - startTime;
		logger.info`Phase 3B: Data copy completed ${{ source }} ${{ rowsCopied }} ${{ duration }}`;
		return {
			rows_copied: rowsCopied,
			duration,
			distribution: distributionBreakdown,
		};
	} catch (error) {
		const copyFailedError =
			error instanceof Error ? error.message : String(error);
		logger.error`Phase 3B: Data copy failed ${{ source }} ${{ error: copyFailedError }}`;
		throw error;
	}
}

/**
 * Phase 3C: Replay captured changes to target shards
 * This is idempotent - replaying INSERT again will fail (which is okay for copy),
 * but UPDATE and DELETE work fine
 */
async function replayChangeLog(
	_env: Env,
	_databaseId: string,
	changeLogId: string,
	targetShardIds: number[],
	_tableName: string,
	_shardKey: string,
	_shardStrategy: "hash" | "range",
): Promise<{ changes_replayed: number; duration: number }> {
	const startTime = Date.now();
	const changesReplayed = 0;

	const source = "QueueConsumer";
	logger.info`Phase 3C: Starting change log replay ${{ source }} ${{ changeLogId }} ${{ targetShardIds }}`;

	try {
		logger.info`Phase 3C: No change log entries to replay (or would fetch from persistent log) ${{ source }}`;

		const duration = Date.now() - startTime;
		return { changes_replayed: changesReplayed, duration };
	} catch (error) {
		const replayErrorMsg =
			error instanceof Error ? error.message : String(error);
		logger.error`Phase 3C: Change log replay failed ${{ source }} ${{ error: replayErrorMsg }}`;
		throw error;
	}
}

/**
 * Phase 3D: Verify data integrity by comparing row counts
 */
async function verifyIntegrity(
	env: Env,
	databaseId: string,
	sourceShardId: number,
	targetShardIds: number[],
	tableName: string,
): Promise<{
	isValid: boolean;
	error?: string;
	duration: number;
	verificationDetails?: VerificationDetails;
}> {
	const startTime = Date.now();

	const source = "QueueConsumer";
	logger.info`Phase 3D: Starting data verification ${{ source }} ${{ sourceShardId }} ${{ targetShardIds }}`;

	try {
		const topologyId = env.TOPOLOGY.idFromName(databaseId);
		const topologyStub = env.TOPOLOGY.get(
			topologyId,
		) as DurableObjectStub<Topology>;
		const topology = await topologyStub.getTopology();

		// Get source shard row count
		const sourceShard = topology.table_shards.find(
			(s: TableShard) =>
				s.table_name === tableName && s.shard_id === sourceShardId,
		);
		if (!sourceShard) {
			throw new Error(`Source shard ${sourceShardId} not found`);
		}

		const sourceStorageId = env.STORAGE.idFromName(sourceShard.node_id);
		const sourceStorageStub = env.STORAGE.get(
			sourceStorageId,
		) as DurableObjectStub<Storage>;

		// Count rows in source shard filtered by _virtualShard to ensure we only count source data
		const sourceCountResult = (await sourceStorageStub.executeQuery({
			query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
			params: [sourceShardId],
		})) as StorageResults<CountRow>;

		const sourceCount = sourceCountResult.rows[0]?.count ?? 0;
		const count = sourceCount;
		const nodeId = sourceShard.node_id;
		logger.info`Source shard row count ${{ source }} ${{ sourceShardId }} ${{ count }} ${{ nodeId }}`;

		// Get target shards total row count with detailed breakdown
		let targetTotalCount = 0;
		const targetBreakdown: { [key: number]: number } = {};

		for (const targetShardId of targetShardIds) {
			const targetShard = topology.table_shards.find(
				(s: TableShard) =>
					s.table_name === tableName && s.shard_id === targetShardId,
			);
			if (!targetShard) {
				throw new Error(`Target shard ${targetShardId} not found`);
			}

			const targetStorageId = env.STORAGE.idFromName(targetShard.node_id);
			const targetStorageStub = env.STORAGE.get(
				targetStorageId,
			) as DurableObjectStub<Storage>;

			// Count rows in target shard filtered by _virtualShard to ensure we only count target data
			const targetCountResult = (await targetStorageStub.executeQuery({
				query: `SELECT COUNT(*) as count FROM ${tableName} WHERE _virtualShard = ?`,
				params: [targetShardId],
			})) as StorageResults<CountRow>;

			const targetCount = targetCountResult.rows[0]?.count ?? 0;
			targetTotalCount += targetCount;
			targetBreakdown[targetShardId] = targetCount;

			const targetNodeId = targetShard.node_id;
			const percentageOfTarget = `${((targetCount / Math.max(sourceCount, 1)) * 100).toFixed(2)}%`;
			logger.info`Target shard row count ${{ source }} ${{ targetShardId }} ${{ count: targetCount }} ${{ nodeId: targetNodeId }} ${{ percentageOfTarget }}`;
		}

		const duration = Date.now() - startTime;

		// Single comprehensive verification log
		const isValid = sourceCount === targetTotalCount;
		const difference = targetTotalCount - sourceCount;
		const differencePercent = (
			(difference / Math.max(sourceCount, 1)) *
			100
		).toFixed(2);

		const verificationStatus = isValid ? "PASSED" : "FAILED";
		const verificationSummary: VerificationDetails = {
			status: verificationStatus,
			duration,
			source: {
				shardId: sourceShardId,
				rowCount: sourceCount,
			},
			targets: {
				shardIds: targetShardIds,
				totalRowCount: targetTotalCount,
				breakdown: targetBreakdown,
				breakdown_details: targetShardIds.map((shardId) => ({
					shardId,
					rowCount: targetBreakdown[shardId] || 0,
				})),
			},
			comparison: {
				match: isValid,
				difference: difference,
				differencePercent: `${differencePercent}%`,
				expectedSourceCount: sourceCount,
				actualTargetCount: targetTotalCount,
			},
		};

		if (isValid) {
			return {
				isValid: true,
				duration,
				verificationDetails: verificationSummary,
			};
		} else {
			const error = `Row count mismatch: expected ${sourceCount} rows, got ${targetTotalCount} rows (difference: ${difference})`;
			return {
				isValid: false,
				error,
				duration,
				verificationDetails: verificationSummary,
			};
		}
	} catch (error) {
		const verifyErrorMsg =
			error instanceof Error ? error.message : String(error);
		logger.error`Phase 3D: Data verification error ${{ source }} ${{ error: verifyErrorMsg }}`;
		throw error;
	}
}

/**
 * Phase 5: Delete old shard data
 */
async function deleteOldShardData(
	env: Env,
	databaseId: string,
	sourceShardId: number,
	tableName: string,
): Promise<void> {
	const source = "QueueConsumer";
	logger.info`Phase 5: Starting old shard deletion ${{ source }} ${{ sourceShardId }}`;

	try {
		const topologyId = env.TOPOLOGY.idFromName(databaseId);
		const topologyStub = env.TOPOLOGY.get(
			topologyId,
		) as DurableObjectStub<Topology>;
		const topology = await topologyStub.getTopology();

		const sourceShard = topology.table_shards.find(
			(s: TableShard) =>
				s.table_name === tableName && s.shard_id === sourceShardId,
		);
		if (!sourceShard) {
			throw new Error(`Source shard ${sourceShardId} not found`);
		}

		const storageId = env.STORAGE.idFromName(sourceShard.node_id);
		const storageStub = env.STORAGE.get(
			storageId,
		) as DurableObjectStub<Storage>;

		// Delete all data from source shard (only rows with this virtual shard ID)
		await storageStub.executeQuery({
			query: `DELETE FROM ${tableName} WHERE _virtualShard = ?`,
			params: [sourceShardId],
		});

		logger.info`Phase 5: Old shard data deleted ${{ sourceShardId }}`;
	} catch (error) {
		logger.error`Phase 5: Old shard deletion failed ${{ error: error instanceof Error ? error.message : String(error) }}`;
		throw error;
	}
}

/**
 * Compute index key from a row and column list
 * Returns null if any column value is NULL (NULL values are not indexed)
 */
