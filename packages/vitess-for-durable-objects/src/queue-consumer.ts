/**
 * Queue Consumer Worker for Virtual Index Operations
 *
 * This worker processes jobs from the vitess-index-jobs queue:
 * - IndexBuildJob: Build a new virtual index from existing data
 * - IndexUpdateJob: Update virtual index entries after data modifications
 */

import type { IndexJob, IndexBuildJob, IndexUpdateJob, MessageBatch } from './Queue/types';
import type { Storage } from './Storage/Storage';
import type { Topology } from './Topology/Topology';

/**
 * Queue message handler
 * Receives batches of up to 10 messages and processes them
 */
export async function queueHandler(batch: MessageBatch<IndexJob>, env: Env): Promise<void> {
	console.log(`Processing batch of ${batch.messages.length} index jobs from queue: ${batch.queue}`);

	// Process messages in parallel where possible
	const results = await Promise.allSettled(
		batch.messages.map(async (message) => {
			try {
				await processIndexJob(message.body, env);
				console.log(`Successfully processed job ${message.id}:`, message.body.type);
			} catch (error) {
				console.error(`Failed to process job ${message.id}:`, error);
				// Throwing will cause the message to be retried
				throw error;
			}
		}),
	);

	// Log summary
	const successful = results.filter((r) => r.status === 'fulfilled').length;
	const failed = results.filter((r) => r.status === 'rejected').length;
	console.log(`Batch complete: ${successful} succeeded, ${failed} failed`);
}

/**
 * Process a single index job
 */
async function processIndexJob(job: IndexJob, env: Env): Promise<void> {
	switch (job.type) {
		case 'build_index':
			await processBuildIndexJob(job, env);
			break;
		case 'index_update':
			await processIndexUpdateJob(job, env);
			break;
		default:
			throw new Error(`Unknown job type: ${(job as any).type}`);
	}
}

/**
 * Build a virtual index from existing data
 *
 * Process:
 * 1. Get all shards for this table from topology
 * 2. For each distinct value in the indexed column:
 *    - Query all shards to find which contain that value
 *    - Create index entry mapping value → shard_ids
 * 3. Update index status to 'ready' or 'failed'
 */
async function processBuildIndexJob(job: IndexBuildJob, env: Env): Promise<void> {
	console.log(`Building index ${job.index_name} on ${job.table_name}.${job.column_name}`);

	const topologyId = env.TOPOLOGY.idFromName(job.database_id);
	const topologyStub = env.TOPOLOGY.get(topologyId);

	try {
		// 1. Get all shards for this table from topology
		const topology = await topologyStub.getTopology();
		const tableShards = topology.table_shards.filter((s) => s.table_name === job.table_name);

		if (tableShards.length === 0) {
			throw new Error(`No shards found for table '${job.table_name}'`);
		}

		console.log(`Found ${tableShards.length} shards for table ${job.table_name}`);

		// 2. Collect all distinct values from all shards
		// Map: value → Set<shard_id>
		const valueToShards = new Map<string, Set<number>>();

		for (const shard of tableShards) {
			try {
				// Get storage stub for this shard
				const storageId = env.STORAGE.idFromName(shard.node_id);
				const storageStub = env.STORAGE.get(storageId);

				// Query for all distinct values in this shard
				const result = await storageStub.executeQuery({
					query: `SELECT DISTINCT ${job.column_name} FROM ${job.table_name}`,
					params: [],
					queryType: 'SELECT',
				});

				// For each distinct value, add this shard to its shard set
				for (const row of result.rows) {
					const value = row[job.column_name];

					// Skip NULL values - we don't index NULLs for now
					if (value === null || value === undefined) {
						continue;
					}

					// Convert value to string for storage
					const keyValue = String(value);

					if (!valueToShards.has(keyValue)) {
						valueToShards.set(keyValue, new Set());
					}

					valueToShards.get(keyValue)!.add(shard.shard_id);
				}

				console.log(`Processed shard ${shard.shard_id}, found values for index`);
			} catch (error) {
				console.error(`Error querying shard ${shard.shard_id}:`, error);
				throw new Error(`Failed to query shard ${shard.shard_id}: ${error instanceof Error ? error.message : String(error)}`);
			}
		}

		console.log(`Found ${valueToShards.size} distinct values across all shards`);

		// 3. Create index entries in batch
		const entries = Array.from(valueToShards.entries()).map(([keyValue, shardIdSet]) => ({
			keyValue,
			shardIds: Array.from(shardIdSet).sort((a, b) => a - b), // Sort for consistency
		}));

		if (entries.length > 0) {
			const result = await topologyStub.batchUpsertIndexEntries(job.index_name, entries);
			console.log(`Created ${result.count} index entries`);
		}

		// 4. Update index status to 'ready'
		await topologyStub.updateIndexStatus(job.index_name, 'ready');

		console.log(`Index ${job.index_name} build complete - ${entries.length} unique values indexed`);
	} catch (error) {
		console.error(`Failed to build index ${job.index_name}:`, error);

		// Update index status to 'failed'
		const errorMessage = error instanceof Error ? error.message : String(error);
		await topologyStub.updateIndexStatus(job.index_name, 'failed', errorMessage);

		throw error;
	}
}

/**
 * Update virtual index entries after a data modification
 *
 * Process:
 * 1. For INSERT: Add shard_id to index entries for new values
 * 2. For UPDATE: Remove shard_id from old value entries, add to new value entries
 * 3. For DELETE: Remove shard_id from index entries for deleted values
 */
async function processIndexUpdateJob(job: IndexUpdateJob, env: Env): Promise<void> {
	console.log(`Updating index for ${job.operation} on ${job.table_name} shard ${job.shard_id}`);

	const topologyId = env.TOPOLOGY.idFromName(job.database_id);
	const topologyStub = env.TOPOLOGY.get(topologyId);

	try {
		// TODO: Phase 4 implementation
		// Get all indexes for this table
		// For each indexed column in the job data:
		//   - For INSERT: Add shard_id to the value's entry
		//   - For UPDATE: Remove from old value, add to new value
		//   - For DELETE: Remove shard_id from the value's entry

		console.log(`Index update complete for ${job.table_name} (TODO: implement)`);
	} catch (error) {
		console.error(`Failed to update index for ${job.table_name}:`, error);
		throw error;
	}
}
