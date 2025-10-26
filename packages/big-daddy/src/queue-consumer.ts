/**
 * Queue Consumer Worker for Virtual Index Operations
 *
 * This worker processes jobs from the vitess-index-jobs queue:
 * - IndexBuildJob: Build a new virtual index from existing data
 * - IndexMaintenanceJob: Maintain indexes after UPDATE/DELETE operations
 */

import { withLogTags } from 'workers-tagged-logger';
import { logger } from './logger';
import type { IndexJob, IndexBuildJob, IndexMaintenanceJob, MessageBatch } from './engine/queue/types';
import type { Storage, QueryResult } from './engine/storage';
import type { Topology } from './engine/topology';

/**
 * Queue message handler
 * Receives batches of up to 10 messages and processes them
 */
export async function queueHandler(batch: MessageBatch<IndexJob>, env: Env, batchCorrelationId?: string): Promise<void> {
	return withLogTags({ source: 'QueueConsumer' }, async () => {
		const cid = batchCorrelationId || crypto.randomUUID();
		logger.setTags({
			correlationId: cid,
			requestId: cid,
			component: 'QueueConsumer',
			operation: 'queueHandler',
		});

		logger.info('Processing queue batch', {
			batchSize: batch.messages.length,
			queue: batch.queue,
		});

		// Process messages in parallel where possible
		const results = await Promise.allSettled(
			batch.messages.map(async (message) => {
				try {
					const jobCorrelationId = message.body.correlation_id || cid;
					logger.setTags({
						correlationId: jobCorrelationId,
						requestId: jobCorrelationId,
						jobId: message.id,
						jobType: message.body.type,
					});

					logger.info('Processing queue message', {
						jobId: message.id,
						jobType: message.body.type,
						attempts: message.attempts,
					});

					await processIndexJob(message.body, env, jobCorrelationId);

					logger.info('Successfully processed job', {
						jobId: message.id,
						jobType: message.body.type,
						status: 'success',
					});
				} catch (error) {
					logger.error('Failed to process job', {
						jobId: message.id,
						jobType: message.body.type,
						error: error instanceof Error ? error.message : String(error),
						status: 'failure',
					});
					// Throwing will cause the message to be retried
					throw error;
				}
			}),
		);

		// Log summary
		const successful = results.filter((r) => r.status === 'fulfilled').length;
		const failed = results.filter((r) => r.status === 'rejected').length;

		logger.info('Batch processing complete', {
			batchSize: batch.messages.length,
			successful,
			failed,
			status: failed > 0 ? 'partial' : 'success',
		});
	});
}

/**
 * Process a single index job
 */
async function processIndexJob(job: IndexJob, env: Env, correlationId?: string): Promise<void> {
	switch (job.type) {
		case 'build_index':
			await processBuildIndexJob(job, env, correlationId);
			break;
		case 'maintain_index':
			await processIndexMaintenanceJob(job, env, correlationId);
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
async function processBuildIndexJob(job: IndexBuildJob, env: Env, correlationId?: string): Promise<void> {
	const columnList = job.columns.join(', ');
	logger.setTags({
		table: job.table_name,
		indexName: job.index_name,
	});

	logger.info('Building virtual index', {
		indexName: job.index_name,
		table: job.table_name,
		columns: columnList,
	});

	const topologyId = env.TOPOLOGY.idFromName(job.database_id);
	const topologyStub = env.TOPOLOGY.get(topologyId);

	try {
		// 1. Get all shards for this table from topology
		const topology = await topologyStub.getTopology();
		const tableShards = topology.table_shards.filter((s) => s.table_name === job.table_name);

		if (tableShards.length === 0) {
			logger.error('No shards found for table', { table: job.table_name });
			throw new Error(`No shards found for table '${job.table_name}'`);
		}

		logger.info('Found shards for table', {
			shardCount: tableShards.length,
		});

		// 2. Collect all distinct values from all shards
		// Map: composite key value → Set<shard_id>
		const valueToShards = new Map<string, Set<number>>();

		for (const shard of tableShards) {
			try {
				// Get storage stub for this shard
				const storageId = env.STORAGE.idFromName(shard.node_id);
				const storageStub = env.STORAGE.get(storageId);

				// Query for all distinct combinations of indexed columns
				const result = await storageStub.executeQuery({
					query: `SELECT DISTINCT ${columnList} FROM ${job.table_name}`,
					params: [],
					queryType: 'SELECT',
				});

				// Type guard: single query always returns QueryResult
				if (!('rows' in result)) {
					throw new Error('Expected QueryResult but got BatchQueryResult');
				}

				// Extract rows with proper typing to avoid deep instantiation with Disposable types
				const resultRows = (result as any).rows as Record<string, any>[];

				// For each distinct combination, add this shard to its shard set
				for (const row of resultRows) {
					// Build composite key from all indexed columns
					// For single column: just the value
					// For multiple columns: JSON array of values
					let keyValue: string;

					if (job.columns.length === 1) {
						const value: any = row[job.columns[0]];
						// Skip NULL values
						if (value === null || value === undefined) {
							continue;
						}
						keyValue = String(value);
					} else {
						// Composite index - build key from all column values
						const values = job.columns.map(col => row[col]);
						// Skip if any value is NULL
						if (values.some(v => v === null || v === undefined)) {
							continue;
						}
						// Store as JSON array
						keyValue = JSON.stringify(values);
					}

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

		logger.info('Collected distinct values from shards', {
			distinctValues: valueToShards.size,
		});

		// 3. Create index entries in batch
		const entries = Array.from(valueToShards.entries()).map(([keyValue, shardIdSet]) => ({
			keyValue,
			shardIds: Array.from(shardIdSet).sort((a, b) => a - b), // Sort for consistency
		}));

		if (entries.length > 0) {
			const result = await topologyStub.batchUpsertIndexEntries(job.index_name, entries);
			logger.info('Created index entries', {
				entryCount: result.count,
			});
		}

		// 4. Update index status to 'ready'
		await topologyStub.updateIndexStatus(job.index_name, 'ready');

		logger.info('Index build complete', {
			indexName: job.index_name,
			uniqueValues: entries.length,
			status: 'ready',
		});
	} catch (error) {
		logger.error('Failed to build index', {
			indexName: job.index_name,
			error: error instanceof Error ? error.message : String(error),
		});

		// Update index status to 'failed'
		const errorMessage = error instanceof Error ? error.message : String(error);
		await topologyStub.updateIndexStatus(job.index_name, 'failed', errorMessage);

		throw error;
	}
}

/**
 * Maintain virtual indexes after UPDATE/DELETE operations
 *
 * Process:
 * 1. For each affected shard, read current data
 * 2. Compute what index entries should exist based on current state
 * 3. Compare with topology's current state
 * 4. Apply changes in a single batched call
 */
async function processIndexMaintenanceJob(job: IndexMaintenanceJob, env: Env, correlationId?: string): Promise<void> {
	logger.setTags({
		table: job.table_name,
	});

	logger.info('Maintaining indexes after write operation', {
		operation: job.operation,
		affectedIndexes: job.affected_indexes.length,
		shardCount: job.shard_ids.length,
	});
	console.log(`Maintaining indexes for ${job.operation} on ${job.table_name}, shards: ${job.shard_ids.join(',')}`);

	const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(job.database_id));
	const topology = await topologyStub.getTopology();

	// Get index definitions
	const indexes = topology.virtual_indexes.filter(
		(idx) => job.affected_indexes.includes(idx.index_name) && idx.status === 'ready'
	);

	if (indexes.length === 0) {
		console.log('No ready indexes to maintain');
		return;
	}

	// Collect all index changes across all shards
	const allChanges: Array<{
		operation: 'add' | 'remove';
		index_name: string;
		key_value: string;
		shard_id: number;
	}> = [];

	// Process each affected shard
	for (const shardId of job.shard_ids) {
		const shard = topology.table_shards.find((s) => s.table_name === job.table_name && s.shard_id === shardId);
		if (!shard) {
			console.warn(`Shard ${shardId} not found for table ${job.table_name}`);
			continue;
		}

		const storageStub = env.STORAGE.get(env.STORAGE.idFromName(shard.node_id));

		// For each index, rebuild entries for this shard
		for (const index of indexes) {
			const indexColumns = JSON.parse(index.columns);

			// Read current state from shard
			const columnList = indexColumns.join(', ');
			const query = `SELECT ${columnList} FROM ${job.table_name}`;

			try {
				const result = await storageStub.executeQuery({
					query,
					params: [],
					queryType: 'SELECT',
				});

				// Type guard: single query always returns QueryResult
				if (!('rows' in result)) {
					throw new Error('Expected QueryResult but got BatchQueryResult');
				}

				// Extract rows with proper typing to avoid deep instantiation with Disposable types
				const resultRows = (result as any).rows as Record<string, any>[];

				// Build set of values that SHOULD include this shard
				const shouldExist = new Set<string>();
				for (const row of resultRows) {
					const keyValue = computeIndexKey(row, indexColumns);
					if (keyValue) {
						shouldExist.add(keyValue);
					}
				}

				// Get current state from Topology
				const currentEntries = await topologyStub.getIndexEntriesForShard(index.index_name, shardId);
				const currentlyExists = new Set(currentEntries.map((e) => e.key_value));

				// Compute changes
				// Add shard to entries where it should exist but doesn't
				for (const keyValue of shouldExist) {
					if (!currentlyExists.has(keyValue)) {
						allChanges.push({
							operation: 'add',
							index_name: index.index_name,
							key_value: keyValue,
							shard_id: shardId,
						});
					}
				}

				// Remove shard from entries where it shouldn't exist but does
				for (const keyValue of currentlyExists) {
					if (!shouldExist.has(keyValue)) {
						allChanges.push({
							operation: 'remove',
							index_name: index.index_name,
							key_value: keyValue,
							shard_id: shardId,
						});
					}
				}
			} catch (error) {
				console.error(`Error querying shard ${shardId}:`, error);
				throw error;
			}
		}
	}

	// Apply all changes in a single Topology call
	if (allChanges.length > 0) {
		await topologyStub.batchMaintainIndexes(allChanges);
		console.log(`Applied ${allChanges.length} index changes for ${job.table_name}`);
	} else {
		console.log(`No index changes needed for ${job.table_name}`);
	}
}

/**
 * Compute index key from a row and column list
 * Returns null if any column value is NULL (NULL values are not indexed)
 */
function computeIndexKey(row: Record<string, any>, columns: string[]): string | null {
	const values = [];
	for (const col of columns) {
		const val = row[col];
		if (val === null || val === undefined) {
			return null; // Don't index NULL values
		}
		values.push(val);
	}
	return columns.length === 1 ? String(values[0]) : JSON.stringify(values);
}

