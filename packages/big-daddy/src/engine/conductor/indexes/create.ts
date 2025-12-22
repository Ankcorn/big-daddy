import type { CreateIndexStatement, SelectStatement } from '@databases/sqlite-ast';
import type { QueryResult, QueryHandlerContext } from '../types';
import type { IndexJob } from '../../queue/types';
import { getCachedQueryPlanData } from '../utils/write';

/**
 * Enqueue an index job to the queue
 *
 * In test environments, this also triggers immediate processing of the queue.
 * This is necessary because:
 * 1. Tests need synchronous behavior to verify index maintenance worked
 * 2. Cloudflare's test environment doesn't automatically process queues
 * 3. Without this, tests would need to manually call queueHandler after every write
 *
 * In production, queue processing happens automatically via Cloudflare's infrastructure.
 */
async function enqueueIndexJob(context: QueryHandlerContext, job: IndexJob): Promise<void> {
	const { indexQueue, env } = context;

	if (!indexQueue) {
		console.warn('INDEX_QUEUE not available, skipping index job:', job.type);
		return;
	}

	try {
		await indexQueue.send(job);
		console.log(`Enqueued ${job.type} job for ${job.table_name}`);

		// In test environment, trigger queue processing immediately
		// This is detected by checking if env was provided (only happens in tests)
		if (env) {
			// Dynamically import queueHandler to avoid circular dependencies
			const { queueHandler } = await import('../../../queue-consumer');

			// Simulate queue batch with this single message
			const jobCorrelationId = job.correlation_id || crypto.randomUUID();
			await queueHandler(
				{
					queue: 'vitess-index-jobs',
					messages: [
						{
							id: `test-${Date.now()}-${Math.random()}`,
							timestamp: new Date(),
							body: job,
							attempts: 1,
						},
					],
				},
				env,
				jobCorrelationId,
			);
		}
	} catch (error) {
		console.error(`Failed to enqueue index job:`, error);
		// Don't throw - index operations should not block queries
	}
}

/**
 * Handle CREATE INDEX statement execution
 *
 * This method:
 * 1. Validates the index and table
 * 2. Creates SQLite indexes on all storage shards (for query performance)
 * 3. Creates the virtual index definition in Topology with status 'building'
 * 4. Enqueues an IndexBuildJob to the queue for async processing (builds virtual index metadata)
 * 5. Returns immediately to the client (non-blocking)
 */
export async function handleCreateIndex(statement: CreateIndexStatement, context: QueryHandlerContext): Promise<QueryResult> {
	const indexName = statement.name.name;
	const tableName = statement.table.name;

	// Extract column names from the CREATE INDEX statement
	const columns = statement.columns.map((col) => col.name);

	// Get topology stub
	const { databaseId, storage, topology } = context;
	const topologyId = topology.idFromName(databaseId);
	const topologyStub = topology.get(topologyId);

	// Use getCachedQueryPlanData to get active shards (same as CRUD operations)
	// This ensures consistent handling of shard status filtering
	const dummySelect = {
		type: 'SelectStatement',
		select: [{ type: 'SelectClause', expression: { type: 'Identifier', name: '*' } }],
		from: { type: 'Identifier', name: tableName },
	} as SelectStatement;
	const { planData } = await getCachedQueryPlanData(context, tableName, dummySelect, []);

	// Dedupe by node_id - SQLite index only needs to be created once per storage node
	const uniqueNodeIds = [...new Set(planData.shardsToQuery.map(s => s.node_id))];

	// Build the CREATE INDEX SQL statement
	const uniqueClause = statement.unique ? 'UNIQUE ' : '';
	const ifNotExistsClause = statement.ifNotExists ? 'IF NOT EXISTS ' : '';
	const columnList = columns.join(', ');
	const createIndexSQL = `CREATE ${uniqueClause}INDEX ${ifNotExistsClause}${indexName} ON ${tableName}(${columnList})`;

	// Create SQLite indexes on each unique storage node in parallel
	// This ensures queries on individual shards are fast (reduces rows scanned)
	await Promise.all(
		uniqueNodeIds.map(async (nodeId) => {
			const storageId = storage.idFromName(nodeId);
			const storageStub = storage.get(storageId);

			try {
				await storageStub.executeQuery({
					query: createIndexSQL,
					params: [],
				});
			} catch (error) {
				// If IF NOT EXISTS is specified, ignore "already exists" errors
				if (statement.ifNotExists && (error as Error).message.includes('already exists')) {
					return;
				}
				throw error;
			}
		}),
	);

	// Create the virtual index in Topology with 'building' status
	// Virtual index provides shard-level routing optimization
	const indexType = statement.unique ? 'unique' : 'hash';
	const result = await topologyStub.createVirtualIndex(indexName, tableName, columns, indexType);

	if (!result.success) {
		// If IF NOT EXISTS is specified and index already exists, return success
		if (statement.ifNotExists && result.error?.includes('already exists')) {
			return {
				rows: [],
				rowsAffected: 0,
			};
		}

		throw new Error(result.error || 'Failed to create index');
	}

	// Enqueue index build job for background processing
	// This populates the virtual index metadata with existing data
	await enqueueIndexJob(context, {
		type: 'build_index',
		database_id: databaseId,
		table_name: tableName,
		columns: columns,
		index_name: indexName,
		created_at: new Date().toISOString(),
		correlation_id: context.correlationId,
	});

	return {
		rows: [],
		rowsAffected: 0,
	};
}
