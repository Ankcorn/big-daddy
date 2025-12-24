/**
 * Administration and system operations for Topology
 *
 * Handles system-level operations:
 * - getQueryPlanData: Generate query execution plans with shard targeting
 * - Async job tracking: createAsyncJob, startAsyncJob, completeAsyncJob, failAsyncJob, getAsyncJob, getAsyncJobsForTable
 * - alarm: Periodic health check of storage nodes
 */

import type {
	DeleteStatement,
	SelectStatement,
	Statement,
	UpdateStatement,
} from "@databases/sqlite-ast";
import { logger } from "../../logger";
import type { Storage } from "../storage";
import type { Topology } from "./index";
import type {
	AsyncJob,
	QueryPlanData,
	SqlParam,
	TableMetadata,
	TableShard,
} from "./types";

export class AdministrationOperations {
	constructor(
		private storage: DurableObjectStorage,
		private env: Env,
		private topology: Topology,
	) {}

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
	async getQueryPlanData(
		tableName: string,
		statement: Statement,
		params: SqlParam[],
		correlationId?: string,
	): Promise<QueryPlanData> {
		const source = "Topology";
		const component = "Topology";
		const operation = "getQueryPlanData";
		const table = tableName;

		const startTime = Date.now();
		logger.debug`Getting query plan data ${{ source }} ${{ component }} ${{ operation }} ${{ correlationId }} ${{ requestId: correlationId }} ${{ table }}`;

		// Fetch ALL topology data for this table in a single pass
		const tables = this.storage.sql
			.exec(`SELECT * FROM tables WHERE table_name = ?`, tableName)
			.toArray() as unknown as TableMetadata[];

		const tableMetadata = tables[0];
		if (!tableMetadata) {
			logger.error`Table not found in topology ${{ source }} ${{ component }} ${{ table }}`;
			throw new Error(`Table '${tableName}' not found in topology`);
		}

		const tableShards = this.storage.sql
			.exec(
				`SELECT * FROM table_shards WHERE table_name = ? AND status = 'active' ORDER BY shard_id`,
				tableName,
			)
			.toArray() as unknown as TableShard[];

		if (tableShards.length === 0) {
			logger.error`No active shards found for table ${{ source }} ${{ component }} ${{ table }}`;
			throw new Error(`No active shards found for table '${tableName}'`);
		}

		// Only get ready indexes
		const virtual_indexes = this.storage.sql
			.exec(
				`SELECT index_name, columns, index_type FROM virtual_indexes WHERE table_name = ? AND status = 'ready'`,
				tableName,
			)
			.toArray() as unknown as Array<{
			index_name: string;
			columns: string;
			index_type: "hash" | "unique";
		}>;

		const shardCount = tableShards.length;
		const indexCount = virtual_indexes.length;
		logger.debug`Topology data fetched ${{ source }} ${{ component }} ${{ shardCount }} ${{ indexCount }}`;

		// Determine which shards to query (using indexes if possible)
		const shardsToQuery = await this.determineShardTargets(
			statement,
			tableMetadata,
			tableShards,
			virtual_indexes,
			params,
		);

		const shardsSelected = shardsToQuery.length;
		const totalShards = tableShards.length;
		const strategy =
			shardsToQuery.length === 1
				? "single"
				: shardsToQuery.length === tableShards.length
					? "all"
					: "subset";
		logger.info`Shard targets determined ${{ source }} ${{ component }} ${{ shardsSelected }} ${{ totalShards }} ${{ strategy }}`;

		// NOTE: For INSERT, index maintenance is handled in handleInsert() AFTER
		// we know which shard each row is going to (based on shard key hash).
		// We can't do it here because shardsToQuery[0] may not be the actual target shard.

		const duration = Date.now() - startTime;
		logger.debug`Query plan data completed ${{ source }} ${{ component }} ${{ duration }}`;

		return {
			shardsToQuery,
			virtualIndexes: virtual_indexes.map((idx) => ({
				...idx,
				table_name: tableName,
			})),
			shardKey: tableMetadata.shard_key,
		};
	}

	/**
	 * Determine which shards to query based on the statement and available indexes
	 * This is called internally by getQueryPlanData
	 */
	async determineShardTargets(
		statement: Statement,
		tableMetadata: TableMetadata,
		tableShards: Array<{
			table_name: string;
			shard_id: number;
			node_id: string;
		}>,
		virtualIndexes: Array<{
			index_name: string;
			columns: string;
			index_type: "hash" | "unique";
		}>,
		params: SqlParam[],
	): Promise<Array<{ table_name: string; shard_id: number; node_id: string }>> {
		// For INSERT statements, return all shards
		// The INSERT handler will distribute rows to appropriate shards based on shard key hashing
		if (statement.type === "InsertStatement") {
			return tableShards;
		}

		// For SELECT/UPDATE/DELETE, check WHERE clause for optimization opportunities
		const where = (
			statement as SelectStatement | UpdateStatement | DeleteStatement
		).where;

		if (where) {
			// First, check if WHERE clause filters on the shard key
			// This is the most efficient optimization
			// Pass actual shard IDs (not just count) to handle non-0-indexed shards after resharding
			const shardIds = tableShards.map((s) => s.shard_id).sort((a, b) => a - b);
			const shardId = this.topology.getShardIdFromWhere(
				where,
				tableMetadata,
				shardIds,
				params,
			);
			if (shardId !== null) {
				// Found a matching shard by shard key
				const targetShard = tableShards.find((s) => s.shard_id === shardId);
				if (targetShard) {
					return [targetShard];
				}
			}

			// If shard key optimization didn't work, check if we can use a virtual index to narrow shards
			const indexedShards = await this.topology.getShardsFromIndexedWhere(
				where,
				tableMetadata.table_name,
				tableShards,
				virtualIndexes,
				params,
			);
			if (indexedShards !== null) {
				return indexedShards;
			}
		}

		// No optimization possible - query all shards
		return tableShards;
	}

	/**
	 * Create a new async job record to track long-running operations
	 */
	async createAsyncJob(
		jobId: string,
		jobType: "reshard_table" | "build_index" | "maintain_index",
		tableName: string,
		metadata?: Record<string, unknown>,
	): Promise<void> {
		const now = Date.now();

		// Check if job already exists (handles retries gracefully)
		const existing = this.storage.sql
			.exec(`SELECT job_id FROM async_jobs WHERE job_id = ?`, jobId)
			.toArray() as unknown as { job_id: string }[];

		if (existing.length > 0) {
			logger.info`Async job already exists, skipping creation ${{ jobId }} ${{ jobType }} ${{ tableName }}`;
			return;
		}

		// Create new async job
		this.storage.sql.exec(
			`INSERT INTO async_jobs (job_id, job_type, table_name, status, started_at, metadata, retries, created_at, updated_at)
			 VALUES (?, ?, ?, 'pending', ?, ?, 0, ?, ?)`,
			jobId,
			jobType,
			tableName,
			now,
			metadata ? JSON.stringify(metadata) : null,
			now,
			now,
		);

		logger.info`Async job created ${{ jobId }} ${{ jobType }} ${{ tableName }}`;
	}

	/**
	 * Increment retry counter for an async job
	 */
	async incrementAsyncJobRetries(jobId: string): Promise<void> {
		const now = Date.now();
		this.storage.sql.exec(
			`UPDATE async_jobs SET retries = retries + 1, updated_at = ? WHERE job_id = ?`,
			now,
			jobId,
		);

		logger.info`Async job retries incremented ${{ jobId }}`;
	}

	/**
	 * Mark an async job as running
	 */
	async startAsyncJob(jobId: string): Promise<void> {
		const now = Date.now();
		this.storage.sql.exec(
			`UPDATE async_jobs SET status = 'running', updated_at = ? WHERE job_id = ?`,
			now,
			jobId,
		);

		logger.info`Async job started ${{ jobId }}`;
	}

	/**
	 * Mark an async job as completed
	 */
	async completeAsyncJob(jobId: string): Promise<void> {
		const now = Date.now();
		this.storage.sql.exec(
			`UPDATE async_jobs SET status = 'completed', ended_at = ?, duration_ms = ? - started_at, updated_at = ? WHERE job_id = ?`,
			now,
			now,
			now,
			jobId,
		);

		logger.info`Async job completed ${{ jobId }}`;
	}

	/**
	 * Mark an async job as failed with error message
	 */
	async failAsyncJob(jobId: string, errorMessage: string): Promise<void> {
		const now = Date.now();
		this.storage.sql.exec(
			`UPDATE async_jobs SET status = 'failed', error_message = ?, ended_at = ?, duration_ms = ? - started_at, updated_at = ? WHERE job_id = ?`,
			errorMessage,
			now,
			now,
			now,
			jobId,
		);

		logger.error`Async job failed ${{ jobId }} ${{ errorMessage }}`;
	}

	/**
	 * Get an async job by ID
	 */
	async getAsyncJob(jobId: string): Promise<AsyncJob | null> {
		const result = this.storage.sql
			.exec(`SELECT * FROM async_jobs WHERE job_id = ?`, jobId)
			.toArray() as unknown as AsyncJob[];

		return result.length > 0 ? result[0]! : null;
	}

	/**
	 * Get all async jobs for a table
	 */
	async getAsyncJobsForTable(
		tableName: string,
		limit: number = 100,
	): Promise<AsyncJob[]> {
		return this.storage.sql
			.exec(
				`SELECT * FROM async_jobs WHERE table_name = ? ORDER BY created_at DESC LIMIT ?`,
				tableName,
				limit,
			)
			.toArray() as unknown as AsyncJob[];
	}

	/**
	 * Alarm handler - runs every 5 minutes to check storage capacity and status
	 */
	async alarm(): Promise<void> {
		// Get all storage nodes
		const nodes = this.storage.sql
			.exec(`SELECT node_id FROM storage_nodes`)
			.toArray() as unknown as { node_id: string }[];

		const now = Date.now();

		// Check each storage node's capacity and availability
		for (const node of nodes) {
			try {
				// Get the Storage DO stub
				const storageId = this.env.STORAGE.idFromName(node.node_id);
				const storageStub = this.env.STORAGE.get(
					storageId,
				) as DurableObjectStub<Storage>;

				// Fetch the database size
				const size = await storageStub.getDatabaseSize();

				// Update capacity, set status to active, clear any previous error, and update timestamp
				this.storage.sql.exec(
					`UPDATE storage_nodes SET capacity_used = ?, status = ?, error = NULL, updated_at = ? WHERE node_id = ?`,
					size,
					"active",
					now,
					node.node_id,
				);
			} catch (error) {
				// If we couldn't reach the storage node, mark it as down, store the error, and update timestamp
				const errorMessage =
					error instanceof Error ? error.message : String(error);
				this.storage.sql.exec(
					`UPDATE storage_nodes SET status = ?, error = ?, updated_at = ? WHERE node_id = ?`,
					"down",
					errorMessage,
					now,
					node.node_id,
				);
			}
		}

		// Schedule the next alarm in 5 minutes
		this.storage.setAlarm(Date.now() + 5 * 60 * 1000);
	}
}
