/**
 * Topology Durable Object for managing distributed database cluster topology
 *
 * This is organized into feature-specific modules:
 * - crud.ts: Create, Read, Update operations for cluster initialization and table management
 * - resharding.ts: Resharding operations for scaling tables horizontally
 * - virtual-indexes.ts: Virtual index management for query optimization
 * - administration.ts: System operations like query planning and async job tracking
 *
 * Each module is implemented as a mini class that receives the Topology instance,
 * allowing access to DurableObject state and environment while keeping concerns separated.
 */

import { DurableObject } from "cloudflare:workers";
import type {
	BinaryExpression,
	Expression,
	InsertStatement,
	Literal,
	Placeholder,
	Statement,
} from "@databases/sqlite-ast";
import { AdministrationOperations } from "./administration";
import { CRUDOperations } from "./crud";
import { ReshardingOperations } from "./resharding";
import { initializeSchemaTables, runSchemaMigrations } from "./tables";
import type {
	AsyncJob,
	IndexStatus,
	QueryPlanData,
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
import { VirtualIndexOperations } from "./virtual-indexes";

/**
 * Topology Durable Object for managing cluster topology
 *
 * Delegates feature-specific operations to specialized mini classes:
 * - this.crud.*: CRUD operations
 * - this.resharding.*: Resharding operations
 * - this.virtualIndexes.*: Virtual index management
 * - this.administration.*: System operations and query planning
 */
export class Topology extends DurableObject<Env> {
	// Feature-specific operation handlers
	private crud: CRUDOperations;
	private resharding: ReshardingOperations;
	private virtualIndexes: VirtualIndexOperations;
	private administration: AdministrationOperations;
	/**
	 * The constructor is invoked once upon creation of the Durable Object
	 *
	 * @param ctx - The interface for interacting with Durable Object state
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 */
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.initializeSchema();

		// Initialize feature operation handlers with direct storage access
		this.crud = new CRUDOperations(this.ctx.storage);
		this.resharding = new ReshardingOperations(this.ctx.storage, this.env);
		this.virtualIndexes = new VirtualIndexOperations(this.ctx.storage);
		// AdministrationOperations needs reference to Topology for cross-module method access
		this.administration = new AdministrationOperations(
			this.ctx.storage,
			this.env,
			this,
		);

		// Set up recurring alarm to check capacity every 5 minutes
		this.ctx.storage.setAlarm(Date.now() + 5 * 60 * 1000);
	}

	/**
	 * Initialize the SQLite schema for topology metadata
	 */
	private initializeSchema(): void {
		initializeSchemaTables((sql: string) => this.ctx.storage.sql.exec(sql));
		// Run migrations for existing schemas (adds new columns if missing)
		runSchemaMigrations(
			(sql: string) => this.ctx.storage.sql.exec(sql),
			(tableName: string) =>
				this.ctx.storage.sql
					.exec(`PRAGMA table_info('${tableName}')`)
					.toArray() as Array<{ name: string }>,
		);
	}

	/**
	 * Check if the topology has been created
	 */
	public isCreated(): boolean {
		const result = this.ctx.storage.sql
			.exec(`SELECT value FROM cluster_metadata WHERE key = 'created'`)
			.toArray() as unknown as {
			value: string;
		}[];

		return result.length > 0 && result[0]?.value === "true";
	}

	/**
	 * Ensure the topology has been created
	 */
	public ensureCreated(): void {
		if (!this.isCreated()) {
			throw new Error(
				"Topology not created. Call create() first to initialize the cluster.",
			);
		}
	}

	// ============================================================================
	// CRUD Operations (from crud.ts)
	// ============================================================================

	/**
	 * Create the cluster topology with a fixed number of storage nodes
	 * This can only be called once - storage nodes cannot be modified after creation
	 */
	async create(
		numNodes: number,
	): Promise<{ success: boolean; error?: string }> {
		return this.crud.create(numNodes);
	}

	/**
	 * Read all topology information in a single operation
	 */
	async getTopology(): Promise<TopologyData> {
		this.ensureCreated();
		return this.crud.getTopology();
	}

	/**
	 * Batch update topology information
	 */
	async updateTopology(
		updates: TopologyUpdates,
	): Promise<{ success: boolean }> {
		this.ensureCreated();
		return this.crud.updateTopology(updates);
	}

	// ============================================================================
	// Row Count Operations
	// ============================================================================

	/**
	 * Get row counts for all active shards of a table
	 */
	getTableShardRowCounts(tableName: string): ShardRowCount[] {
		this.ensureCreated();
		return this.crud.getTableShardRowCounts(tableName);
	}

	/**
	 * Bump (increment or decrement) the row count for a specific shard
	 * Clamps at 0 to prevent negative counts
	 */
	bumpTableShardRowCount(
		tableName: string,
		shardId: number,
		delta: number,
	): void {
		this.ensureCreated();
		this.crud.bumpTableShardRowCount(tableName, shardId, delta);
	}

	/**
	 * Batch bump row counts for multiple shards of a table
	 */
	batchBumpTableShardRowCounts(
		tableName: string,
		deltaByShard: Map<number, number>,
	): void {
		this.ensureCreated();
		this.crud.batchBumpTableShardRowCounts(tableName, deltaByShard);
	}

	/**
	 * Set row counts for shards (used after resharding to set exact counts)
	 */
	setTableShardRowCounts(
		tableName: string,
		countsByShardId: Map<number, number>,
	): void {
		this.ensureCreated();
		this.crud.setTableShardRowCounts(tableName, countsByShardId);
	}

	// ============================================================================
	// Resharding Operations (from resharding.ts)
	// ============================================================================

	/**
	 * Create pending shards for resharding operation
	 */
	async createPendingShards(
		tableName: string,
		newShardCount: number,
		changeLogId: string,
	): Promise<TableShard[]> {
		this.ensureCreated();
		return this.resharding.createPendingShards(
			tableName,
			newShardCount,
			changeLogId,
		);
	}

	/**
	 * Get the current resharding state for a table
	 */
	async getReshardingState(tableName: string): Promise<ReshardingState | null> {
		this.ensureCreated();
		return this.resharding.getReshardingState(tableName);
	}

	/**
	 * Start resharding: transition from 'pending_shards' to 'copying' status
	 */
	async startResharding(tableName: string): Promise<void> {
		this.ensureCreated();
		return this.resharding.startResharding(tableName);
	}

	/**
	 * Mark resharding as complete
	 */
	async markReshardingComplete(tableName: string): Promise<void> {
		this.ensureCreated();
		return this.resharding.markReshardingComplete(tableName);
	}

	/**
	 * Mark resharding as failed
	 */
	async markReshardingFailed(
		tableName: string,
		errorMessage: string,
	): Promise<void> {
		this.ensureCreated();
		return this.resharding.markReshardingFailed(tableName, errorMessage);
	}

	/**
	 * Perform atomic status switch: mark target shards as 'active' and source shard as 'to_be_deleted'
	 */
	async atomicStatusSwitch(tableName: string): Promise<void> {
		this.ensureCreated();
		return this.resharding.atomicStatusSwitch(tableName);
	}

	/**
	 * Delete a virtual shard
	 * Only allows deletion of shards with status 'to_be_deleted', 'pending', or 'failed'
	 */
	async deleteVirtualShard(
		tableName: string,
		shardId: number,
	): Promise<{ success: boolean; error?: string }> {
		this.ensureCreated();
		return this.resharding.deleteVirtualShard(tableName, shardId);
	}

	/**
	 * Mark target shards as 'failed' when resharding fails
	 * Used for cleanup when resharding operations encounter errors
	 */
	async markTargetShardsAsFailed(
		tableName: string,
		targetShardIds: number[],
	): Promise<void> {
		this.ensureCreated();
		return this.resharding.markTargetShardsAsFailed(tableName, targetShardIds);
	}

	// ============================================================================
	// Virtual Index Management (from virtual-indexes.ts)
	// ============================================================================

	/**
	 * Create a virtual index
	 */
	async createVirtualIndex(
		indexName: string,
		tableName: string,
		columns: string[],
		indexType: "hash" | "unique",
	): Promise<{ success: boolean; error?: string }> {
		this.ensureCreated();
		return this.virtualIndexes.createVirtualIndex(
			indexName,
			tableName,
			columns,
			indexType,
		);
	}

	/**
	 * Update virtual index status
	 */
	async updateIndexStatus(
		indexName: string,
		status: IndexStatus,
		errorMessage?: string,
	): Promise<void> {
		this.ensureCreated();
		return this.virtualIndexes.updateIndexStatus(
			indexName,
			status,
			errorMessage,
		);
	}

	/**
	 * Batch upsert multiple virtual index entries
	 */
	async batchUpsertIndexEntries(
		indexName: string,
		entries: Array<{ keyValue: string; shardIds: number[] }>,
	): Promise<{ count: number }> {
		this.ensureCreated();
		return this.virtualIndexes.batchUpsertIndexEntries(indexName, entries);
	}

	/**
	 * Get shard IDs for a specific indexed value
	 */
	async getIndexedShards(
		indexName: string,
		keyValue: string,
	): Promise<number[] | null> {
		this.ensureCreated();
		return this.virtualIndexes.getIndexedShards(indexName, keyValue);
	}

	/**
	 * Add a shard ID to an index entry
	 */
	async addShardToIndexEntry(
		indexName: string,
		keyValue: string,
		shardId: number,
	): Promise<void> {
		// Skip if topology not created
		if (!this.isCreated()) {
			return;
		}
		return this.virtualIndexes.addShardToIndexEntry(
			indexName,
			keyValue,
			shardId,
		);
	}

	/**
	 * Remove a shard ID from an index entry
	 */
	async removeShardFromIndexEntry(
		indexName: string,
		keyValue: string,
		shardId: number,
	): Promise<void> {
		// Skip if topology not created
		if (!this.isCreated()) {
			return;
		}
		return this.virtualIndexes.removeShardFromIndexEntry(
			indexName,
			keyValue,
			shardId,
		);
	}

	/**
	 * Maintain indexes for DELETE operation (synchronous)
	 */
	async maintainIndexesForDelete(
		rows: Record<string, SqlParam>[],
		indexes: Array<{ index_name: string; columns: string }>,
		shardId: number,
	): Promise<void> {
		// Skip if topology not created or no indexes
		if (!this.isCreated() || indexes.length === 0) {
			return;
		}
		return this.virtualIndexes.maintainIndexesForDelete(rows, indexes, shardId);
	}

	/**
	 * Maintain indexes for UPDATE operation (synchronous)
	 */
	async maintainIndexesForUpdate(
		oldRows: Record<string, SqlParam>[],
		newRows: Record<string, SqlParam>[],
		indexes: Array<{ index_name: string; columns: string }>,
		shardId: number,
	): Promise<void> {
		// Skip if topology not created or no indexes
		if (!this.isCreated() || indexes.length === 0) {
			return;
		}
		return this.virtualIndexes.maintainIndexesForUpdate(
			oldRows,
			newRows,
			indexes,
			shardId,
		);
	}

	/**
	 * Delete a virtual index and all its entries
	 */
	async dropVirtualIndex(indexName: string): Promise<{ success: boolean }> {
		this.ensureCreated();
		return this.virtualIndexes.dropVirtualIndex(indexName);
	}

	/**
	 * Get all index entries that include a specific shard
	 */
	async getIndexEntriesForShard(
		indexName: string,
		shardId: number,
	): Promise<Array<{ key_value: string }>> {
		this.ensureCreated();
		return this.virtualIndexes.getIndexEntriesForShard(indexName, shardId);
	}

	/**
	 * Apply a batch of index changes
	 */
	async batchMaintainIndexes(
		changes: Array<{
			operation: "add" | "remove";
			index_name: string;
			key_value: string;
			shard_id: number;
		}>,
	): Promise<void> {
		this.ensureCreated();
		return this.virtualIndexes.batchMaintainIndexes(changes);
	}

	// ============================================================================
	// Administration & System Operations (from administration.ts)
	// ============================================================================

	/**
	 * Get all topology data needed for query planning
	 */
	async getQueryPlanData(
		tableName: string,
		statement: Statement,
		params: SqlParam[],
		correlationId?: string,
	): Promise<QueryPlanData> {
		this.ensureCreated();
		return this.administration.getQueryPlanData(
			tableName,
			statement,
			params,
			correlationId,
		);
	}

	/**
	 * Async job tracking: Create a new async job record
	 */
	async createAsyncJob(
		jobId: string,
		jobType: "reshard_table" | "build_index" | "maintain_index",
		tableName: string,
		metadata?: Record<string, unknown>,
	): Promise<void> {
		this.ensureCreated();
		return this.administration.createAsyncJob(
			jobId,
			jobType,
			tableName,
			metadata,
		);
	}

	/**
	 * Async job tracking: Mark an async job as running
	 */
	async startAsyncJob(jobId: string): Promise<void> {
		this.ensureCreated();
		return this.administration.startAsyncJob(jobId);
	}

	/**
	 * Async job tracking: Mark an async job as completed
	 */
	async completeAsyncJob(jobId: string): Promise<void> {
		this.ensureCreated();
		return this.administration.completeAsyncJob(jobId);
	}

	/**
	 * Async job tracking: Mark an async job as failed
	 */
	async failAsyncJob(jobId: string, errorMessage: string): Promise<void> {
		this.ensureCreated();
		return this.administration.failAsyncJob(jobId, errorMessage);
	}

	/**
	 * Async job tracking: Increment retry counter for an async job
	 */
	async incrementAsyncJobRetries(jobId: string): Promise<void> {
		this.ensureCreated();
		return this.administration.incrementAsyncJobRetries(jobId);
	}

	/**
	 * Async job tracking: Get an async job by ID
	 */
	async getAsyncJob(jobId: string): Promise<AsyncJob | null> {
		this.ensureCreated();
		return this.administration.getAsyncJob(jobId);
	}

	/**
	 * Async job tracking: Get all async jobs for a table
	 */
	async getAsyncJobsForTable(
		tableName: string,
		limit: number = 100,
	): Promise<AsyncJob[]> {
		this.ensureCreated();
		return this.administration.getAsyncJobsForTable(tableName, limit);
	}

	/**
	 * Alarm handler - runs every 5 minutes to check storage capacity and status
	 */
	override async alarm(): Promise<void> {
		return this.administration.alarm.call(this);
	}

	// ============================================================================
	// Private Helper Methods (used by multiple modules)
	// ============================================================================

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
		return this.administration.determineShardTargets(
			statement,
			tableMetadata,
			tableShards,
			virtualIndexes,
			params,
		);
	}

	/**
	 * Calculate shard ID for INSERT based on shard key value
	 * Used by AdministrationOperations
	 */
	public getShardIdForInsert(
		statement: InsertStatement,
		tableMetadata: TableMetadata,
		numShards: number,
		params: SqlParam[],
	): number {
		// Find the shard key column in the INSERT
		const columnIndex = statement.columns?.findIndex(
			(col) => col.name === tableMetadata.shard_key,
		);

		if (columnIndex === undefined || columnIndex === -1) {
			throw new Error(
				`Shard key '${tableMetadata.shard_key}' not found in INSERT statement`,
			);
		}

		// Get the value for the shard key
		// statement.values[0] is an array of expressions, one for each column
		if (statement.values.length === 0) {
			throw new Error("INSERT statement has no values");
		}

		const valueExpression = statement.values[0]?.[columnIndex];
		const value = this.extractValueFromExpression(valueExpression!, params);

		if (value === null || value === undefined) {
			throw new Error(`Could not resolve shard key value from parameters`);
		}

		// Hash the value to get shard ID
		return this.hashToShardId(String(value), numShards);
	}

	/**
	 * Try to determine shard ID from WHERE clause
	 * Handles simple equality conditions, AND expressions, and IN expressions on shard key
	 * For IN expressions, returns null since they can span multiple shards
	 * Used by AdministrationOperations
	 *
	 * @param where - WHERE clause expression
	 * @param tableMetadata - Table metadata
	 * @param shardIds - Array of actual shard IDs (may not be 0-indexed after resharding)
	 * @param params - Query parameters
	 */
	public getShardIdFromWhere(
		where: Expression,
		tableMetadata: TableMetadata,
		shardIds: number[],
		params: SqlParam[],
	): number | null {
		if (!where) {
			return null;
		}

		if (where.type === "BinaryExpression") {
			const binExpr = where as BinaryExpression;

			// Handle simple equality on the shard key
			if (binExpr.operator === "=") {
				let columnName: string | null = null;
				let valueExpression: Expression | null = null;

				if (binExpr.left.type === "Identifier") {
					columnName = binExpr.left.name;
					valueExpression = binExpr.right;
				} else if (binExpr.right.type === "Identifier") {
					columnName = binExpr.right.name;
					valueExpression = binExpr.left;
				}

				if (columnName === tableMetadata.shard_key && valueExpression) {
					const value = this.extractValueFromExpression(
						valueExpression,
						params,
					);
					if (value !== null && value !== undefined) {
						// Hash to index, then map to actual shard ID (handles non-0-indexed shards after resharding)
						const hashIndex = this.hashToShardId(
							String(value),
							shardIds.length,
						);
						return shardIds[hashIndex]!;
					}
				}
			}

			// Handle IN expressions on shard key - return null to indicate multiple potential shards
			// These should be handled by virtual index lookup or full shard scan
			if (binExpr.operator === "IN") {
				let columnName: string | null = null;
				if (binExpr.left.type === "Identifier") {
					columnName = binExpr.left.name;
				}
				if (columnName === tableMetadata.shard_key) {
					// IN expressions can map to multiple shards, return null to let virtual index handle it
					return null;
				}
			}

			// Handle AND expressions - recursively check both sides
			if (binExpr.operator === "AND") {
				// Try to find shard key condition on left side
				const leftShardId = this.getShardIdFromWhere(
					binExpr.left,
					tableMetadata,
					shardIds,
					params,
				);
				if (leftShardId !== null) {
					return leftShardId;
				}

				// Try to find shard key condition on right side
				const rightShardId = this.getShardIdFromWhere(
					binExpr.right,
					tableMetadata,
					shardIds,
					params,
				);
				if (rightShardId !== null) {
					return rightShardId;
				}
			}
		}

		return null;
	}

	/**
	 * Extract value from an expression node
	 */
	private extractValueFromExpression(
		expression: Expression,
		params: SqlParam[],
	): SqlParam {
		if (!expression) {
			return null;
		}
		if (expression.type === "Literal") {
			return (expression as Literal).value;
		} else if (expression.type === "Placeholder") {
			return params[(expression as Placeholder).parameterIndex]!;
		}
		return null;
	}

	/**
	 * Hash a value to a shard ID with optional offset
	 * @param value - The value to hash
	 * @param numShards - Number of shards to distribute across
	 * @param offset - Optional offset for shard IDs (for handling non-zero starting shard IDs)
	 */
	private hashToShardId(
		value: string,
		numShards: number,
		offset: number = 0,
	): number {
		let hash = 0;
		for (let i = 0; i < value.length; i++) {
			hash = (hash << 5) - hash + value.charCodeAt(i);
			hash = hash & hash;
		}
		return (Math.abs(hash) % numShards) + offset;
	}

	/**
	 * Check if WHERE clause can use a virtual index to reduce shard fan-out
	 */
	async getShardsFromIndexedWhere(
		where: Expression,
		tableName: string,
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
	): Promise<Array<{
		table_name: string;
		shard_id: number;
		node_id: string;
	}> | null> {
		return this.virtualIndexes.getShardsFromIndexedWhere(
			where,
			tableName,
			tableShards,
			virtualIndexes,
			params,
		);
	}

	/**
	 * Maintain indexes for INSERT operation
	 */
	async maintainIndexesForInsert(
		statement: InsertStatement,
		indexes: Array<{ index_name: string; columns: string }>,
		shard: { table_name: string; shard_id: number; node_id: string },
		params: SqlParam[],
	): Promise<void> {
		return this.virtualIndexes.maintainIndexesForInsert(
			statement,
			indexes,
			shard,
			params,
		);
	}
}

// Re-export types for backwards compatibility
export type {
	StorageNode,
	TableMetadata,
	TableShard,
	ReshardingState,
	VirtualIndex,
	VirtualIndexEntry,
	AsyncJob,
	TopologyData,
	TopologyUpdates,
	IndexStatus,
	QueryPlanData,
	ShardRowCount,
};
