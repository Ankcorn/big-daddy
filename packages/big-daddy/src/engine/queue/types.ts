/**
 * Queue job type definitions for virtual index operations
 */

/** SQL parameter type for query bindings */
export type SqlParam = string | number | boolean | null;

/**
 * Job to build a virtual index from existing data
 * This is enqueued when CREATE INDEX is executed
 */
export interface IndexBuildJob {
	type: "build_index";
	database_id: string;
	table_name: string;
	columns: string[]; // Array of column names for composite indexes
	index_name: string;
	created_at: string;
	correlation_id?: string; // Optional correlation ID for tracing
}

/**
 * Job to maintain virtual indexes after UPDATE/DELETE operations
 * This is enqueued after writes complete to asynchronously update indexes
 */
export interface IndexMaintenanceJob {
	type: "maintain_index";
	database_id: string;
	table_name: string;
	operation: "UPDATE" | "DELETE";
	shard_ids: number[]; // Shards that were affected by the operation
	affected_indexes: string[]; // Index names that need updating
	updated_columns?: string[]; // For UPDATE: which columns changed
	created_at: string;
	correlation_id?: string; // Optional correlation ID for tracing
}

/**
 * Job containing deduplicated index maintenance events from DELETE/UPDATE
 * This is a more efficient alternative to IndexMaintenanceJob
 * Pre-computes the index changes at query time instead of re-querying storage
 */
export interface IndexMaintenanceEventJob {
	type: "maintain_index_events";
	database_id: string;
	table_name: string;
	events: Array<{
		index_name: string; // The index being updated
		key_value: string; // The indexed value (single or JSON-stringified composite)
		shard_id: number; // Which shard contains this index entry
		operation: "add" | "remove"; // 'add' for INSERT, 'remove' for DELETE
	}>;
	created_at: string;
	correlation_id?: string; // Optional correlation ID for tracing
}

/**
 * Job to reshard a table across multiple shards
 * This is enqueued when PRAGMA reshardTable is executed
 *
 * Phases:
 * 3A: Start change log (in Conductor)
 * 3B: Copy data from source shard to target shards
 * 3C: Replay all captured changes to target shards
 * 3D: Verify data integrity (count rows)
 */
export interface ReshardTableJob {
	type: "reshard_table";
	database_id: string;
	table_name: string;
	source_shard_id: number; // The original shard (usually 0)
	target_shard_ids: number[]; // New shard IDs to create
	shard_key: string; // Column to use for distribution (hash key)
	shard_strategy: "hash" | "range"; // Sharding strategy
	change_log_id: string; // ID to fetch change logs from queue
	created_at: string;
	correlation_id?: string;
	progress?: {
		phase: "copying" | "replaying" | "verifying";
		rows_copied?: number;
		total_rows?: number;
		changes_replayed?: number;
		last_processed_key?: string;
		status: "in_progress" | "completed" | "failed";
		error_message?: string;
	};
}

/**
 * Change log entry for writes during resharding
 * All INSERTs/UPDATEs/DELETEs during Phase 3 are captured
 * and replayed to new shards in Phase 3C
 */
export interface ReshardingChangeLog {
	type: "resharding_change_log";
	resharding_id: string; // Links to ReshardTableJob.change_log_id
	database_id: string;
	table_name: string;
	operation: "INSERT" | "UPDATE" | "DELETE";
	query: string; // Full SQL query
	params: SqlParam[]; // Query parameters
	timestamp: number; // When the change was captured
	correlation_id?: string;
}

/**
 * Union type of all possible queue jobs (index + resharding)
 */
export type IndexJob =
	| IndexBuildJob
	| IndexMaintenanceJob
	| IndexMaintenanceEventJob
	| ReshardTableJob
	| ReshardingChangeLog;

/**
 * Queue message batch from Cloudflare
 */
export interface MessageBatch<T = IndexJob> {
	queue: string;
	messages: readonly Message<T>[];
}

/**
 * Individual queue message
 */
export interface Message<T = IndexJob> {
	id: string;
	timestamp: Date;
	body: T;
	attempts: number;
}
