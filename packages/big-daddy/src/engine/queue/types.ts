/**
 * Queue job type definitions for virtual index operations
 */

/**
 * Job to build a virtual index from existing data
 * This is enqueued when CREATE INDEX is executed
 */
export interface IndexBuildJob {
	type: 'build_index';
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
	type: 'maintain_index';
	database_id: string;
	table_name: string;
	operation: 'UPDATE' | 'DELETE';
	shard_ids: number[];           // Shards that were affected by the operation
	affected_indexes: string[];    // Index names that need updating
	updated_columns?: string[];    // For UPDATE: which columns changed
	created_at: string;
	correlation_id?: string; // Optional correlation ID for tracing
}

/**
 * Union type of all possible index jobs
 */
export type IndexJob = IndexBuildJob | IndexMaintenanceJob;

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
