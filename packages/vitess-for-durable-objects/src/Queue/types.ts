/**
 * Queue job type definitions for virtual index operations
 */

/**
 * Job to build a virtual index from existing data
 */
export interface IndexBuildJob {
	type: 'build_index';
	database_id: string;
	table_name: string;
	column_name: string;
	index_name: string;
	created_at: string;
}

/**
 * Job to update a virtual index entry after a data modification
 */
export interface IndexUpdateJob {
	type: 'index_update';
	database_id: string;
	table_name: string;
	operation: 'insert' | 'update' | 'delete';
	shard_id: number;
	indexed_values: Record<string, any>; // New values (for insert/update)
	old_values?: Record<string, any>; // Old values (for update/delete)
}

/**
 * Union type of all possible index jobs
 */
export type IndexJob = IndexBuildJob | IndexUpdateJob;

/**
 * Queue message batch from Cloudflare
 */
export interface MessageBatch<T = IndexJob> {
	queue: string;
	messages: Array<Message<T>>;
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
