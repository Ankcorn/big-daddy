/**
 * Topology Type Definitions
 *
 * These types define the structure of distributed database topology,
 * including storage nodes, tables, shards, virtual indexes, and async jobs.
 */

export type { SqlParam } from "../conductor/types";

export interface StorageNode {
	node_id: string;
	created_at: number;
	updated_at: number;
	status: "active" | "down" | "draining";
	capacity_used: number;
	error: string | null;
}

export interface TableMetadata {
	table_name: string;
	primary_key: string;
	primary_key_type: string;
	shard_strategy: "hash" | "range";
	shard_key: string;
	num_shards: number;
	block_size: number;
	created_at: number;
	updated_at: number;
}

export interface TableShard {
	table_name: string;
	shard_id: number;
	node_id: string;
	status: "active" | "pending" | "to_be_deleted" | "failed";
	row_count: number;
	created_at: number;
	updated_at: number;
}

/**
 * Per-shard row count for a table
 */
export interface ShardRowCount {
	table_name: string;
	shard_id: number;
	row_count: number;
	updated_at: number;
}

/**
 * Resharding state - tracks ongoing resharding operations
 * Allows the conductor to know when to log changes and when resharding is complete
 */
export interface ReshardingState {
	table_name: string;
	source_shard_id: number;
	target_shard_ids: string; // Stored as JSON array
	change_log_id: string;
	status:
		| "pending_shards"
		| "copying"
		| "replaying"
		| "verifying"
		| "complete"
		| "failed";
	error_message: string | null;
	created_at: number;
	updated_at: number;
}

export type IndexStatus = "building" | "ready" | "failed" | "rebuilding";

export interface VirtualIndex {
	index_name: string;
	table_name: string;
	columns: string; // Stored as JSON array of column names for composite indexes
	index_type: "hash" | "unique";
	status: IndexStatus;
	error_message: string | null;
	created_at: number;
	updated_at: number;
}

export interface VirtualIndexEntry {
	index_name: string;
	key_value: string;
	shard_ids: string; // Stored as JSON array string
	updated_at: number;
}

/**
 * Async job tracking - monitors long-running operations like resharding and index building
 */
export interface AsyncJob {
	job_id: string;
	job_type: "reshard_table" | "build_index" | "maintain_index";
	table_name: string;
	status: "pending" | "running" | "completed" | "failed";
	error_message: string | null;
	started_at: number;
	ended_at: number | null;
	duration_ms: number | null;
	metadata: string; // JSON string for job-specific data
	created_at: number;
	updated_at: number;
}

export interface TopologyData {
	storage_nodes: StorageNode[];
	tables: TableMetadata[];
	table_shards: TableShard[];
	virtual_indexes: VirtualIndex[];
	virtual_index_entries: VirtualIndexEntry[];
	resharding_states: ReshardingState[];
	async_jobs: AsyncJob[];
}

export interface StorageNodeUpdate {
	node_id: string;
	status?: "active" | "down" | "draining";
	capacity_used?: number;
	error?: string | null;
}

export interface TableMetadataUpdate {
	table_name: string;
	num_shards?: number;
	block_size?: number;
}

export interface TopologyUpdates {
	// Storage nodes are immutable
	storage_nodes?: never;
	tables?: {
		add?: Omit<TableMetadata, "created_at" | "updated_at">[];
		update?: TableMetadataUpdate[];
		remove?: string[];
	};
}

export interface QueryPlanData {
	shardsToQuery: Array<{
		table_name: string;
		shard_id: number;
		node_id: string;
	}>;
	virtualIndexes: Array<{
		table_name: string;
		index_name: string;
		columns: string;
		index_type: "hash" | "unique";
	}>;
	shardKey: string;
}
