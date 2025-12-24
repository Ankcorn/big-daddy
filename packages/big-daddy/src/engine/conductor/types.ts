import type { Storage } from "../storage";
import type { Topology } from "../topology/index";
import type { CacheStats, TopologyCache } from "../utils/topology-cache";

/**
 * SQL parameter type - only scalar values are supported by SQLite
 */
export type SqlParam = string | number | boolean | null;

/**
 * Error types for conductor operations
 */
export class TableAlreadyExistsError extends Error {
	constructor(public tableName: string) {
		super(`Table '${tableName}' already exists in topology`);
		this.name = "TableAlreadyExistsError";
	}
}

export class TopologyFetchError extends Error {
	public override cause?: unknown;

	constructor(
		public databaseId: string,
		cause?: unknown,
	) {
		super(`Failed to fetch topology for database '${databaseId}'`);
		this.name = "TopologyFetchError";
		this.cause = cause;
	}
}

export class TopologyUpdateError extends Error {
	public override cause?: unknown;

	constructor(
		public tableName: string,
		cause?: unknown,
	) {
		super(`Failed to update topology for table '${tableName}'`);
		this.name = "TopologyUpdateError";
		this.cause = cause;
	}
}

export class StorageExecutionError extends Error {
	public override cause?: unknown;

	constructor(
		public nodeId: string,
		public query: string,
		cause?: unknown,
	) {
		super(`Failed to execute query on node '${nodeId}'`);
		this.name = "StorageExecutionError";
		this.cause = cause;
	}
}

/**
 * Cache statistics for a query
 */
export interface QueryCacheStats {
	cacheHit: boolean; // Whether this specific query was a cache hit
	totalHits: number; // Total cache hits for this conductor instance
	totalMisses: number; // Total cache misses for this conductor instance
	cacheSize: number; // Number of entries currently in cache
}

/**
 * Statistics for a single shard query
 */
export interface ShardStats {
	shardId: number;
	nodeId: string;
	rowsReturned: number;
	rowsAffected?: number;
	duration: number; // in milliseconds
}

/**
 * Result from a SQL query execution
 */
export interface QueryResult<T = Record<string, unknown>> {
	rows: T[];
	rowsAffected?: number;
	cacheStats?: QueryCacheStats; // Cache statistics (only for SELECT queries)
	shardStats?: ShardStats[]; // Statistics for each shard queried
}

/**
 * Conductor API interface with sql execution and cache management
 */
export interface ConductorAPI {
	sql: <T = Record<string, unknown>>(
		strings: TemplateStringsArray,
		...values: SqlParam[]
	) => Promise<QueryResult<T>>;
	getCacheStats: () => CacheStats;
	clearCache: () => void;
}

/**
 * Handler context passed to query handlers
 */
export interface QueryHandlerContext {
	databaseId: string;
	correlationId: string;
	storage: DurableObjectNamespace<Storage>;
	topology: DurableObjectNamespace<Topology>;
	indexQueue?: Queue;
	env?: Env;
	cache: TopologyCache;
}

/**
 * Shard to query
 */
export interface ShardInfo {
	table_name: string;
	shard_id: number;
	node_id: string;
}

/**
 * Shard execution result
 */
export interface ShardExecutionResult {
	results: QueryResult[];
	shardStats: ShardStats[];
}
