/**
 * Centralized logger for Big Daddy database system
 *
 * Uses hatchlet for structured logging with correlation IDs
 * and tag-based context tracking across distributed Durable Objects.
 */

import { Logger } from "hatchlet";

/**
 * Standard log tags used throughout the Big Daddy system
 */
export type BigDaddyLogTags = {
	/** Correlation ID for tracing requests across the distributed system */
	correlationId: string;
	/** Request ID (alias for correlationId, used by Workers Logs UI) */
	requestId?: string;
	/** Database identifier being queried */
	databaseId?: string;
	/** Component generating the log (VitessWorker, Conductor, Topology, Storage, QueueConsumer) */
	component?: string;
	/** Operation being performed (query, route, execute, maintain, etc.) */
	operation?: string;
	/** Query type (SELECT, INSERT, UPDATE, DELETE) */
	queryType?: string;
	/** Table being operated on */
	table?: string;
	/** Shard ID for storage operations */
	shardId?: string;
	/** Index name for index operations */
	indexName?: string;
	/** Job ID for queue operations */
	jobId?: string;
	/** Job type for queue operations */
	jobType?: string;
	/** Cache hit/miss indicator */
	cacheHit?: boolean;
	/** Duration in milliseconds */
	duration?: number;
	/** Error code or type */
	errorCode?: string;
	/** Number of shards involved */
	shardCount?: number;
	/** Status of operation (success, failure, partial) */
	status?: string;
	/** Source tag (auto-populated by decorators) */
	source?: string;
};

/**
 * Global logger instance for Big Daddy
 *
 * This logger should be used throughout the application with structured
 * tags to enable effective debugging and analytics.
 */
export const logger = new Logger();
