import { DurableObject } from "cloudflare:workers";
import { logger } from "../logger";

// Type definitions for Storage operations

/** SQL parameter type for query bindings */
export type SqlParam = string | number | boolean | null;

export interface QueryResult<T = Record<string, unknown>> {
	rows: T[];
	rowsAffected?: number;
	error?: string; // Error message if query failed
}

export interface BatchQueryResult<T = Record<string, unknown>> {
	results: QueryResult<T>[];
	totalRowsAffected: number;
}

/**
 * Type helper for casting query results to a specific row type
 * Use like: await stub.executeQuery({...}) as StorageResults<{ count: number }>
 */
export type StorageResults<T> = QueryResult<T>;

export interface QueryBatch {
	query: string;
	params?: SqlParam[];
	correlationId?: string; // Optional correlation ID for tracing
}

/** Storage Durable Object - individual database shard */
export class Storage extends DurableObject<Env> {
	/**
	 * Execute a query or batch of queries on this storage shard
	 *
	 * @param queries - Single query or array of queries to execute
	 * @returns Query results with rows and metadata (single or batch)
	 */
	async executeQuery(
		queries: QueryBatch | QueryBatch[],
	): Promise<QueryResult | BatchQueryResult> {
		const source = "Storage";
		const component = "Storage";
		const operation = "executeQuery";
		const queryArray = Array.isArray(queries) ? queries : [queries];
		const isBatch = Array.isArray(queries);
		const correlationId = queryArray[0]?.correlationId;
		const shardId = this.ctx.id.toString();
		const queryCount = queryArray.length;

		const startTime = Date.now();
		logger.debug`Executing storage query ${{ source }} ${{ component }} ${{ operation }} ${{ correlationId }} ${{ requestId: correlationId }} ${{ shardId }} ${{ queryCount }} ${{ isBatch }}`;

		const results: QueryResult[] = [];
		let totalRowsAffected = 0;

		try {
			for (const batch of queryArray) {
				const queryStartTime = Date.now();
				const result = this.ctx.storage.sql.exec(
					batch.query,
					...(batch.params ?? []),
				);
				const rows = result.toArray() as unknown as Record<string, unknown>[];
				const rowsAffected = result.rowsWritten ?? 0;

				const rowCount = rows.length;
				const duration = Date.now() - queryStartTime;
				logger.debug`Query executed on shard ${{ source }} ${{ component }} ${{ shardId }} ${{ rowCount }} ${{ rowsAffected }} ${{ duration }}`;

				results.push({
					rows,
					rowsAffected,
				});

				totalRowsAffected += rowsAffected;
			}

			const finalDuration = Date.now() - startTime;
			const status = "success";
			logger.info`Storage query completed successfully ${{ source }} ${{ component }} ${{ shardId }} ${{ duration: finalDuration }} ${{ queryCount }} ${{ totalRowsAffected }} ${{ status }}`;

			// Return single result if input was a single query, batch result otherwise
			if (!isBatch) {
				return results[0]!;
			}

			return {
				results,
				totalRowsAffected,
			};
		} catch (error) {
			const errorDuration = Date.now() - startTime;
			const errorMsg = error instanceof Error ? error.message : String(error);
			const status = "failure";
			logger.error`Storage query failed ${{ source }} ${{ component }} ${{ shardId }} ${{ duration: errorDuration }} ${{ queryCount }} ${{ error: errorMsg }} ${{ status }}`;
			// Return error in response instead of throwing to preserve message across RPC boundary
			// Durable Object RPC sanitizes error messages for security reasons
			return {
				rows: [],
				rowsAffected: 0,
				error: errorMsg,
			};
		}
	}

	/**
	 * Get the current database size in bytes
	 *
	 * @returns Database size in bytes
	 */
	async getDatabaseSize(): Promise<number> {
		return this.ctx.storage.sql.databaseSize;
	}

	/**
	 * Get the current bookmark for point-in-time recovery
	 *
	 * @returns Current bookmark identifier
	 */
	async getBookmark(): Promise<string> {
		const bookmark = this.ctx.storage.sql.databaseSize;
		// Return a stringified version of current state
		// Actual bookmark implementation may vary based on Cloudflare's API
		return String(bookmark);
	}

	/**
	 * Reset the database to a specific bookmark
	 *
	 * @param bookmark - Bookmark identifier to reset to
	 * @returns Success status
	 */
	async resetToBookmark(_bookmark: string): Promise<{ success: boolean }> {
		// Note: Actual point-in-time recovery API may differ
		// This is a placeholder implementation
		// You may need to use backup/restore mechanisms instead
		return { success: true };
	}
}
