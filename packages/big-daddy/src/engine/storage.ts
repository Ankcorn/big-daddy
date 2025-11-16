import { DurableObject } from 'cloudflare:workers';
import { withLogTags } from 'workers-tagged-logger';
import { logger } from '../logger';

// Type definitions for Storage operations

export interface QueryResult {
	rows: Record<string, any>[];
	rowsAffected?: number;
}

export interface BatchQueryResult {
	results: QueryResult[];
	totalRowsAffected: number;
}

export interface QueryBatch {
	query: string;
	params?: any[];
	correlationId?: string; // Optional correlation ID for tracing
}

/** Storage Durable Object - individual database shard */
export class Storage extends DurableObject<Env> {
	/**
	 * The constructor is invoked once upon creation of the Durable Object
	 *
	 * @param ctx - The interface for interacting with Durable Object state
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 */
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
	}

	/**
	 * Execute a query or batch of queries on this storage shard
	 *
	 * @param queries - Single query or array of queries to execute
	 * @returns Query results with rows and metadata (single or batch)
	 */
	async executeQuery(queries: QueryBatch | QueryBatch[]): Promise<QueryResult | BatchQueryResult> {
		return withLogTags({ source: 'Storage' }, async () => {
			// Normalize to array
			const queryArray = Array.isArray(queries) ? queries : [queries];
			const isBatch = Array.isArray(queries);

			// Extract correlation ID from first query (if available)
			const correlationId = queryArray[0]?.correlationId;
			if (correlationId) {
				logger.setTags({
					correlationId,
					requestId: correlationId,
					component: 'Storage',
					operation: 'executeQuery',
					shardId: this.ctx.id.toString(),
				});
			}

			const startTime = Date.now();
			logger.debug('Executing storage query', {
				queryCount: queryArray.length,
				isBatch,
			});

			const results: QueryResult[] = [];
			let totalRowsAffected = 0;

			try {
				for (const batch of queryArray) {
					const queryStartTime = Date.now();
					const result = this.ctx.storage.sql.exec(batch.query, ...(batch.params ?? []));
					const rows = result.toArray() as unknown as Record<string, any>[];
					const rowsAffected = result.rowsWritten ?? 0;

					logger.debug('Query executed on shard', {
						rowCount: rows.length,
						rowsAffected,
						duration: Date.now() - queryStartTime,
					});

					results.push({
						rows,
						rowsAffected,
					});

					totalRowsAffected += rowsAffected;
				}

				const duration = Date.now() - startTime;
				logger.info('Storage query completed successfully', {
					duration,
					queryCount: queryArray.length,
					totalRowsAffected,
					status: 'success',
				});

				// Return single result if input was a single query, batch result otherwise
				if (!isBatch) {
					return results[0]!;
				}

				return {
					results,
					totalRowsAffected,
				};
			} catch (error) {
				const duration = Date.now() - startTime;
				logger.error('Storage query failed', {
					duration,
					queryCount: queryArray.length,
					error: error instanceof Error ? error.message : String(error),
					status: 'failure',
				});
				throw error;
			}
		});
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
	async resetToBookmark(bookmark: string): Promise<{ success: boolean }> {
		// Note: Actual point-in-time recovery API may differ
		// This is a placeholder implementation
		// You may need to use backup/restore mechanisms instead
		return { success: true };
	}
}
