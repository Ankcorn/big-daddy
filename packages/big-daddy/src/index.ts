/**
 * Vitess for Durable Objects - Distributed SQL Database on Cloudflare
 *
 * This worker provides an RPC interface for executing SQL queries across
 * a sharded database cluster using Durable Objects for storage and coordination.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your Durable Object in action
 * - Run `npm run deploy` to publish your application
 *
 * Learn more at https://developers.cloudflare.com/durable-objects
 */

import { WorkerEntrypoint } from 'cloudflare:workers';
import { withLogTags } from 'workers-tagged-logger';
import { logger } from './logger';
import { createConductor } from './engine/conductor';
import { queueHandler } from './queue-consumer';
import type { QueryResult } from './engine/conductor';
import type { MessageBatch, IndexJob } from './engine/queue/types';

// Export Durable Objects
export { Storage } from './engine/storage';
export { Topology } from './engine/topology';

// Export types
export type { QueryResult } from './engine/conductor';

/**
 * Vitess Worker - RPC-enabled SQL query interface
 *
 * This worker exposes the Conductor's SQL interface via RPC, allowing
 * other workers to execute queries via service bindings.
 *
 * @example Service Binding Usage (from another worker):
 * ```typescript
 * const result = await env.VITESS.sql(['SELECT * FROM users WHERE id = ', ''], [123]);
 * ```
 *
 * @example HTTP API Usage:
 * ```typescript
 * POST /sql
 * {
 *   "database": "my-database",
 *   "query": "SELECT * FROM users WHERE id = ?",
 *   "params": [123]
 * }
 * ```
 */
export default class VitessWorker extends WorkerEntrypoint<Env> {
	/**
	 * RPC method: Execute a SQL query
	 *
	 * This method is callable via service bindings from other workers:
	 * const result = await env.VITESS.sql(strings, values, databaseId);
	 *
	 * @param strings - Template string array (from tagged template literal)
	 * @param values - Parameter values
	 * @param databaseId - Database identifier (defaults to 'default')
	 * @returns Query result with rows and metadata
	 */
	async sql(
		strings: TemplateStringsArray | string[],
		values: any[] = [],
		databaseId: string = 'default',
		correlationId?: string
	): Promise<QueryResult> {
		return withLogTags({ source: 'VitessWorker' }, async () => {
			const startTime = Date.now();
			const cid = correlationId || crypto.randomUUID();

			// Set correlation ID for all logs in this context
			logger.setTags({
				correlationId: cid,
				requestId: cid, // Workers Logs UI uses requestId
				databaseId,
				component: 'VitessWorker',
				operation: 'sql',
			});

			logger.info('Executing SQL query via RPC', {
				paramCount: values.length,
			});

			try {
				const conductor = createConductor(databaseId, this.env);

				// Convert to TemplateStringsArray for the conductor
				const templateStrings = strings as any as TemplateStringsArray;
				const result = await conductor.sql(templateStrings, cid, ...values);

				const duration = Date.now() - startTime;
				logger.info('SQL query completed successfully', {
					duration,
					rowCount: result.rows.length,
					rowsAffected: result.rowsAffected,
					status: 'success',
				});

				return result;
			} catch (error) {
				const duration = Date.now() - startTime;
				logger.error('SQL query failed', {
					duration,
					status: 'failure',
					error: error instanceof Error ? error.message : String(error),
					errorCode: error instanceof Error ? error.name : 'UnknownError',
				});
				throw error;
			}
		});
	}

	/**
	 * HTTP fetch handler for REST API access
	 *
	 * Supports POST /sql for executing queries via HTTP
	 */
	async fetch(request: Request): Promise<Response> {
		return withLogTags({ source: 'VitessWorker' }, async () => {
			const url = new URL(request.url);

			// Generate or extract correlation ID from request headers
			const correlationId =
				request.headers.get('x-correlation-id') ||
				request.headers.get('cf-ray') ||
				crypto.randomUUID();

			logger.setTags({
				correlationId,
				requestId: correlationId,
				component: 'VitessWorker',
				operation: 'fetch',
			});

			logger.debug('HTTP request received', {
				method: request.method,
				path: url.pathname,
			});

			// Handle SQL query endpoint
			if (url.pathname === '/sql' && request.method === 'POST') {
				try {
					const body = await request.json<{
						database?: string;
						query: string;
						params?: any[];
					}>();

					const { database = 'default', query, params = [] } = body;

					// Split query into template strings array and params
					// For now, we'll treat the query string as a single template
					const strings = [query] as any as TemplateStringsArray;
					const result = await this.sql(strings, params, database, correlationId);

					return new Response(JSON.stringify(result, null, 2), {
						headers: { 'Content-Type': 'application/json' },
					});
				} catch (error) {
					logger.error('SQL query request failed', {
						error: error instanceof Error ? error.message : String(error),
						status: 'failure',
					});
					return new Response(
						JSON.stringify({
							error: error instanceof Error ? error.message : String(error),
						}),
						{
							status: 400,
							headers: { 'Content-Type': 'application/json' },
						}
					);
				}
			}

			// Handle health check
			if (url.pathname === '/health') {
				return new Response(JSON.stringify({ status: 'ok' }), {
					headers: { 'Content-Type': 'application/json' },
				});
			}

			// Default response
			return new Response(
				JSON.stringify({
					message: 'Vitess for Durable Objects',
					endpoints: {
						'/sql': 'POST - Execute SQL query',
						'/health': 'GET - Health check',
					},
				}),
				{
					headers: { 'Content-Type': 'application/json' },
				}
			);
		});
	}

	/**
	 * Queue handler for processing virtual index jobs
	 */
	async queue(batch: MessageBatch<unknown>): Promise<void> {
		return withLogTags({ source: 'VitessWorker' }, async () => {
			const correlationId = crypto.randomUUID();
			logger.setTags({
				correlationId,
				requestId: correlationId,
				component: 'VitessWorker',
				operation: 'queue',
			});

			logger.info('Processing queue batch', {
				batchSize: batch.messages.length,
			});

			try {
				await queueHandler(batch as MessageBatch<IndexJob>, this.env, correlationId);
				logger.info('Queue batch processed successfully', {
					batchSize: batch.messages.length,
					status: 'success',
				});
			} catch (error) {
				logger.error('Queue batch processing failed', {
					batchSize: batch.messages.length,
					status: 'failure',
					error: error instanceof Error ? error.message : String(error),
				});
				throw error;
			}
		});
	}
}
