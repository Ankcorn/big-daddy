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

import { WorkerEntrypoint } from "cloudflare:workers";
import { Hono } from "hono";
import { dashboard } from "./dashboard";
import type { QueryResult } from "./engine/conductor";
import { createConductor } from "./engine/conductor";
import type { SqlParam } from "./engine/conductor/types";
import type { IndexJob, MessageBatch } from "./engine/queue/types";
import { Storage } from "./engine/storage";
import { Topology } from "./engine/topology/index";
import { logger } from "./logger";
import { queueHandler } from "./queue-consumer";
// Export Durable Objects
export { Storage, Topology };

// Export types
export type { ConductorAPI, QueryResult } from "./engine/conductor";
// Export conductor creation functions
export { createConductor } from "./engine/conductor";
export type { CacheStats } from "./engine/utils/topology-cache";

/**
 * Configuration options for createConnection
 */
export interface ConnectionConfig {
	/** Number of storage nodes to create for this database */
	nodes: number;
	/** Optional correlation ID for request tracking */
	correlationId?: string;
}

/**
 * SQL tagged template literal function
 */
export type SqlFunction = (
	strings: TemplateStringsArray,
	...values: SqlParam[]
) => Promise<QueryResult>;

/**
 * Environment interface with required Big Daddy bindings
 */
export interface BigDaddyEnv {
	STORAGE: DurableObjectNamespace<Storage>;
	TOPOLOGY: DurableObjectNamespace<Topology>;
	INDEX_QUEUE: Queue;
	AI: Ai; // Workers AI binding for natural language query parsing
}

/**
 * Create a connection to a database with automatic topology initialization
 *
 * This function:
 * 1. Checks if topology exists in the cache
 * 2. Checks if topology exists in the Topology DO
 * 3. Creates a new topology if it doesn't exist
 * 4. Returns a sql tagged template literal function ready to use
 *
 * @param databaseId - Unique identifier for the database
 * @param config - Configuration options (nodes, correlationId)
 * @param env - Worker environment with Durable Object bindings
 * @returns SQL tagged template literal function
 *
 * @example
 * ```typescript
 * const sql = await createConnection('my-database', { nodes: 10 }, env);
 * const result = await sql`SELECT * FROM users WHERE id = ${123}`;
 * ```
 */
export async function createConnection(
	databaseId: string,
	config: ConnectionConfig,
	env: BigDaddyEnv,
): Promise<SqlFunction> {
	const cid = config.correlationId || crypto.randomUUID();
	const source = "createConnection";
	const component = "createConnection";
	const nodes = config.nodes;

	logger.debug`Creating connection ${{ source }} ${{ component }} ${{ correlationId: cid }} ${{ requestId: cid }} ${{ databaseId }} ${{ nodes }}`;

	// Get the Topology DO stub
	const topologyId = env.TOPOLOGY.idFromName(databaseId);
	const topologyStub = env.TOPOLOGY.get(topologyId);

	// Try to check if topology exists by calling getTopology
	// If it throws an error about topology not being created, we'll create it
	try {
		const topologyData = await topologyStub.getTopology();
		const existingNodes = topologyData.storage_nodes.length;

		logger.debug`Topology already exists ${{ source }} ${{ component }} ${{ databaseId }} ${{ nodes: existingNodes }}`;
	} catch (error) {
		// Check if the error is about topology not being created
		if (
			error instanceof Error &&
			error.message.includes("Topology not created")
		) {
			logger.info`Topology does not exist, creating ${{ source }} ${{ component }} ${{ databaseId }} ${{ nodes }}`;

			// Create topology with the specified number of nodes
			const result = await topologyStub.create(config.nodes);

			if (!result.success) {
				throw new Error(`Failed to create topology: ${result.error}`);
			}

			logger.info`Topology created successfully ${{ source }} ${{ component }} ${{ databaseId }} ${{ nodes }}`;
		} else {
			// Re-throw if it's a different error
			throw error;
		}
	}

	const client = createConductor(databaseId, cid, env as unknown as Env);

	// Return the sql function bound with the conductor client
	const sql: SqlFunction = async (
		strings: TemplateStringsArray,
		...values: SqlParam[]
	) => {
		return client.sql(strings, ...values);
	};

	return sql;
}

/**
 * Big Daddy Worker - RPC-enabled distributed SQL database
 *
 * This worker exposes the createConnection API via RPC, allowing
 * other workers to create database connections via service bindings.
 *
 * @example Service Binding Usage (from another worker):
 * ```typescript
 * const sql = await env.BIG_DADDY.createConnection('my-database', { nodes: 8 });
 * const result = await sql`SELECT * FROM users WHERE id = ${123}`;
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
export default class BigDaddy extends WorkerEntrypoint<Env> {
	/**
	 * RPC method: Create a connection to a database
	 *
	 * This method is callable via service bindings from other workers:
	 * const sql = await env.BIG_DADDY.createConnection('my-database', { nodes: 8 });
	 *
	 * @param databaseId - Unique identifier for the database
	 * @param config - Configuration options (nodes, correlationId)
	 * @returns SQL tagged template literal function
	 */
	async createConnection(
		databaseId: string,
		config: ConnectionConfig,
	): Promise<SqlFunction> {
		return createConnection(
			databaseId,
			config,
			this.env as unknown as BigDaddyEnv,
		);
	}

	/**
	 * HTTP fetch handler for REST API access and dashboard
	 *
	 * Mounts the dashboard app and handles API endpoints:
	 * - GET /dash/:databaseId - View database topology
	 * - POST /sql - Execute SQL query
	 * - GET /health - Health check
	 */
	override async fetch(request: Request): Promise<Response> {
		// Create a Hono app for this request
		const app = new Hono<{ Bindings: Env }>();

		// Generate or extract correlation ID from request headers
		const correlationId =
			request.headers.get("x-correlation-id") ||
			request.headers.get("cf-ray") ||
			crypto.randomUUID();

		// Error handling middleware
		app.onError((err, c) => {
			console.error("Hono error:", err);
			return c.json(
				{
					error: err instanceof Error ? err.message : String(err),
					stack: err instanceof Error ? err.stack : undefined,
				},
				500,
			);
		});

		// Mount the dashboard (which includes its own logger middleware)
		app.route("/", dashboard);

		// Handle SQL query endpoint
		app.post("/sql", async (c) => {
			try {
				const body = await c.req.json<{
					database?: string;
					query: string;
					params?: SqlParam[];
				}>();

				const { database = "default", query, params = [] } = body;

				// Create connection and execute query
				const sql = await this.createConnection(database, {
					nodes: 8,
					correlationId,
				});

				// Parse query to build template strings
				const strings = [query] as unknown as TemplateStringsArray;
				const result = await sql(strings, ...params);

				return c.json(result);
			} catch (error) {
				return c.json(
					{
						error: error instanceof Error ? error.message : String(error),
					},
					400,
				);
			}
		});

		// Handle health check
		app.get("/health", (c) => {
			return c.json({ status: "ok" });
		});

		// Default response
		app.get("/", (c) => {
			return c.json({
				message: "Big Daddy - Distributed SQL on Cloudflare",
				endpoints: {
					"/dash/:databaseId": "GET - View database topology dashboard",
					"/sql": "POST - Execute SQL query",
					"/health": "GET - Health check",
				},
			});
		});

		return app.fetch(request, this.env);
	}

	/**
	 * Queue handler for processing virtual index jobs
	 */
	override async queue(batch: MessageBatch<unknown>): Promise<void> {
		const correlationId = crypto.randomUUID();
		const source = "BigDaddy";
		const component = "BigDaddy";
		const operation = "queue";
		const batchSize = batch.messages.length;

		logger.info`Processing queue batch ${{ source }} ${{ component }} ${{ operation }} ${{ correlationId }} ${{ requestId: correlationId }} ${{ batchSize }}`;

		try {
			await queueHandler(
				batch as MessageBatch<IndexJob>,
				this.env,
				correlationId,
			);
			const status = "success";
			logger.info`Queue batch processed successfully ${{ source }} ${{ component }} ${{ batchSize }} ${{ status }}`;
		} catch (error) {
			const status = "failure";
			const errorMsg = error instanceof Error ? error.message : String(error);
			logger.error`Queue batch processing failed ${{ source }} ${{ component }} ${{ batchSize }} ${{ status }} ${{ error: errorMsg }}`;
			throw error;
		}
	}
}
