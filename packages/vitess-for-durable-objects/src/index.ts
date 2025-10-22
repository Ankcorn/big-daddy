/**
 * Welcome to Cloudflare Workers! This is your first Durable Objects application.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your Durable Object in action
 * - Run `npm run deploy` to publish your application
 *
 * Bind resources to your worker in `wrangler.jsonc`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/durable-objects
 */

// Export Durable Objects
export { Storage } from "./Storage/Storage";
export { Topology } from "./Topology/Topology";

// Export Conductor
export { createConductor, ConductorClient } from "./Conductor/Conductor";
export type { QueryResult } from "./Conductor/Conductor";

// Import queue consumer
import { queueHandler } from "./queue-consumer";

export default {
	/**
	 * This is the standard fetch handler for a Cloudflare Worker
	 *
	 * @param request - The request submitted to the Worker from the client
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 * @param ctx - The execution context of the Worker
	 * @returns The response to be sent back to the client
	 */
	async fetch(request, env, ctx): Promise<Response> {
		// Example: Get topology information
		const topologyStub = env.TOPOLOGY.getByName("main");
		const topology = await topologyStub.getTopology();

		// Example: Execute a query on a storage node
		const storageStub = env.STORAGE.getByName("shard-0");
		const result = await storageStub.executeQuery({
			query: "SELECT 1 as test",
			queryType: "SELECT",
		});

		return new Response(JSON.stringify({ topology, result }, null, 2), {
			headers: { "Content-Type": "application/json" },
		});
	},

	/**
	 * Queue handler for processing virtual index jobs
	 */
	queue: queueHandler,
} satisfies ExportedHandler<Env>;
