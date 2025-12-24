/**
 * Benchmark Router
 *
 * Routes between different benchmark workers:
 * - /blog/* -> Blog benchmark (new, uses createConnection API)
 * - /* -> Legacy benchmark (uses BIG_DADDY service binding RPC)
 */

import BlogBenchmarkWorker, { SeedWorkflow } from "./blog-benchmark";

// Re-export the workflow for wrangler to find
export { SeedWorkflow };

export default {
	async fetch(
		request: Request,
		env: Env,
		ctx: ExecutionContext,
	): Promise<Response> {
		const url = new URL(request.url);

		// Route to blog benchmark if path starts with /blog
		if (url.pathname.startsWith("/blog")) {
			// Remove /blog prefix for the blog benchmark handler
			const blogUrl = new URL(request.url);
			blogUrl.pathname = blogUrl.pathname.replace(/^\/blog/, "") || "/";

			const blogRequest = new Request(blogUrl, request);
			const blogWorker = new BlogBenchmarkWorker(ctx, env);
			return await blogWorker.fetch(blogRequest);
		}

		// Legacy benchmark - import dynamically to avoid loading when not needed
		const { default: legacyHandler } = await import("./legacy-benchmark");
		return await legacyHandler.fetch(request, env);
	},
};
