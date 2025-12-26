/**
 * Benchmark Worker
 *
 * Exposes the blog benchmark for load testing Big Daddy.
 */

import BlogBenchmarkWorker, { SeedWorkflow } from "./blog-benchmark";

// Re-export the workflow for wrangler to find
export { SeedWorkflow };

interface BenchmarkEnv {
	BIG_DADDY: {
		createConnection(
			databaseId: string,
			config: { nodes: number; correlationId?: string },
		): Promise<unknown>;
	};
	SEED_WORKFLOW: Workflow;
	AI?: Ai;
}

export default {
	async fetch(
		request: Request,
		env: BenchmarkEnv,
		ctx: ExecutionContext,
	): Promise<Response> {
		const blogWorker = new BlogBenchmarkWorker(ctx, env);
		return await blogWorker.fetch(request);
	},
};
