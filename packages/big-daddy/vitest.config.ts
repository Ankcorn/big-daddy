import { defineWorkersConfig } from "@cloudflare/vitest-pool-workers/config";

export default defineWorkersConfig({
	test: {
		poolOptions: {
			workers: {
				wrangler: {
					configPath: "./wrangler.test.jsonc", // Use test config without remote AI binding
				},
				isolatedStorage: false,
				singleWorker: true, // Required for queue consumer testing - prevents ERR_MULTIPLE_CONSUMERS
			},
		},
	},
});
