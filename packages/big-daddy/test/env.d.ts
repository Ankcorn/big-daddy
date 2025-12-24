declare module "cloudflare:test" {
	// ProvidedEnv controls the type of `import("cloudflare:test").env`
	interface ProvidedEnv {
		STORAGE: DurableObjectNamespace<import("../src/engine/storage").Storage>;
		TOPOLOGY: DurableObjectNamespace<
			import("../src/engine/topology/index").Topology
		>;
		INDEX_QUEUE: Queue;
		SEED_WORKFLOW: Workflow;
		BIG_DADDY: Service;
		AI: Ai;
	}
}
