import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { IndexBuildJob, MessageBatch } from "../../src/engine/queue/types";
import type { VirtualIndexEntry } from "../../src/engine/topology/types";
import { createConductor } from "../../src/index";
import { queueHandler } from "../../src/queue-consumer";

/**
 * Build Index Tests
 *
 * These tests verify the build-index.ts functionality by:
 * 1. Setting up a small DB via the conductor method
 * 2. Inserting test data (10 rows)
 * 3. Manually triggering the queue job
 * 4. Verifying the topology has the new index
 *
 * Tests use public interfaces for maximum confidence.
 */

/**
 * Helper: Initialize topology with specified number of nodes
 */
async function initializeTopology(dbId: string, numNodes: number) {
	const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
	await topologyStub.create(numNodes);
}

/**
 * Helper: Setup a small database with a table and 10 rows of test data
 */
async function setupSmallDatabase(dbId: string) {
	const conductor = createConductor(dbId, crypto.randomUUID(), env);

	// Create a simple users table
	await conductor.sql`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL,
		name TEXT NOT NULL,
		age INTEGER
	)`;

	// Insert 10 rows of test data
	for (let i = 1; i <= 10; i++) {
		await conductor.sql`INSERT INTO users (id, email, name, age) VALUES (
			${i},
			${`user${i}@example.com`},
			${`User ${i}`},
			${20 + i}
		)`;
	}

	return conductor;
}

/**
 * Helper: Manually trigger the build index queue job
 */
async function triggerBuildIndexJob(job: IndexBuildJob): Promise<void> {
	const batch: MessageBatch<IndexBuildJob> = {
		queue: "vitess-index-jobs",
		messages: [
			{
				id: `test-${Date.now()}-${Math.random()}`,
				timestamp: new Date(),
				body: job,
				attempts: 1,
			},
		],
	};

	await queueHandler(batch, env, job.correlation_id);
}

describe("Build Index", () => {
	it("should build index on table with 10 rows via public interfaces", async () => {
		const dbId = "test-build-index-1";
		await initializeTopology(dbId, 3);

		// Setup database with 10 rows
		const conductor = await setupSmallDatabase(dbId);

		// Create index (this creates it in topology with 'building' status)
		await conductor.sql`CREATE INDEX idx_email ON users(email)`;

		// Verify index was created in topology with 'building' status
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes).toHaveLength(1);
		expect(topology.virtual_indexes[0].index_name).toBe("idx_email");
		expect(topology.virtual_indexes[0].status).toBe("ready"); // Auto-built in test environment

		// Verify all 10 email values are indexed
		expect(topology.virtual_index_entries).toHaveLength(10);

		// Check a few specific entries
		const user1Entry = topology.virtual_index_entries.find(
			(e: VirtualIndexEntry) => e.key_value === "user1@example.com",
		);
		const user5Entry = topology.virtual_index_entries.find(
			(e: VirtualIndexEntry) => e.key_value === "user5@example.com",
		);
		const user10Entry = topology.virtual_index_entries.find(
			(e: VirtualIndexEntry) => e.key_value === "user10@example.com",
		);

		expect(user1Entry).toBeDefined();
		expect(user5Entry).toBeDefined();
		expect(user10Entry).toBeDefined();

		// Each email should be in exactly 1 shard (unique emails)
		expect(JSON.parse(user1Entry?.shard_ids)).toHaveLength(1);
		expect(JSON.parse(user5Entry?.shard_ids)).toHaveLength(1);
		expect(JSON.parse(user10Entry?.shard_ids)).toHaveLength(1);
	});

	it("should manually trigger build index job and verify topology update", async () => {
		const dbId = "test-build-index-manual";
		await initializeTopology(dbId, 2);

		const _conductor = await setupSmallDatabase(dbId);

		// Create index metadata in topology (without auto-building)
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex(
			"idx_name",
			"users",
			["name"],
			"hash",
		);

		// Verify index starts in 'building' status
		let topology = await topologyStub.getTopology();
		expect(topology.virtual_indexes[0].status).toBe("building");
		expect(topology.virtual_index_entries).toHaveLength(0);

		// Manually trigger the build index job
		const buildJob: IndexBuildJob = {
			type: "build_index",
			database_id: dbId,
			table_name: "users",
			columns: ["name"],
			index_name: "idx_name",
			created_at: new Date().toISOString(),
		};

		await triggerBuildIndexJob(buildJob);

		// Verify index is now 'ready' with all entries
		topology = await topologyStub.getTopology();
		expect(topology.virtual_indexes[0].status).toBe("ready");
		expect(topology.virtual_indexes[0].error_message).toBeNull();
		expect(topology.virtual_index_entries).toHaveLength(10);

		// Verify all names are indexed
		const names = topology.virtual_index_entries.map(
			(e: VirtualIndexEntry) => e.key_value,
		);
		expect(names).toContain("User 1");
		expect(names).toContain("User 5");
		expect(names).toContain("User 10");
	});

	it("should build composite index on multiple columns", async () => {
		const dbId = "test-build-index-composite";
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, crypto.randomUUID(), env);

		// Create table
		await conductor.sql`CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			category TEXT NOT NULL,
			subcategory TEXT NOT NULL,
			price REAL NOT NULL
		)`;

		// Insert 10 rows with different category/subcategory combinations
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (1, ${"Electronics"}, ${"Phones"}, ${699.99})`;
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (2, ${"Electronics"}, ${"Laptops"}, ${1299.99})`;
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (3, ${"Electronics"}, ${"Phones"}, ${899.99})`;
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (4, ${"Clothing"}, ${"Shirts"}, ${29.99})`;
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (5, ${"Clothing"}, ${"Pants"}, ${49.99})`;
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (6, ${"Electronics"}, ${"Tablets"}, ${499.99})`;
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (7, ${"Clothing"}, ${"Shirts"}, ${39.99})`;
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (8, ${"Home"}, ${"Furniture"}, ${299.99})`;
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (9, ${"Home"}, ${"Decor"}, ${59.99})`;
		await conductor.sql`INSERT INTO products (id, category, subcategory, price) VALUES (10, ${"Electronics"}, ${"Laptops"}, ${1499.99})`;

		// Create composite index on (category, subcategory)
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex(
			"idx_category_subcategory",
			"products",
			["category", "subcategory"],
			"hash",
		);

		// Manually trigger build job
		const buildJob: IndexBuildJob = {
			type: "build_index",
			database_id: dbId,
			table_name: "products",
			columns: ["category", "subcategory"],
			index_name: "idx_category_subcategory",
			created_at: new Date().toISOString(),
		};

		await triggerBuildIndexJob(buildJob);

		// Verify composite index entries
		const topology = await topologyStub.getTopology();
		expect(topology.virtual_indexes[0].status).toBe("ready");

		// Should have 6 unique combinations:
		// ["Electronics","Phones"], ["Electronics","Laptops"], ["Electronics","Tablets"],
		// ["Clothing","Shirts"], ["Clothing","Pants"], ["Home","Furniture"], ["Home","Decor"]
		expect(topology.virtual_index_entries.length).toBe(7);

		// Verify composite key format (JSON array)
		const electronicsPhones = topology.virtual_index_entries.find(
			(e: VirtualIndexEntry) =>
				e.key_value === JSON.stringify(["Electronics", "Phones"]),
		);
		expect(electronicsPhones).toBeDefined();

		const clothingShirts = topology.virtual_index_entries.find(
			(e: VirtualIndexEntry) =>
				e.key_value === JSON.stringify(["Clothing", "Shirts"]),
		);
		expect(clothingShirts).toBeDefined();
	});

	it("should handle NULL values correctly when building index", async () => {
		const dbId = "test-build-index-nulls";
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, crypto.randomUUID(), env);

		await conductor.sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT,
			name TEXT NOT NULL
		)`;

		// Insert 10 rows with some NULL emails
		for (let i = 1; i <= 10; i++) {
			const email = i % 3 === 0 ? null : `user${i}@example.com`; // Every 3rd row has NULL email
			await conductor.sql`INSERT INTO users (id, email, name) VALUES (
				${i},
				${email},
				${`User ${i}`}
			)`;
		}

		// Build index on email column (which has NULLs)
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex(
			"idx_email",
			"users",
			["email"],
			"hash",
		);

		const buildJob: IndexBuildJob = {
			type: "build_index",
			database_id: dbId,
			table_name: "users",
			columns: ["email"],
			index_name: "idx_email",
			created_at: new Date().toISOString(),
		};

		await triggerBuildIndexJob(buildJob);

		// Verify NULL values are skipped (only 7 non-NULL emails indexed)
		const topology = await topologyStub.getTopology();
		expect(topology.virtual_indexes[0].status).toBe("ready");
		expect(topology.virtual_index_entries).toHaveLength(7); // 10 rows - 3 NULLs = 7

		// Verify no NULL entries
		const nullEntry = topology.virtual_index_entries.find(
			(e: VirtualIndexEntry) => e.key_value === "null",
		);
		expect(nullEntry).toBeUndefined();
	});

	it("should handle index build failure gracefully", async () => {
		const dbId = "test-build-index-failure";
		await initializeTopology(dbId, 2);

		const _conductor = await setupSmallDatabase(dbId);

		// Create index on non-existent column
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex(
			"idx_bad",
			"users",
			["nonexistent_column"],
			"hash",
		);

		// Try to build the index (should fail)
		const buildJob: IndexBuildJob = {
			type: "build_index",
			database_id: dbId,
			table_name: "users",
			columns: ["nonexistent_column"],
			index_name: "idx_bad",
			created_at: new Date().toISOString(),
		};

		// Queue handler logs errors but doesn't throw
		await triggerBuildIndexJob(buildJob);

		// Verify index status is 'failed' with error message
		const topology = await topologyStub.getTopology();
		expect(topology.virtual_indexes[0].status).toBe("failed");
		expect(topology.virtual_indexes[0].error_message).toBeTruthy();
		expect(topology.virtual_indexes[0].error_message).toContain(
			"nonexistent_column",
		);
	});

	it("should build index across multiple shards correctly", async () => {
		const dbId = "test-build-index-multishard";
		await initializeTopology(dbId, 5); // Use 5 shards

		const conductor = createConductor(dbId, crypto.randomUUID(), env);

		await conductor.sql`CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER NOT NULL,
			status TEXT NOT NULL
		)`;

		// Insert 10 orders that will be distributed across shards
		for (let i = 1; i <= 10; i++) {
			await conductor.sql`INSERT INTO orders (id, customer_id, status) VALUES (
				${i},
				${100 + i},
				${i % 2 === 0 ? "completed" : "pending"}
			)`;
		}

		// Build index on status
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex(
			"idx_status",
			"orders",
			["status"],
			"hash",
		);

		const buildJob: IndexBuildJob = {
			type: "build_index",
			database_id: dbId,
			table_name: "orders",
			columns: ["status"],
			index_name: "idx_status",
			created_at: new Date().toISOString(),
		};

		await triggerBuildIndexJob(buildJob);

		// Verify index was built across all shards
		const topology = await topologyStub.getTopology();
		expect(topology.virtual_indexes[0].status).toBe("ready");

		// Should have 2 unique status values: 'pending' and 'completed'
		expect(topology.virtual_index_entries).toHaveLength(2);

		const pendingEntry = topology.virtual_index_entries.find(
			(e: VirtualIndexEntry) => e.key_value === "pending",
		);
		const completedEntry = topology.virtual_index_entries.find(
			(e: VirtualIndexEntry) => e.key_value === "completed",
		);

		expect(pendingEntry).toBeDefined();
		expect(completedEntry).toBeDefined();

		// Each status value may be present on multiple shards
		const pendingShards = JSON.parse(pendingEntry?.shard_ids);
		const completedShards = JSON.parse(completedEntry?.shard_ids);

		// At least 1 shard should have each status
		expect(pendingShards.length).toBeGreaterThanOrEqual(1);
		expect(completedShards.length).toBeGreaterThanOrEqual(1);

		// Shards should be sorted
		expect(pendingShards).toEqual([...pendingShards].sort((a, b) => a - b));
		expect(completedShards).toEqual([...completedShards].sort((a, b) => a - b));
	});

	it("should build index on table with duplicate values", async () => {
		const dbId = "test-build-index-duplicates";
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, crypto.randomUUID(), env);

		await conductor.sql`CREATE TABLE logs (
			id INTEGER PRIMARY KEY,
			level TEXT NOT NULL,
			message TEXT NOT NULL
		)`;

		// Insert 10 logs with only 3 unique levels
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (1, ${"INFO"}, ${"Application started"})`;
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (2, ${"ERROR"}, ${"Failed to connect"})`;
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (3, ${"INFO"}, ${"User logged in"})`;
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (4, ${"WARN"}, ${"Deprecated API"})`;
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (5, ${"ERROR"}, ${"Database error"})`;
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (6, ${"INFO"}, ${"Request processed"})`;
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (7, ${"INFO"}, ${"Cache cleared"})`;
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (8, ${"ERROR"}, ${"Timeout occurred"})`;
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (9, ${"WARN"}, ${"High memory usage"})`;
		await conductor.sql`INSERT INTO logs (id, level, message) VALUES (10, ${"INFO"}, ${"Shutdown initiated"})`;

		// Build index on level
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex(
			"idx_level",
			"logs",
			["level"],
			"hash",
		);

		const buildJob: IndexBuildJob = {
			type: "build_index",
			database_id: dbId,
			table_name: "logs",
			columns: ["level"],
			index_name: "idx_level",
			created_at: new Date().toISOString(),
		};

		await triggerBuildIndexJob(buildJob);

		// Verify only 3 unique entries (INFO, ERROR, WARN)
		const topology = await topologyStub.getTopology();
		expect(topology.virtual_indexes[0].status).toBe("ready");
		expect(topology.virtual_index_entries).toHaveLength(3);

		const levels = topology.virtual_index_entries
			.map((e: VirtualIndexEntry) => e.key_value)
			.sort();
		expect(levels).toEqual(["ERROR", "INFO", "WARN"]);

		// INFO appears 5 times, ERROR 3 times, WARN 2 times
		// But each distinct value appears once in the index with shard mappings
		const infoEntry = topology.virtual_index_entries.find(
			(e: VirtualIndexEntry) => e.key_value === "INFO",
		);
		expect(infoEntry).toBeDefined();
		expect(JSON.parse(infoEntry?.shard_ids).length).toBeGreaterThanOrEqual(1);
	});
});
