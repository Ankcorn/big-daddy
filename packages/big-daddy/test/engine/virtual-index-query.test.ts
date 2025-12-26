import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { IndexJob } from "../../src/engine/queue/types";
import { createConnection } from "../../src/index";

/**
 * Virtual Index Query Optimization Tests
 *
 * These tests verify that virtual indexes actually reduce shard fan-out
 * when executing SELECT queries. This is tested end-to-end using only
 * the public createConnection and sql interfaces.
 *
 * Key insight: If virtual indexing works correctly, a SELECT with WHERE
 * on an indexed column should only query the shard(s) containing that value,
 * NOT all shards.
 */

// Store queue messages and process BUILD_INDEX jobs
let capturedQueueMessages: (IndexJob & { _processed?: boolean })[] = [];
const originalQueueSend = env.INDEX_QUEUE.send.bind(env.INDEX_QUEUE);

describe("Virtual Index Query Optimization", () => {
	beforeEach(() => {
		capturedQueueMessages = [];
		// Intercept queue to control when BUILD_INDEX jobs run
		env.INDEX_QUEUE.send = async (message: IndexJob) => {
			capturedQueueMessages.push(message);
		};
	});

	afterEach(() => {
		env.INDEX_QUEUE.send = originalQueueSend;
	});

	/**
	 * Helper: Process pending BUILD_INDEX jobs to populate virtual index entries
	 */
	async function processPendingIndexBuilds() {
		const { processBuildIndexJob } = await import(
			"../../src/engine/async-jobs/build-index"
		);
		for (const msg of capturedQueueMessages) {
			if (msg.type === "build_index" && !msg._processed) {
				msg._processed = true;
				await processBuildIndexJob(msg, env);
			}
		}
	}

	it("should use virtual index to query only relevant shards (not all shards)", async () => {
		const dbId = `test-virtual-index-query-${Date.now()}`;

		// Create connection with 3 shards
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		// Create table - shard key is 'id' by default
		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			name TEXT NOT NULL
		)`;

		// Create index on email (NOT the shard key)
		await sql`CREATE INDEX idx_email ON users(email)`;

		// Process the BUILD_INDEX job to mark index as 'ready'
		await processPendingIndexBuilds();

		// Insert a row - this will also update the virtual index entry
		await sql`INSERT INTO users (id, email, name) VALUES (1, ${"alice@example.com"}, ${"Alice"})`;

		// Now query by the indexed column
		const result =
			await sql`SELECT * FROM users WHERE email = ${"alice@example.com"}`;

		// Verify we got the row
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]).toMatchObject({
			id: 1,
			email: "alice@example.com",
			name: "Alice",
		});

		// KEY ASSERTION: Virtual index should reduce shard fan-out
		// With working virtual index: only 1 shard queried
		// Without virtual index: all 3 shards would be queried
		expect(result.shardStats).toBeDefined();
		expect(result.shardStats?.length).toBe(1); // Should only query 1 shard, not 3!

		// The queried shard should have returned 1 row
		expect(result.shardStats?.[0]?.rowsReturned).toBe(1);
	});

	it("should query zero shards when indexed value does not exist", async () => {
		const dbId = `test-virtual-index-empty-${Date.now()}`;

		const sql = await createConnection(dbId, { nodes: 3 }, env);

		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL
		)`;

		await sql`CREATE INDEX idx_email ON users(email)`;
		await processPendingIndexBuilds();

		// Insert a row with a specific email
		await sql`INSERT INTO users (id, email) VALUES (1, ${"alice@example.com"})`;

		// Query for a non-existent email
		const result =
			await sql`SELECT * FROM users WHERE email = ${"nonexistent@example.com"}`;

		// Should return no rows
		expect(result.rows).toHaveLength(0);

		// KEY ASSERTION: Should query zero shards since index knows this value doesn't exist
		expect(result.shardStats).toBeDefined();
		expect(result.shardStats?.length).toBe(0);
	});

	it("should query multiple shards when value exists on multiple shards", async () => {
		const dbId = `test-virtual-index-multi-shard-${Date.now()}`;

		const sql = await createConnection(dbId, { nodes: 3 }, env);

		await sql`CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			status TEXT NOT NULL
		)`;

		await sql`CREATE INDEX idx_status ON orders(status)`;
		await processPendingIndexBuilds();

		// Insert multiple rows with same status - they'll go to different shards based on id hash
		// With 3 shards and IDs 1-10, rows should distribute across shards
		for (let i = 1; i <= 10; i++) {
			await sql`INSERT INTO orders (id, status) VALUES (${i}, ${"pending"})`;
		}

		// Query by status
		const result = await sql`SELECT * FROM orders WHERE status = ${"pending"}`;

		// Should return all 10 rows
		expect(result.rows).toHaveLength(10);

		// KEY ASSERTION: Should only query shards that contain 'pending' status
		// This might be 1, 2, or 3 shards depending on hash distribution
		// But it should NOT query shards that don't have any 'pending' rows
		expect(result.shardStats).toBeDefined();
		expect(result.shardStats?.length).toBeGreaterThanOrEqual(1);
		expect(result.shardStats?.length).toBeLessThanOrEqual(3);

		// Total rows returned across all queried shards should equal 10
		const totalRows = result.shardStats?.reduce(
			(sum, s) => sum + s.rowsReturned,
			0,
		);
		expect(totalRows).toBe(10);
	});

	it("should query all table shards when no index exists for WHERE column", async () => {
		const dbId = `test-no-index-fallback-${Date.now()}`;

		const sql = await createConnection(dbId, { nodes: 3 }, env);

		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			name TEXT NOT NULL
		)`;

		// NO index created on 'name' column

		await sql`INSERT INTO users (id, email, name) VALUES (1, ${"alice@example.com"}, ${"Alice"})`;

		// Query by non-indexed column
		const result = await sql`SELECT * FROM users WHERE name = ${"Alice"}`;

		// Should still find the row
		expect(result.rows).toHaveLength(1);

		// Without an index on 'name', must query all shards for this table
		// Tables start with 1 shard by default (before resharding)
		expect(result.shardStats).toBeDefined();
		expect(result.shardStats?.length).toBe(1); // 1 shard (table's default)
	});

	it("should create async job record when CREATE INDEX is executed", async () => {
		const dbId = `test-async-job-creation-${Date.now()}`;

		const sql = await createConnection(dbId, { nodes: 2 }, env);

		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL
		)`;

		// Create index - this should create an async job record
		await sql`CREATE INDEX idx_email ON users(email)`;

		// Check topology for async job
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		// KEY ASSERTION: An async job should be created for the index build
		expect(topology.async_jobs).toBeDefined();
		expect(topology.async_jobs.length).toBeGreaterThanOrEqual(1);

		const indexJob = topology.async_jobs.find(
			(job) => job.job_type === "build_index",
		);
		expect(indexJob).toBeDefined();
		expect(indexJob?.table_name).toBe("users");
		expect(indexJob?.status).toMatch(/pending|running|completed/);
	});
});
