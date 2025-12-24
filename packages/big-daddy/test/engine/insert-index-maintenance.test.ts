import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { processBuildIndexJob } from "../../src/engine/async-jobs/build-index";
import type { IndexBuildJob, IndexJob } from "../../src/engine/queue/types";
import type { VirtualIndexEntry } from "../../src/engine/topology/types";
import { createConnection } from "../../src/index";

/**
 * INSERT with Index Maintenance Tests
 *
 * These tests verify that INSERT operations properly maintain index entries synchronously.
 *
 * Key insight: Index maintenance is now synchronous - when an INSERT executes,
 * the index entries are updated immediately in the Topology DO.
 */

// Store all queue messages for inspection in tests
let capturedQueueMessages: (IndexJob & { _processed?: boolean })[] = [];

// Save original queue.send to restore later
const originalQueueSend = env.INDEX_QUEUE.send.bind(env.INDEX_QUEUE);

describe("INSERT with Index Maintenance", () => {
	beforeEach(() => {
		// Clear captured messages before each test
		capturedQueueMessages = [];
		// Intercept queue.send to capture calls WITHOUT calling original
		// This prevents async background processing that breaks test isolation
		env.INDEX_QUEUE.send = async (message: IndexJob) => {
			capturedQueueMessages.push(message);
			// Don't call original - we'll process manually to avoid async race conditions
		};
	});

	afterEach(() => {
		// Restore original queue.send to prevent hanging
		env.INDEX_QUEUE.send = originalQueueSend;
	});

	/**
	 * Helper: Process any queued BUILD_INDEX jobs to actually create the indexes
	 */
	async function processPendingIndexBuilds() {
		let processed = true;
		while (processed) {
			processed = false;
			for (let i = capturedQueueMessages.length - 1; i >= 0; i--) {
				const msg = capturedQueueMessages[i]!;
				if (msg.type === "build_index" && !msg._processed) {
					// Mark as processed to avoid infinite loop
					msg._processed = true;
					await processBuildIndexJob(msg as IndexBuildJob, env);
					processed = true;
				}
			}
		}
	}

	/**
	 * Helper: Get index entries from topology
	 */
	async function getIndexEntries(dbId: string) {
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();
		return topology.virtual_index_entries;
	}

	it("should maintain index entries synchronously when inserting rows", async () => {
		const dbId = "test-insert-with-index";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table with indexed column
		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			name TEXT NOT NULL
		)`;

		// Create index on email - this will queue a BUILD_INDEX job
		await sql`CREATE INDEX idx_email ON users (email)`;

		// Process the BUILD_INDEX job to actually create the index
		await processPendingIndexBuilds();

		// Now insert rows - index entries should be created synchronously
		await sql`INSERT INTO users (id, email, name) VALUES (1, ${"alice@example.com"}, ${"Alice"})`;
		await sql`INSERT INTO users (id, email, name) VALUES (2, ${"bob@example.com"}, ${"Bob"})`;
		await sql`INSERT INTO users (id, email, name) VALUES (3, ${"charlie@example.com"}, ${"Charlie"})`;

		// Verify index entries were created in the topology
		const entries = await getIndexEntries(dbId);

		// Should have entries for all 3 emails
		expect(entries.length).toBe(3);

		const keyValues = entries.map((e: VirtualIndexEntry) => e.key_value);
		expect(keyValues).toContain("alice@example.com");
		expect(keyValues).toContain("bob@example.com");
		expect(keyValues).toContain("charlie@example.com");

		// All entries should point to shard 0 (single shard before resharding)
		entries.forEach((entry: VirtualIndexEntry) => {
			const shardIds = JSON.parse(entry.shard_ids);
			expect(shardIds).toContain(0);
		});
	});

	it("should deduplicate index entries for duplicate values", async () => {
		const dbId = "test-insert-dedup";
		const sql = await createConnection(dbId, { nodes: 1 }, env); // Single shard

		// Create table
		await sql`CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			status TEXT NOT NULL
		)`;

		// Create index on status
		await sql`CREATE INDEX idx_status ON orders (status)`;

		// Process the BUILD_INDEX job
		await processPendingIndexBuilds();

		// Insert multiple rows with the same status value
		await sql`INSERT INTO orders (id, status) VALUES (1, ${"pending"})`;
		await sql`INSERT INTO orders (id, status) VALUES (2, ${"pending"})`;
		await sql`INSERT INTO orders (id, status) VALUES (3, ${"pending"})`;
		await sql`INSERT INTO orders (id, status) VALUES (4, ${"completed"})`;

		// Verify index entries - should have only 2 unique entries (pending, completed)
		const entries = await getIndexEntries(dbId);

		expect(entries.length).toBe(2);

		const keyValues = entries.map((e: VirtualIndexEntry) => e.key_value);
		expect(keyValues).toContain("pending");
		expect(keyValues).toContain("completed");
	});

	it("should handle NULL values correctly (not index them)", async () => {
		const dbId = "test-insert-nulls";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE records (
			id INTEGER PRIMARY KEY,
			email TEXT
		)`;

		// Create index on email (nullable column)
		await sql`CREATE INDEX idx_email ON records (email)`;

		// Process the BUILD_INDEX job
		await processPendingIndexBuilds();

		// Insert some rows with NULLs
		await sql`INSERT INTO records (id, email) VALUES (1, ${"user1@example.com"})`;
		await sql`INSERT INTO records (id, email) VALUES (2, ${null})`;
		await sql`INSERT INTO records (id, email) VALUES (3, ${"user3@example.com"})`;
		await sql`INSERT INTO records (id, email) VALUES (4, ${null})`;

		// Verify index entries - should only have entries for non-NULL values
		const entries = await getIndexEntries(dbId);

		expect(entries.length).toBe(2);

		const keyValues = entries.map((e: VirtualIndexEntry) => e.key_value);
		expect(keyValues).toContain("user1@example.com");
		expect(keyValues).toContain("user3@example.com");
		expect(keyValues).not.toContain(null);
		expect(keyValues).not.toContain("null");
	});

	it("should handle multiple indexes on same table", async () => {
		const dbId = "test-insert-multi-index";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			username TEXT NOT NULL
		)`;

		// Create two indexes
		await sql`CREATE INDEX idx_email ON users (email)`;
		await sql`CREATE INDEX idx_username ON users (username)`;

		// Process all BUILD_INDEX jobs
		await processPendingIndexBuilds();

		// Insert rows
		await sql`INSERT INTO users (id, email, username) VALUES (1, ${"alice@example.com"}, ${"alice"})`;
		await sql`INSERT INTO users (id, email, username) VALUES (2, ${"bob@example.com"}, ${"bob"})`;

		// Verify index entries for both indexes
		const entries = await getIndexEntries(dbId);

		// Should have 4 entries total (2 emails + 2 usernames)
		expect(entries.length).toBe(4);

		const emailEntries = entries.filter(
			(e: VirtualIndexEntry) => e.index_name === "idx_email",
		);
		const usernameEntries = entries.filter(
			(e: VirtualIndexEntry) => e.index_name === "idx_username",
		);

		expect(emailEntries.length).toBe(2);
		expect(usernameEntries.length).toBe(2);

		const emailValues = emailEntries.map((e: VirtualIndexEntry) => e.key_value);
		const usernameValues = usernameEntries.map(
			(e: VirtualIndexEntry) => e.key_value,
		);

		expect(emailValues).toContain("alice@example.com");
		expect(emailValues).toContain("bob@example.com");
		expect(usernameValues).toContain("alice");
		expect(usernameValues).toContain("bob");
	});
});
