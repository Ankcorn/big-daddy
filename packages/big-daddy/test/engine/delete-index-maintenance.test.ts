import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { processBuildIndexJob } from "../../src/engine/async-jobs/build-index";
import type { IndexBuildJob, IndexJob } from "../../src/engine/queue/types";
import { createConnection } from "../../src/index";

/**
 * DELETE with Index Maintenance Tests
 *
 * These tests verify that DELETE operations properly maintain index entries synchronously.
 *
 * Key insight: Index maintenance is now synchronous - when a DELETE executes,
 * the index entries are updated immediately in the Topology DO.
 */

// Store all queue messages for inspection in tests
let capturedQueueMessages: (IndexJob & { _processed?: boolean })[] = [];

// Save original queue.send to restore later
const originalQueueSend = env.INDEX_QUEUE.send.bind(env.INDEX_QUEUE);

describe("DELETE with Index Maintenance", () => {
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

	it("should remove index entries synchronously when deleting rows", async () => {
		const dbId = "test-delete-with-index";
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

		// Insert rows - this creates index entries
		await sql`INSERT INTO users (id, email, name) VALUES (1, ${"alice@example.com"}, ${"Alice"})`;
		await sql`INSERT INTO users (id, email, name) VALUES (2, ${"bob@example.com"}, ${"Bob"})`;
		await sql`INSERT INTO users (id, email, name) VALUES (3, ${"charlie@example.com"}, ${"Charlie"})`;

		// Verify index entries exist
		let entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(3);

		// Now delete rows - index entries should be removed synchronously
		await sql`DELETE FROM users WHERE id = 1`;
		await sql`DELETE FROM users WHERE id = 2`;
		await sql`DELETE FROM users WHERE id = 3`;

		// Verify index entries were removed
		entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(0);
	});

	it("should handle NULL values correctly (not index them) during DELETE", async () => {
		const dbId = "test-delete-nulls";
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

		// Verify only non-NULL values are indexed
		let entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(2);

		// Delete all rows
		await sql`DELETE FROM records`;

		// Verify all index entries were removed
		entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(0);
	});

	it("should handle multiple indexes on same table during DELETE", async () => {
		const dbId = "test-delete-multi-index";
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

		// Verify index entries exist for both indexes
		let entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(4); // 2 emails + 2 usernames

		// Delete rows
		await sql`DELETE FROM users WHERE id = 1`;
		await sql`DELETE FROM users WHERE id = 2`;

		// Verify all index entries were removed
		entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(0);
	});
});
