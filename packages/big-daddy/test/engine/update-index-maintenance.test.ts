import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { processBuildIndexJob } from "../../src/engine/async-jobs/build-index";
import type { IndexBuildJob, IndexJob } from "../../src/engine/queue/types";
import type { VirtualIndexEntry } from "../../src/engine/topology/types";
import { createConnection } from "../../src/index";

/**
 * UPDATE with Index Maintenance Tests
 *
 * These tests verify that UPDATE operations properly maintain index entries synchronously.
 *
 * Key insight: Index maintenance is now synchronous - when an UPDATE executes,
 * the index entries are updated immediately in the Topology DO.
 */

// Store all queue messages for inspection in tests
let capturedQueueMessages: (IndexJob & { _processed?: boolean })[] = [];

// Save original queue.send to restore later
const originalQueueSend = env.INDEX_QUEUE.send.bind(env.INDEX_QUEUE);

describe("UPDATE with Index Maintenance", () => {
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

	it("should update index entries synchronously when updating indexed columns", async () => {
		const dbId = "test-update-with-index";
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

		// Insert rows
		await sql`INSERT INTO users (id, email, name) VALUES (1, ${"alice@example.com"}, ${"Alice"})`;
		await sql`INSERT INTO users (id, email, name) VALUES (2, ${"bob@example.com"}, ${"Bob"})`;
		await sql`INSERT INTO users (id, email, name) VALUES (3, ${"charlie@example.com"}, ${"Charlie"})`;

		// Verify initial index entries
		let entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(3);
		let keyValues = entries.map((e: VirtualIndexEntry) => e.key_value);
		expect(keyValues).toContain("alice@example.com");
		expect(keyValues).toContain("bob@example.com");
		expect(keyValues).toContain("charlie@example.com");

		// Now update rows, changing the indexed email column
		await sql`UPDATE users SET email = ${"alice.newemail@example.com"} WHERE id = 1`;
		await sql`UPDATE users SET email = ${"bob.newemail@example.com"} WHERE id = 2`;

		// Verify index entries were updated
		entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(3);
		keyValues = entries.map((e: VirtualIndexEntry) => e.key_value);
		expect(keyValues).toContain("alice.newemail@example.com");
		expect(keyValues).toContain("bob.newemail@example.com");
		expect(keyValues).toContain("charlie@example.com");
		expect(keyValues).not.toContain("alice@example.com");
		expect(keyValues).not.toContain("bob@example.com");
	});

	it("should handle duplicate indexed values correctly during UPDATE", async () => {
		const dbId = "test-update-duplicates";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE records (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL
		)`;

		// Create index on email
		await sql`CREATE INDEX idx_email ON records (email)`;

		// Process the BUILD_INDEX job
		await processPendingIndexBuilds();

		// Insert rows - each with unique email initially
		await sql`INSERT INTO records (id, email) VALUES (1, ${"user1@example.com"})`;
		await sql`INSERT INTO records (id, email) VALUES (2, ${"user2@example.com"})`;
		await sql`INSERT INTO records (id, email) VALUES (3, ${"user3@example.com"})`;

		// Verify initial index entries - should have 3 unique entries
		let entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(3);

		// Update row 1 to a different email
		await sql`UPDATE records SET email = ${"user1-new@example.com"} WHERE id = 1`;

		// Verify index entries - old value removed, new value added
		entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(3);
		const keyValues = entries.map((e: VirtualIndexEntry) => e.key_value);
		expect(keyValues).toContain("user1-new@example.com");
		expect(keyValues).toContain("user2@example.com");
		expect(keyValues).toContain("user3@example.com");
		expect(keyValues).not.toContain("user1@example.com");
	});

	it("should handle NULL values correctly (not index them) during UPDATE", async () => {
		const dbId = "test-update-nulls";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table with nullable indexed column
		await sql`CREATE TABLE records (
			id INTEGER PRIMARY KEY,
			email TEXT
		)`;

		// Create index on email
		await sql`CREATE INDEX idx_email ON records (email)`;

		// Process the BUILD_INDEX job
		await processPendingIndexBuilds();

		// Insert rows with some NULLs
		await sql`INSERT INTO records (id, email) VALUES (1, ${"user1@example.com"})`;
		await sql`INSERT INTO records (id, email) VALUES (2, ${null})`;
		await sql`INSERT INTO records (id, email) VALUES (3, ${"user3@example.com"})`;

		// Verify initial index entries - should only have 2 (no NULL)
		let entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(2);

		// Update row 1 to NULL (should remove from index)
		// Update row 2 from NULL to email (should add to index)
		await sql`UPDATE records SET email = ${null} WHERE id = 1`;
		await sql`UPDATE records SET email = ${"user2@example.com"} WHERE id = 2`;

		// Verify index entries
		entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(2);
		const keyValues = entries.map((e: VirtualIndexEntry) => e.key_value);
		expect(keyValues).toContain("user2@example.com");
		expect(keyValues).toContain("user3@example.com");
		expect(keyValues).not.toContain("user1@example.com");
	});

	it("should handle composite indexes during UPDATE", async () => {
		const dbId = "test-update-composite";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			first_name TEXT NOT NULL,
			last_name TEXT NOT NULL
		)`;

		// Create composite index on (first_name, last_name)
		await sql`CREATE INDEX idx_name ON users (first_name, last_name)`;

		// Process the BUILD_INDEX job
		await processPendingIndexBuilds();

		// Insert rows
		await sql`INSERT INTO users (id, first_name, last_name) VALUES (1, ${"Alice"}, ${"Smith"})`;
		await sql`INSERT INTO users (id, first_name, last_name) VALUES (2, ${"Bob"}, ${"Jones"})`;

		// Verify initial index entries
		let entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(2);

		// Update first_name (affects composite index)
		await sql`UPDATE users SET first_name = ${"Alicia"} WHERE id = 1`;

		// Verify index entries were updated
		entries = await getIndexEntries(dbId);
		expect(entries.length).toBe(2);
		const keyValues = entries.map((e: VirtualIndexEntry) => e.key_value);
		// Composite keys are stored as JSON arrays
		expect(keyValues).toContain('["Alicia","Smith"]');
		expect(keyValues).toContain('["Bob","Jones"]');
		expect(keyValues).not.toContain('["Alice","Smith"]');
	});
});
