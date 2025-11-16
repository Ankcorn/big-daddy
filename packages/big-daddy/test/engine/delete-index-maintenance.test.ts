import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { createConnection } from '../../src/index';
import { processBuildIndexJob } from '../../src/engine/async-jobs/build-index';
import type { IndexBuildJob, IndexMaintenanceEventJob } from '../../src/engine/queue/types';

/**
 * DELETE with Index Maintenance Tests
 *
 * These tests verify that DELETE operations properly queue index maintenance events.
 *
 * Key insight: We capture index build jobs from the queue, process them immediately
 * to actually create the indexes in the topology, then verify that subsequent DELETEs
 * queue the correct index maintenance events with 'remove' operations.
 */

// Store all queue messages for inspection in tests
let capturedQueueMessages: any[] = [];

// Wrap the queue.send to capture calls
const originalQueueSend = env.INDEX_QUEUE.send.bind(env.INDEX_QUEUE);
env.INDEX_QUEUE.send = async (message: any) => {
	capturedQueueMessages.push(message);
	return originalQueueSend(message);
};

describe('DELETE with Index Maintenance', () => {
	beforeEach(() => {
		// Clear captured messages before each test
		capturedQueueMessages = [];
	});

	/**
	 * Helper: Process any queued BUILD_INDEX jobs to actually create the indexes
	 */
	async function processPendingIndexBuilds() {
		let processed = true;
		while (processed) {
			processed = false;
			for (let i = capturedQueueMessages.length - 1; i >= 0; i--) {
				const msg = capturedQueueMessages[i];
				if (msg.type === 'build_index' && !msg._processed) {
					// Mark as processed to avoid infinite loop
					msg._processed = true;
					await processBuildIndexJob(msg as IndexBuildJob, env);
					processed = true;
				}
			}
		}
	}

	it('should queue index maintenance events when deleting rows with indexes', async () => {
		const dbId = 'test-delete-with-index';
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
		await sql`INSERT INTO users (id, email, name) VALUES (1, ${'alice@example.com'}, ${'Alice'})`;
		await sql`INSERT INTO users (id, email, name) VALUES (2, ${'bob@example.com'}, ${'Bob'})`;
		await sql`INSERT INTO users (id, email, name) VALUES (3, ${'charlie@example.com'}, ${'Charlie'})`;

		// Clear captured messages from INSERT
		capturedQueueMessages = [];

		// Now delete rows
		await sql`DELETE FROM users WHERE id = 1`;
		await sql`DELETE FROM users WHERE id = 2`;
		await sql`DELETE FROM users WHERE id = 3`;

		// Filter for maintain_index_events messages (not build_index)
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		// Should have queued index maintenance events
		expect(indexEvents.length).toBeGreaterThanOrEqual(1);

		// Collect all events from all jobs
		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		// Should have at least 3 events for 3 deleted rows
		expect(allEvents.length).toBeGreaterThanOrEqual(3);

		// All should be 'remove' operations
		allEvents.forEach((event: any) => {
			expect(event.operation).toBe('remove');
			expect(event.index_name).toBe('idx_email');
		});

		// Verify we have events for all deleted emails
		const keyValues = allEvents.map((e: any) => e.key_value);
		expect(keyValues).toContain('alice@example.com');
		expect(keyValues).toContain('bob@example.com');
		expect(keyValues).toContain('charlie@example.com');
	});

	it('should handle NULL values correctly (not index them) during DELETE', async () => {
		const dbId = 'test-delete-nulls';
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
		await sql`INSERT INTO records (id, email) VALUES (1, ${'user1@example.com'})`;
		await sql`INSERT INTO records (id, email) VALUES (2, ${null})`;
		await sql`INSERT INTO records (id, email) VALUES (3, ${'user3@example.com'})`;
		await sql`INSERT INTO records (id, email) VALUES (4, ${null})`;

		// Clear captured messages from INSERT
		capturedQueueMessages = [];

		// Delete all rows
		await sql`DELETE FROM records`;

		// Collect maintain_index_events
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		// Should have exactly 2 events (for user1 and user3, not for NULLs)
		expect(allEvents.length).toBeLessThanOrEqual(4); // At most 2 rows * 2 shards
		expect(allEvents.length).toBeGreaterThanOrEqual(2); // At least 2 non-NULL rows

		// Verify no NULL values in events
		const keyValues = allEvents.map((e: any) => e.key_value);
		expect(keyValues).not.toContain(null);
		expect(keyValues).not.toContain('null');
		expect(keyValues).toContain('user1@example.com');
		expect(keyValues).toContain('user3@example.com');
	});

	it('should handle multiple indexes on same table during DELETE', async () => {
		const dbId = 'test-delete-multi-index';
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
		await sql`INSERT INTO users (id, email, username) VALUES (1, ${'alice@example.com'}, ${'alice'})`;
		await sql`INSERT INTO users (id, email, username) VALUES (2, ${'bob@example.com'}, ${'bob'})`;

		// Clear captured messages from INSERT
		capturedQueueMessages = [];

		// Delete rows
		await sql`DELETE FROM users WHERE id = 1`;
		await sql`DELETE FROM users WHERE id = 2`;

		// Collect maintain_index_events
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		// Should have events for both indexes
		const emailEvents = allEvents.filter((e: any) => e.index_name === 'idx_email');
		const usernameEvents = allEvents.filter((e: any) => e.index_name === 'idx_username');

		// At least 2 events per index (one per deleted row)
		expect(emailEvents.length).toBeGreaterThanOrEqual(2);
		expect(usernameEvents.length).toBeGreaterThanOrEqual(2);

		// Verify values
		const emailValues = emailEvents.map((e: any) => e.key_value);
		const usernameValues = usernameEvents.map((e: any) => e.key_value);

		expect(emailValues).toContain('alice@example.com');
		expect(emailValues).toContain('bob@example.com');
		expect(usernameValues).toContain('alice');
		expect(usernameValues).toContain('bob');

		// All should be 'remove' operations
		allEvents.forEach((event: any) => {
			expect(event.operation).toBe('remove');
		});
	});
});
