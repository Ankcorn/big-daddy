import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { createConnection } from '../../src/index';
import { processBuildIndexJob } from '../../src/engine/async-jobs/build-index';
import type { IndexBuildJob, IndexMaintenanceEventJob } from '../../src/engine/queue/types';

/**
 * UPDATE with Index Maintenance Tests
 *
 * These tests verify that UPDATE operations properly queue index maintenance events.
 *
 * Key insight: We use SELECT + UPDATE + SELECT batch to capture old and new values,
 * then queue the difference (removals for old values no longer present, additions for new values).
 */

// Store all queue messages for inspection in tests
let capturedQueueMessages: any[] = [];

// Wrap the queue.send to capture calls
const originalQueueSend = env.INDEX_QUEUE.send.bind(env.INDEX_QUEUE);
env.INDEX_QUEUE.send = async (message: any) => {
	capturedQueueMessages.push(message);
	return originalQueueSend(message);
};

describe('UPDATE with Index Maintenance', () => {
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

	it('should queue index maintenance events when updating indexed columns', async () => {
		const dbId = 'test-update-with-index';
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

		// Now update rows, changing the indexed email column
		await sql`UPDATE users SET email = ${'alice.newemail@example.com'} WHERE id = 1`;
		await sql`UPDATE users SET email = ${'bob.newemail@example.com'} WHERE id = 2`;
		// Row 3 (charlie) is not updated

		// Filter for maintain_index_events messages (not build_index)
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		// Should have queued index maintenance events
		expect(indexEvents.length).toBeGreaterThanOrEqual(1);

		// Collect all events from all jobs
		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		// Should have events for updates: removals of old emails + additions of new emails
		expect(allEvents.length).toBeGreaterThanOrEqual(4); // At least 2 removes + 2 adds

		// Verify we have both 'remove' and 'add' operations
		const removeEvents = allEvents.filter((e: any) => e.operation === 'remove');
		const addEvents = allEvents.filter((e: any) => e.operation === 'add');

		expect(removeEvents.length).toBeGreaterThanOrEqual(2); // Old alice and bob emails removed
		expect(addEvents.length).toBeGreaterThanOrEqual(2); // New alice and bob emails added

		// Verify specific old values are removed
		const removedValues = removeEvents.map((e: any) => e.key_value);
		expect(removedValues).toContain('alice@example.com');
		expect(removedValues).toContain('bob@example.com');

		// Verify specific new values are added
		const addedValues = addEvents.map((e: any) => e.key_value);
		expect(addedValues).toContain('alice.newemail@example.com');
		expect(addedValues).toContain('bob.newemail@example.com');

		// Verify all events are for the correct index
		allEvents.forEach((event: any) => {
			expect(event.index_name).toBe('idx_email');
		});
	});

	it('should handle duplicate indexed values correctly during UPDATE', async () => {
		const dbId = 'test-update-duplicates';
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

		// Insert rows with duplicate email
		await sql`INSERT INTO records (id, email) VALUES (1, ${'shared@example.com'})`;
		await sql`INSERT INTO records (id, email) VALUES (2, ${'shared@example.com'})`;
		await sql`INSERT INTO records (id, email) VALUES (3, ${'unique@example.com'})`;

		// Clear captured messages from INSERT
		capturedQueueMessages = [];

		// Update row 1 to a different email
		// - shared@example.com should NOT be removed (row 2 still has it)
		// - unique-new@example.com should be added
		await sql`UPDATE records SET email = ${'unique-new@example.com'} WHERE id = 1`;

		// Collect maintain_index_events
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		// Should have add event for new unique-new email
		// Should have remove event for shared@example.com (even though row 2 still has it)
		// The index handler will use idempotent DELETE which won't actually remove it
		// because row 2 still references it. Deduplication happens at the index layer.
		const removeEvents = allEvents.filter((e: any) => e.operation === 'remove');
		const addEvents = allEvents.filter((e: any) => e.operation === 'add');

		expect(addEvents.length).toBeGreaterThanOrEqual(1);
		const addedValues = addEvents.map((e: any) => e.key_value);
		expect(addedValues).toContain('unique-new@example.com');

		// Verify we generate remove event for shared@example.com
		// (the index handler's idempotent operations prevent actual removal)
		const removedValues = removeEvents.map((e: any) => e.key_value);
		expect(removedValues).toContain('shared@example.com');
	});

	it('should handle NULL values correctly (not index them) during UPDATE', async () => {
		const dbId = 'test-update-nulls';
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
		await sql`INSERT INTO records (id, email) VALUES (1, ${'user1@example.com'})`;
		await sql`INSERT INTO records (id, email) VALUES (2, ${null})`;
		await sql`INSERT INTO records (id, email) VALUES (3, ${'user3@example.com'})`;

		// Clear captured messages from INSERT
		capturedQueueMessages = [];

		// Update row 1 to NULL (should remove from index)
		// Update row 2 from NULL to email (should add to index)
		await sql`UPDATE records SET email = ${null} WHERE id = 1`;
		await sql`UPDATE records SET email = ${'user2@example.com'} WHERE id = 2`;

		// Collect maintain_index_events
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		const removeEvents = allEvents.filter((e: any) => e.operation === 'remove');
		const addEvents = allEvents.filter((e: any) => e.operation === 'add');

		// Should have 1 remove (user1@example.com - changed to NULL)
		expect(removeEvents.length).toBeGreaterThanOrEqual(1);
		const removedValues = removeEvents.map((e: any) => e.key_value);
		expect(removedValues).toContain('user1@example.com');

		// Should have 1 add (user2@example.com - changed from NULL)
		expect(addEvents.length).toBeGreaterThanOrEqual(1);
		const addedValues = addEvents.map((e: any) => e.key_value);
		expect(addedValues).toContain('user2@example.com');

		// Should NOT have NULL in any events
		allEvents.forEach((event: any) => {
			expect(event.key_value).not.toBeNull();
		});
	});

	it('should handle composite indexes during UPDATE', async () => {
		const dbId = 'test-update-composite';
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
		await sql`INSERT INTO users (id, first_name, last_name) VALUES (1, ${'Alice'}, ${'Smith'})`;
		await sql`INSERT INTO users (id, first_name, last_name) VALUES (2, ${'Bob'}, ${'Jones'})`;

		// Clear captured messages from INSERT
		capturedQueueMessages = [];

		// Update first_name (affects composite index)
		await sql`UPDATE users SET first_name = ${'Alicia'} WHERE id = 1`;

		// Collect maintain_index_events
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		const removeEvents = allEvents.filter((e: any) => e.operation === 'remove');
		const addEvents = allEvents.filter((e: any) => e.operation === 'add');

		// Should have remove for old composite key ["Alice", "Smith"]
		expect(removeEvents.length).toBeGreaterThanOrEqual(1);
		// Should have add for new composite key ["Alicia", "Smith"]
		expect(addEvents.length).toBeGreaterThanOrEqual(1);
	});
});
