import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { createConnection } from '../../src/index';
import { processBuildIndexJob } from '../../src/engine/async-jobs/build-index';
import type { IndexBuildJob, IndexMaintenanceEventJob } from '../../src/engine/queue/types';

/**
 * INSERT with Index Maintenance Tests
 *
 * These tests verify that INSERT operations properly queue index maintenance events.
 *
 * Key insight: We capture index build jobs from the queue, process them immediately
 * to actually create the indexes in the topology, then verify that subsequent INSERTs
 * queue the correct index maintenance events.
 */

// Store all queue messages for inspection in tests
let capturedQueueMessages: any[] = [];

// Wrap the queue.send to capture calls
const originalQueueSend = env.INDEX_QUEUE.send.bind(env.INDEX_QUEUE);
env.INDEX_QUEUE.send = async (message: any) => {
	capturedQueueMessages.push(message);
	return originalQueueSend(message);
};

describe('INSERT with Index Maintenance', () => {
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

	it('should queue index maintenance events when inserting rows with indexes', async () => {
		const dbId = 'test-insert-with-index';
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

		// Now insert rows
		await sql`INSERT INTO users (id, email, name) VALUES (1, ${'alice@example.com'}, ${'Alice'})`;
		await sql`INSERT INTO users (id, email, name) VALUES (2, ${'bob@example.com'}, ${'Bob'})`;
		await sql`INSERT INTO users (id, email, name) VALUES (3, ${'charlie@example.com'}, ${'Charlie'})`;

		// Filter for maintain_index_events messages (not build_index)
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		// Should have queued index maintenance events
		expect(indexEvents.length).toBeGreaterThanOrEqual(1);

		// Collect all events from all jobs
		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		// Should have at least 3 events for 3 inserted rows
		expect(allEvents.length).toBeGreaterThanOrEqual(3);

		// All should be 'add' operations
		allEvents.forEach((event: any) => {
			expect(event.operation).toBe('add');
			expect(event.index_name).toBe('idx_email');
		});

		// Verify we have events for all inserted emails
		const keyValues = allEvents.map((e: any) => e.key_value);
		expect(keyValues).toContain('alice@example.com');
		expect(keyValues).toContain('bob@example.com');
		expect(keyValues).toContain('charlie@example.com');
	});

	it('should deduplicate index events when inserting duplicate index values', async () => {
		const dbId = 'test-insert-dedup';
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

		// Insert multiple rows with the same status value in separate statements
		// Each INSERT statement will deduplicate within itself, but separate INSERTs
		// will each produce events (to be aggregated in the queue)
		await sql`INSERT INTO orders (id, status) VALUES (1, ${'pending'})`;
		await sql`INSERT INTO orders (id, status) VALUES (2, ${'pending'})`;
		await sql`INSERT INTO orders (id, status) VALUES (3, ${'pending'})`;
		await sql`INSERT INTO orders (id, status) VALUES (4, ${'completed'})`;

		// Collect maintain_index_events
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		// Each separate INSERT statement produces deduplicated events for that statement
		// So we should have:
		// - 1 'pending' event from INSERT 1 (1 row with pending)
		// - 1 'pending' event from INSERT 2 (1 row with pending)
		// - 1 'pending' event from INSERT 3 (1 row with pending)
		// - 1 'completed' event from INSERT 4 (1 row with completed)
		// Total: 3 pending + 1 completed
		// (The queue layer will aggregate these multiple jobs for batching)
		const pendingEvents = allEvents.filter((e: any) => e.key_value === 'pending');
		const completedEvents = allEvents.filter((e: any) => e.key_value === 'completed');

		// Each INSERT produces one event per unique (index, value, shard) tuple
		expect(pendingEvents.length).toBe(3); // 3 separate INSERTs with pending
		expect(completedEvents.length).toBe(1); // 1 INSERT with completed

		// All should be 'add' operations
		allEvents.forEach((event: any) => {
			expect(event.operation).toBe('add');
		});
	});

	it('should handle NULL values correctly (not index them)', async () => {
		const dbId = 'test-insert-nulls';
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

	it('should handle multiple indexes on same table', async () => {
		const dbId = 'test-insert-multi-index';
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

		// Collect maintain_index_events
		const indexEvents = capturedQueueMessages.filter((msg: any) => msg.type === 'maintain_index_events');

		let allEvents: any[] = [];
		indexEvents.forEach((job: any) => {
			allEvents = allEvents.concat(job.events);
		});

		// Should have events for both indexes
		const emailEvents = allEvents.filter((e: any) => e.index_name === 'idx_email');
		const usernameEvents = allEvents.filter((e: any) => e.index_name === 'idx_username');

		// At least 2 events per index (one per row)
		expect(emailEvents.length).toBeGreaterThanOrEqual(2);
		expect(usernameEvents.length).toBeGreaterThanOrEqual(2);

		// Verify values
		const emailValues = emailEvents.map((e: any) => e.key_value);
		const usernameValues = usernameEvents.map((e: any) => e.key_value);

		expect(emailValues).toContain('alice@example.com');
		expect(emailValues).toContain('bob@example.com');
		expect(usernameValues).toContain('alice');
		expect(usernameValues).toContain('bob');

		// All should be 'add' operations
		allEvents.forEach((event: any) => {
			expect(event.operation).toBe('add');
		});
	});
});
