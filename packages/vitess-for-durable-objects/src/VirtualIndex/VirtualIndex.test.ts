import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { createConductor } from '../Conductor/Conductor';
import { queueHandler } from '../queue-consumer';
import type { IndexBuildJob } from '../Queue/types';

/**
 * End-to-end tests for Virtual Index functionality
 *
 * These tests verify the complete flow:
 * 1. CREATE TABLE and INSERT data
 * 2. CREATE INDEX (enqueues build job)
 * 3. Queue consumer processes the job
 * 4. Index is built and ready to use
 */

async function initializeTopology(dbId: string, numNodes: number) {
	const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
	await topologyStub.create(numNodes);
}

describe('Virtual Index End-to-End', () => {
	it('should build an index from existing data', async () => {
		const dbId = 'test-index-e2e-1';
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, env);

		// 1. Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// 2. Insert test data - use IDs that will hash to different shards
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (1, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (2, ${'Bob'}, ${'bob@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (3, ${'Charlie'}, ${'charlie@example.com'})`;

		// 3. Create index (this enqueues a job)
		await conductor.sql`CREATE INDEX idx_email ON users(email)`;

		// Verify index was created with 'building' status
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		let topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes).toHaveLength(1);
		expect(topology.virtual_indexes[0].index_name).toBe('idx_email');
		expect(topology.virtual_indexes[0].status).toBe('building');
		expect(topology.virtual_index_entries).toHaveLength(0); // No entries yet

		// 4. Manually trigger queue consumer to process the build job
		// In production, this happens automatically, but in tests we need to trigger it
		const buildJob: IndexBuildJob = {
			type: 'build_index',
			database_id: dbId,
			table_name: 'users',
			column_name: 'email',
			index_name: 'idx_email',
			created_at: new Date().toISOString(),
		};

		await queueHandler(
			{
				queue: 'vitess-index-jobs',
				messages: [
					{
						id: 'test-msg-1',
						timestamp: new Date(),
						body: buildJob,
						attempts: 1,
					},
				],
			},
			env,
		);

		// 5. Verify index was built successfully
		topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes[0].status).toBe('ready');
		expect(topology.virtual_indexes[0].error_message).toBeNull();

		// Should have 3 unique email values indexed
		expect(topology.virtual_index_entries).toHaveLength(3);

		// Check specific entries
		const aliceEntry = topology.virtual_index_entries.find((e) => e.key_value === 'alice@example.com');
		const bobEntry = topology.virtual_index_entries.find((e) => e.key_value === 'bob@example.com');
		const charlieEntry = topology.virtual_index_entries.find((e) => e.key_value === 'charlie@example.com');

		expect(aliceEntry).toBeDefined();
		expect(bobEntry).toBeDefined();
		expect(charlieEntry).toBeDefined();

		// Each email should be in exactly 1 shard
		expect(JSON.parse(aliceEntry!.shard_ids)).toHaveLength(1);
		expect(JSON.parse(bobEntry!.shard_ids)).toHaveLength(1);
		expect(JSON.parse(charlieEntry!.shard_ids)).toHaveLength(1);
	});

	it('should handle building index on empty table', async () => {
		const dbId = 'test-index-e2e-2';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		// Create empty table
		await conductor.sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT)`;

		// Create index on empty table
		await conductor.sql`CREATE INDEX idx_category ON products(category)`;

		// Build the index
		const buildJob: IndexBuildJob = {
			type: 'build_index',
			database_id: dbId,
			table_name: 'products',
			column_name: 'category',
			index_name: 'idx_category',
			created_at: new Date().toISOString(),
		};

		await queueHandler(
			{
				queue: 'vitess-index-jobs',
				messages: [{ id: 'test-msg-2', timestamp: new Date(), body: buildJob, attempts: 1 }],
			},
			env,
		);

		// Verify index is ready with no entries
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes[0].status).toBe('ready');
		expect(topology.virtual_index_entries).toHaveLength(0);
	});

	it('should skip NULL values when building index', async () => {
		const dbId = 'test-index-e2e-3';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert data with NULL emails
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (1, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (2, ${'Bob'}, ${null})`; // NULL email
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (3, ${'Charlie'}, ${'charlie@example.com'})`;

		await conductor.sql`CREATE INDEX idx_email ON users(email)`;

		// Build the index
		const buildJob: IndexBuildJob = {
			type: 'build_index',
			database_id: dbId,
			table_name: 'users',
			column_name: 'email',
			index_name: 'idx_email',
			created_at: new Date().toISOString(),
		};

		await queueHandler(
			{
				queue: 'vitess-index-jobs',
				messages: [{ id: 'test-msg-3', timestamp: new Date(), body: buildJob, attempts: 1 }],
			},
			env,
		);

		// Should only have 2 entries (Alice and Charlie), NULL is skipped
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes[0].status).toBe('ready');
		expect(topology.virtual_index_entries).toHaveLength(2);

		const emails = topology.virtual_index_entries.map((e) => e.key_value);
		expect(emails).toContain('alice@example.com');
		expect(emails).toContain('charlie@example.com');
		expect(emails).not.toContain('null');
	});

	it('should handle index build failure gracefully', async () => {
		const dbId = 'test-index-e2e-4';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`;

		// Create index on non-existent column (will fail during build)
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.createVirtualIndex('idx_bad', 'users', 'nonexistent_column', 'hash');

		// Try to build the index (should fail)
		const buildJob: IndexBuildJob = {
			type: 'build_index',
			database_id: dbId,
			table_name: 'users',
			column_name: 'nonexistent_column',
			index_name: 'idx_bad',
			created_at: new Date().toISOString(),
		};

		// Queue handler logs errors but doesn't throw - it's designed to continue processing other messages
		await queueHandler(
			{
				queue: 'vitess-index-jobs',
				messages: [{ id: 'test-msg-4', timestamp: new Date(), body: buildJob, attempts: 1 }],
			},
			env,
		);

		// Verify index status is 'failed' with error message
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes[0].status).toBe('failed');
		expect(topology.virtual_indexes[0].error_message).toBeTruthy();
		expect(topology.virtual_indexes[0].error_message).toContain('nonexistent_column');
	});

	it('should build index with many unique values', async () => {
		const dbId = 'test-index-e2e-5';
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert 50 users with unique emails
		for (let i = 0; i < 50; i++) {
			await conductor.sql`INSERT INTO users (id, name, email) VALUES (${i}, ${`User${i}`}, ${`user${i}@example.com`})`;
		}

		await conductor.sql`CREATE INDEX idx_email ON users(email)`;

		// Build the index
		const buildJob: IndexBuildJob = {
			type: 'build_index',
			database_id: dbId,
			table_name: 'users',
			column_name: 'email',
			index_name: 'idx_email',
			created_at: new Date().toISOString(),
		};

		await queueHandler(
			{
				queue: 'vitess-index-jobs',
				messages: [{ id: 'test-msg-5', timestamp: new Date(), body: buildJob, attempts: 1 }],
			},
			env,
		);

		// Verify all 50 unique emails are indexed
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes[0].status).toBe('ready');
		expect(topology.virtual_index_entries).toHaveLength(50);

		// Each email should have exactly 1 shard
		for (const entry of topology.virtual_index_entries) {
			const shardIds = JSON.parse(entry.shard_ids);
			expect(shardIds).toHaveLength(1);
		}
	});
});
