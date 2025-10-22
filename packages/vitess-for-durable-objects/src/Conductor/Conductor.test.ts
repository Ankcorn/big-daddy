import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';
import { createConductor } from './Conductor';

describe('Conductor', () => {
	async function initializeTopology(dbId: string, numNodes: number = 2) {
		const topologyId = env.TOPOLOGY.idFromName(dbId);
		const topologyStub = env.TOPOLOGY.get(topologyId);
		await topologyStub.create(numNodes);
	}

	it('should execute queries with parameters', async () => {
		const dbId = 'test-query';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`;

		const userId = 123;
		const name = "John's";
		const age = 25;

		await conductor.sql`SELECT * FROM users WHERE id = ${userId}`;
		await conductor.sql`SELECT * FROM users WHERE name = ${name} AND age > ${age}`;
		await conductor.sql`SELECT * FROM users`;
	});

	it('should create tables and update topology', async () => {
		const dbId = 'test-create';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`;

		// Verify topology
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.tables).toHaveLength(1);
		expect(topology.tables[0]).toMatchObject({
			table_name: 'products',
			primary_key: 'id',
			shard_key: 'id',
			num_shards: 1,
		});

		// Verify table exists in all storage nodes (get node IDs from topology)
		for (const node of topology.storage_nodes) {
			const storageStub = env.STORAGE.get(env.STORAGE.idFromName(node.node_id));
			const result = await storageStub.executeQuery({
				query: 'SELECT name FROM sqlite_master WHERE type="table" AND name="products"',
				queryType: 'SELECT',
			});
			if ('rows' in result) {
				expect(result.rows).toHaveLength(1);
			}
		}
	});

	it('should route INSERT to correct shard based on shard key', async () => {
		const dbId = 'test-insert-routing';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		// Create table with 4 shards (distributed across 2 nodes)
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Get topology to see shard distribution
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();
		const userShards = topology.table_shards.filter((s) => s.table_name === 'users');

		// Insert users with different IDs
		const userId1 = 100;
		const userId2 = 200;

		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${userId1}, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${userId2}, ${'Bob'}, ${'bob@example.com'})`;

		// Query all shards to verify data distribution
		const allUsers = await conductor.sql`SELECT * FROM users ORDER BY id`;

		expect(allUsers.rows).toHaveLength(2);
		expect(allUsers.rows[0]).toMatchObject({ id: userId1, name: 'Alice' });
		expect(allUsers.rows[1]).toMatchObject({ id: userId2, name: 'Bob' });
	});

	it('should route UPDATE to correct shard when WHERE filters on shard key', async () => {
		const dbId = 'test-update-routing';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		const userId1 = 100;
		const userId2 = 200;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${userId1}, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${userId2}, ${'Bob'}, ${'bob@example.com'})`;

		// Update a specific user by shard key (should route to one shard)
		const result = await conductor.sql`UPDATE users SET name = ${'Alice Updated'} WHERE id = ${userId1}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the update worked
		const allUsers = await conductor.sql`SELECT * FROM users ORDER BY id`;
		expect(allUsers.rows).toHaveLength(2);
		expect(allUsers.rows[0]).toMatchObject({ id: userId1, name: 'Alice Updated' });
		expect(allUsers.rows[1]).toMatchObject({ id: userId2, name: 'Bob' });
	});

	it('should route UPDATE to all shards when WHERE does not filter on shard key', async () => {
		const dbId = 'test-update-all-shards';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${100}, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${200}, ${'Bob'}, ${'bob@example.com'})`;

		// Update based on non-shard-key column (should query all shards)
		const result = await conductor.sql`UPDATE users SET email = ${'updated@example.com'} WHERE name = ${'Alice'}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the update worked
		const alice = await conductor.sql`SELECT * FROM users WHERE id = ${100}`;
		expect(alice.rows[0]).toMatchObject({ email: 'updated@example.com' });
	});

	it('should route DELETE to correct shard when WHERE filters on shard key', async () => {
		const dbId = 'test-delete-routing';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		const userId1 = 100;
		const userId2 = 200;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${userId1}, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${userId2}, ${'Bob'}, ${'bob@example.com'})`;

		// Delete a specific user by shard key (should route to one shard)
		const result = await conductor.sql`DELETE FROM users WHERE id = ${userId1}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the delete worked
		const allUsers = await conductor.sql`SELECT * FROM users ORDER BY id`;
		expect(allUsers.rows).toHaveLength(1);
		expect(allUsers.rows[0]).toMatchObject({ id: userId2, name: 'Bob' });
	});

	it('should route DELETE to all shards when WHERE does not filter on shard key', async () => {
		const dbId = 'test-delete-all-shards';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${100}, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${200}, ${'Bob'}, ${'bob@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${300}, ${'Alice'}, ${'alice2@example.com'})`;

		// Delete based on non-shard-key column (should query all shards)
		const result = await conductor.sql`DELETE FROM users WHERE name = ${'Alice'}`;

		expect(result.rowsAffected).toBe(2);

		// Verify the delete worked
		const remaining = await conductor.sql`SELECT * FROM users ORDER BY id`;
		expect(remaining.rows).toHaveLength(1);
		expect(remaining.rows[0]).toMatchObject({ id: 200, name: 'Bob' });
	});

	it('should route SELECT to correct shard when WHERE filters on shard key', async () => {
		const dbId = 'test-select-routing';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		const userId1 = 100;
		const userId2 = 200;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${userId1}, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${userId2}, ${'Bob'}, ${'bob@example.com'})`;

		// Select a specific user by shard key (should route to one shard)
		const result = await conductor.sql`SELECT * FROM users WHERE id = ${userId1}`;

		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]).toMatchObject({ id: userId1, name: 'Alice', email: 'alice@example.com' });
	});

	it('should route SELECT to all shards when WHERE does not filter on shard key', async () => {
		const dbId = 'test-select-all-shards';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data with same name on different shards
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${100}, ${'Alice'}, ${'alice1@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${200}, ${'Alice'}, ${'alice2@example.com'})`;

		// Select based on non-shard-key column (should query all shards)
		const result = await conductor.sql`SELECT * FROM users WHERE name = ${'Alice'} ORDER BY id`;

		expect(result.rows).toHaveLength(2);
		expect(result.rows[0]).toMatchObject({ id: 100, name: 'Alice' });
		expect(result.rows[1]).toMatchObject({ id: 200, name: 'Alice' });
	});

	it('should handle complex WHERE with multiple placeholders - shard key last', async () => {
		const dbId = 'test-complex-where-1';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`;

		// Insert test data
		await conductor.sql`INSERT INTO users (id, name, age) VALUES (${100}, ${'Alice'}, ${25})`;
		await conductor.sql`INSERT INTO users (id, name, age) VALUES (${200}, ${'Bob'}, ${30})`;

		// SELECT with multiple placeholders: WHERE age > ? AND id = ?
		// This tests that we use the correct parameter (id=100, not age=20)
		const result = await conductor.sql`SELECT * FROM users WHERE age > ${20} AND id = ${100}`;

		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]).toMatchObject({ id: 100, name: 'Alice' });
	});

	it('should handle UPDATE with multiple placeholders in SET and WHERE', async () => {
		const dbId = 'test-update-complex';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`;

		// Insert test data
		await conductor.sql`INSERT INTO users (id, name, age) VALUES (${100}, ${'Alice'}, ${25})`;
		await conductor.sql`INSERT INTO users (id, name, age) VALUES (${200}, ${'Bob'}, ${30})`;

		// UPDATE with SET placeholders and WHERE placeholders: SET name = ?, age = ? WHERE id = ?
		const result = await conductor.sql`UPDATE users SET name = ${'Alice Updated'}, age = ${26} WHERE id = ${100}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the update
		const check = await conductor.sql`SELECT * FROM users WHERE id = ${100}`;
		expect(check.rows[0]).toMatchObject({ id: 100, name: 'Alice Updated', age: 26 });
	});

	it('should handle DELETE with complex WHERE clause', async () => {
		const dbId = 'test-delete-complex';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`;

		// Insert test data
		await conductor.sql`INSERT INTO users (id, name, age) VALUES (${100}, ${'Alice'}, ${25})`;
		await conductor.sql`INSERT INTO users (id, name, age) VALUES (${200}, ${'Bob'}, ${30})`;

		// DELETE with multiple placeholders: WHERE age < ? AND id = ?
		const result = await conductor.sql`DELETE FROM users WHERE age < ${28} AND id = ${100}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the delete
		const remaining = await conductor.sql`SELECT * FROM users ORDER BY id`;
		expect(remaining.rows).toHaveLength(1);
		expect(remaining.rows[0]).toMatchObject({ id: 200, name: 'Bob' });
	});

	it('should handle UPDATE and DELETE without WHERE clause (affects all shards)', async () => {
		const dbId = 'test-no-where';
		await initializeTopology(dbId, 2);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${100}, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${200}, ${'Bob'}, ${'bob@example.com'})`;

		// Update all users (no WHERE clause)
		const updateResult = await conductor.sql`UPDATE users SET email = ${'everyone@example.com'}`;
		expect(updateResult.rowsAffected).toBe(2);

		// Verify updates
		const allUsers = await conductor.sql`SELECT * FROM users ORDER BY id`;
		expect(allUsers.rows.every(u => u.email === 'everyone@example.com')).toBe(true);

		// Delete all users (no WHERE clause)
		const deleteResult = await conductor.sql`DELETE FROM users`;
		expect(deleteResult.rowsAffected).toBe(2);

		// Verify deletes
		const remaining = await conductor.sql`SELECT * FROM users`;
		expect(remaining.rows).toHaveLength(0);
	});

	it('should handle queries with more than 7 shards (batching)', async () => {
		const dbId = 'test-many-shards';
		// Create 10 nodes (more than the 7 parallel query limit)
		await initializeTopology(dbId, 10);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert data that will be distributed across shards
		const inserts = [];
		for (let i = 0; i < 20; i++) {
			inserts.push(
				conductor.sql`INSERT INTO users (id, name, email) VALUES (${i}, ${`User ${i}`}, ${`user${i}@example.com`})`,
			);
		}
		await Promise.all(inserts);

		// Query all shards - should work with batching
		const allUsers = await conductor.sql`SELECT * FROM users ORDER BY id`;

		expect(allUsers.rows).toHaveLength(20);
		expect(allUsers.rows[0]).toMatchObject({ id: 0, name: 'User 0' });
		expect(allUsers.rows[19]).toMatchObject({ id: 19, name: 'User 19' });
	});

	it('should create a virtual index', async () => {
		const dbId = 'test-create-index-1';
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Create index
		const result = await conductor.sql`CREATE INDEX idx_email ON users(email)`;
		expect(result.rows).toHaveLength(0);
		expect(result.rowsAffected).toBe(0);

		// Verify index was created in topology
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes).toHaveLength(1);
		expect(topology.virtual_indexes[0].index_name).toBe('idx_email');
		expect(topology.virtual_indexes[0].table_name).toBe('users');
		expect(topology.virtual_indexes[0].column_name).toBe('email');
		expect(topology.virtual_indexes[0].index_type).toBe('hash');
		expect(topology.virtual_indexes[0].status).toBe('building');
	});

	it('should create a unique index', async () => {
		const dbId = 'test-create-index-2';
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Create unique index
		await conductor.sql`CREATE UNIQUE INDEX idx_email ON users(email)`;

		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes[0].index_type).toBe('unique');
	});

	it('should handle CREATE INDEX IF NOT EXISTS', async () => {
		const dbId = 'test-create-index-3';
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Create index
		await conductor.sql`CREATE INDEX idx_email ON users(email)`;

		// Create again with IF NOT EXISTS - should succeed silently
		const result = await conductor.sql`CREATE INDEX IF NOT EXISTS idx_email ON users(email)`;
		expect(result.rows).toHaveLength(0);

		// Verify still only one index
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();
		expect(topology.virtual_indexes).toHaveLength(1);
	});

	it('should error on duplicate index without IF NOT EXISTS', async () => {
		const dbId = 'test-create-index-4';
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		await conductor.sql`CREATE INDEX idx_email ON users(email)`;

		// Try to create duplicate without IF NOT EXISTS
		await expect(conductor.sql`CREATE INDEX idx_email ON users(email)`).rejects.toThrow('already exists');
	});

	it('should error on index for non-existent table', async () => {
		const dbId = 'test-create-index-5';
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, env);

		// Try to create index on non-existent table
		await expect(conductor.sql`CREATE INDEX idx_email ON users(email)`).rejects.toThrow('does not exist');
	});

	it('should error on multi-column indexes', async () => {
		const dbId = 'test-create-index-6';
		await initializeTopology(dbId, 3);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Try to create multi-column index
		await expect(conductor.sql`CREATE INDEX idx_name_email ON users(name, email)`).rejects.toThrow(
			'Multi-column indexes are not yet supported',
		);
	});
});

