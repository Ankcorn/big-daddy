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

		// Verify table exists in storage nodes
		for (const nodeId of ['node-0', 'node-1']) {
			const storageStub = env.STORAGE.get(env.STORAGE.idFromName(nodeId));
			const result = await storageStub.executeQuery({
				query: 'SELECT name FROM sqlite_master WHERE type="table" AND name="products"',
				queryType: 'SELECT',
			});
			if ('rows' in result) {
				expect(result.rows).toHaveLength(1);
			}
		}
	});
});
