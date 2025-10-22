import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';

describe('Topology Durable Object', () => {
	describe('create', () => {
		it('should create a topology with specified number of nodes', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-create-1');
			const stub = env.TOPOLOGY.get(id);

			// Create topology with 3 nodes
			const result = await stub.create(3);
			expect(result.success).toBe(true);

			// Verify nodes were created
			const topology = await stub.getTopology();
			expect(topology.storage_nodes).toHaveLength(3);

			// All nodes should have unique UUIDs and be active
			topology.storage_nodes.forEach((node) => {
				expect(node.node_id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
				expect(node.status).toBe('active');
				expect(node.capacity_used).toBe(0);
				expect(node.error).toBeNull();
			});

			// Verify all node IDs are unique
			const nodeIds = topology.storage_nodes.map((n) => n.node_id);
			expect(new Set(nodeIds).size).toBe(3);
		});

		it('should throw if create is called twice', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-create-2');
			const stub = env.TOPOLOGY.get(id);

			// First create should succeed
			const result1 = await stub.create(2);
			expect(result1.success).toBe(true);

			// Second create should throw
			await expect(stub.create(2)).rejects.toThrow('Topology already created');
		});

		it('should error if numNodes is less than 1', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-create-3');
			const stub = env.TOPOLOGY.get(id);

			const result = await stub.create(0);
			expect(result.success).toBe(false);
			expect(result.error).toContain('Number of nodes must be at least 1');
		});
	});

	describe('getTopology', () => {
		it('should throw if topology not created', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-get-1');
			const stub = env.TOPOLOGY.get(id);

			// Should throw when trying to get topology before creation
			await expect(stub.getTopology()).rejects.toThrow('Topology not created');
		});

		it('should return topology after creation', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-get-2');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			const topology = await stub.getTopology();
			expect(topology).toHaveProperty('storage_nodes');
			expect(topology).toHaveProperty('tables');
			expect(topology.storage_nodes).toHaveLength(2);
			expect(topology.tables).toEqual([]);
		});
	});

	describe('updateTopology', () => {
		it('should throw if topology not created', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-update-1');
			const stub = env.TOPOLOGY.get(id);

			// Should throw when trying to update topology before creation
			await expect(
				stub.updateTopology({
					tables: {
						add: [
							{
								table_name: 'test',
								primary_key: 'id',
								primary_key_type: 'INTEGER',
								shard_strategy: 'hash',
								shard_key: 'id',
								num_shards: 1,
								block_size: 500,
							},
						],
					},
				})
			).rejects.toThrow('Topology not created');
		});

		it('should add tables and create table_shards', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-update-2');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 4,
							block_size: 500,
						},
					],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.tables).toHaveLength(1);
			expect(topology.tables[0].table_name).toBe('users');

			// Verify table_shards were created
			const userShards = topology.table_shards.filter((s) => s.table_name === 'users');
			expect(userShards).toHaveLength(4);

			// Verify shards are distributed across nodes using modulo
			// With 2 nodes and 4 shards: shard 0 and 2 should map to same node, 1 and 3 to the other
			const node0 = userShards[0].node_id;
			const node1 = userShards[1].node_id;
			expect(userShards[0].shard_id).toBe(0);
			expect(userShards[1].shard_id).toBe(1);
			expect(userShards[2].shard_id).toBe(2);
			expect(userShards[2].node_id).toBe(node0); // 2 % 2 = 0, same as shard 0
			expect(userShards[3].shard_id).toBe(3);
			expect(userShards[3].node_id).toBe(node1); // 3 % 2 = 1, same as shard 1
			expect(node0).not.toBe(node1); // Nodes should be different
		});

		it('should update table configuration', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-update-4');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Add table
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'items',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Update table
			await stub.updateTopology({
				tables: {
					update: [
						{
							table_name: 'items',
							block_size: 1000,
						},
					],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.tables[0].block_size).toBe(1000);
		});

		it('should remove tables', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-update-5');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Add table
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'temp',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Remove table
			await stub.updateTopology({
				tables: {
					remove: ['temp'],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.tables).toHaveLength(0);
		});
	});

	describe('Virtual Indexes', () => {
		it('should create a virtual index', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-1');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Add a table first
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Create index
			const result = await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');
			expect(result.success).toBe(true);

			// Verify index was created
			const topology = await stub.getTopology();
			expect(topology.virtual_indexes).toHaveLength(1);
			expect(topology.virtual_indexes[0].index_name).toBe('idx_email');
			expect(topology.virtual_indexes[0].table_name).toBe('users');
			expect(topology.virtual_indexes[0].column_name).toBe('email');
			expect(topology.virtual_indexes[0].index_type).toBe('hash');
			expect(topology.virtual_indexes[0].status).toBe('building');
			expect(topology.virtual_indexes[0].error_message).toBeNull();
		});

		it('should not allow duplicate index names', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-2');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Create first index
			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');

			// Try to create duplicate
			const result = await stub.createVirtualIndex('idx_email', 'users', 'name', 'hash');
			expect(result.success).toBe(false);
			expect(result.error).toContain('already exists');
		});

		it('should not allow index on non-existent table', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-3');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Try to create index on non-existent table
			const result = await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');
			expect(result.success).toBe(false);
			expect(result.error).toContain('does not exist');
		});

		it('should update index status', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-4');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');

			// Update to ready
			await stub.updateIndexStatus('idx_email', 'ready');

			const topology = await stub.getTopology();
			expect(topology.virtual_indexes[0].status).toBe('ready');
			expect(topology.virtual_indexes[0].error_message).toBeNull();
		});

		it('should update index status with error message', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-5');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');

			// Update to failed with error
			await stub.updateIndexStatus('idx_email', 'failed', 'Column does not exist');

			const topology = await stub.getTopology();
			expect(topology.virtual_indexes[0].status).toBe('failed');
			expect(topology.virtual_indexes[0].error_message).toBe('Column does not exist');
		});

		it('should batch upsert index entries', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-6');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');

			// Add index entry
			await stub.batchUpsertIndexEntries('idx_email', [{ keyValue: 'alice@example.com', shardIds: [0, 1] }]);

			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(1);
			expect(topology.virtual_index_entries[0].index_name).toBe('idx_email');
			expect(topology.virtual_index_entries[0].key_value).toBe('alice@example.com');
			expect(JSON.parse(topology.virtual_index_entries[0].shard_ids)).toEqual([0, 1]);

			// Update same entry (upsert)
			await stub.batchUpsertIndexEntries('idx_email', [{ keyValue: 'alice@example.com', shardIds: [0] }]);

			const topology2 = await stub.getTopology();
			expect(topology2.virtual_index_entries).toHaveLength(1);
			expect(JSON.parse(topology2.virtual_index_entries[0].shard_ids)).toEqual([0]);
		});

		it('should get indexed shards for a value', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-7');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');
			await stub.batchUpsertIndexEntries('idx_email', [{ keyValue: 'alice@example.com', shardIds: [0, 1] }]);

			// Get shards for existing value
			const shards = await stub.getIndexedShards('idx_email', 'alice@example.com');
			expect(shards).toEqual([0, 1]);

			// Get shards for non-existent value
			const noShards = await stub.getIndexedShards('idx_email', 'bob@example.com');
			expect(noShards).toBeNull();
		});

		it('should drop virtual index and cascade delete entries', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-8');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');
			await stub.batchUpsertIndexEntries('idx_email', [
				{ keyValue: 'alice@example.com', shardIds: [0, 1] },
				{ keyValue: 'bob@example.com', shardIds: [1] },
			]);

			// Drop index
			const result = await stub.dropVirtualIndex('idx_email');
			expect(result.success).toBe(true);

			// Verify index and entries are deleted
			const topology = await stub.getTopology();
			expect(topology.virtual_indexes).toHaveLength(0);
			expect(topology.virtual_index_entries).toHaveLength(0);
		});

		it('should batch upsert multiple index entries efficiently', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-9');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');

			// Batch insert multiple entries
			const result = await stub.batchUpsertIndexEntries('idx_email', [
				{ keyValue: 'alice@example.com', shardIds: [0, 1] },
				{ keyValue: 'bob@example.com', shardIds: [1] },
				{ keyValue: 'charlie@example.com', shardIds: [0] },
			]);

			expect(result.count).toBe(3);

			// Verify all entries were created
			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(3);

			const alice = topology.virtual_index_entries.find((e) => e.key_value === 'alice@example.com');
			const bob = topology.virtual_index_entries.find((e) => e.key_value === 'bob@example.com');
			const charlie = topology.virtual_index_entries.find((e) => e.key_value === 'charlie@example.com');

			expect(JSON.parse(alice!.shard_ids)).toEqual([0, 1]);
			expect(JSON.parse(bob!.shard_ids)).toEqual([1]);
			expect(JSON.parse(charlie!.shard_ids)).toEqual([0]);
		});

		it('should batch upsert update existing entries', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-10');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');

			// Initial batch insert
			await stub.batchUpsertIndexEntries('idx_email', [
				{ keyValue: 'alice@example.com', shardIds: [0, 1] },
				{ keyValue: 'bob@example.com', shardIds: [1] },
			]);

			// Batch upsert with one update and one new entry
			const result = await stub.batchUpsertIndexEntries('idx_email', [
				{ keyValue: 'alice@example.com', shardIds: [0] }, // Update: remove shard 1
				{ keyValue: 'charlie@example.com', shardIds: [0, 1] }, // New entry
			]);

			expect(result.count).toBe(2);

			// Verify updates
			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(3); // bob, alice, charlie

			const alice = topology.virtual_index_entries.find((e) => e.key_value === 'alice@example.com');
			const bob = topology.virtual_index_entries.find((e) => e.key_value === 'bob@example.com');
			const charlie = topology.virtual_index_entries.find((e) => e.key_value === 'charlie@example.com');

			expect(JSON.parse(alice!.shard_ids)).toEqual([0]); // Updated
			expect(JSON.parse(bob!.shard_ids)).toEqual([1]); // Unchanged
			expect(JSON.parse(charlie!.shard_ids)).toEqual([0, 1]); // New
		});

		it('should handle empty batch upsert', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-11');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');

			// Empty batch should return count 0
			const result = await stub.batchUpsertIndexEntries('idx_email', []);
			expect(result.count).toBe(0);

			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(0);
		});

		it('should handle large batch upsert', async () => {
			const id = env.TOPOLOGY.idFromName('test-topology-index-12');
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: 'users',
							primary_key: 'id',
							primary_key_type: 'INTEGER',
							shard_strategy: 'hash',
							shard_key: 'id',
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex('idx_email', 'users', 'email', 'hash');

			// Create a large batch (100 entries)
			const entries = [];
			for (let i = 0; i < 100; i++) {
				entries.push({
					keyValue: `user${i}@example.com`,
					shardIds: [i % 2], // Alternate between shards 0 and 1
				});
			}

			const result = await stub.batchUpsertIndexEntries('idx_email', entries);
			expect(result.count).toBe(100);

			// Verify all entries were created
			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(100);

			// Verify a few random entries
			const user0 = topology.virtual_index_entries.find((e) => e.key_value === 'user0@example.com');
			const user50 = topology.virtual_index_entries.find((e) => e.key_value === 'user50@example.com');
			const user99 = topology.virtual_index_entries.find((e) => e.key_value === 'user99@example.com');

			expect(JSON.parse(user0!.shard_ids)).toEqual([0]);
			expect(JSON.parse(user50!.shard_ids)).toEqual([0]);
			expect(JSON.parse(user99!.shard_ids)).toEqual([1]);
		});
	});
});
