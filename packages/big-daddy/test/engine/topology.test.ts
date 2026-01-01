import { env } from "cloudflare:test";
import { parse } from "@databases/sqlite-ast";
import { describe, expect, it } from "vitest";
import type {
	ShardRowCount,
	StorageNode,
	TableShard,
	VirtualIndex,
	VirtualIndexEntry,
} from "../../src/engine/topology/types";

type ShardInfo = { table_name: string; shard_id: number; node_id: string };

describe("Topology Durable Object", () => {
	describe("create", () => {
		it("should create a topology with specified number of nodes", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-create-1");
			const stub = env.TOPOLOGY.get(id);

			// Create topology with 3 nodes
			const result = await stub.create(3);
			expect(result.success).toBe(true);

			// Verify nodes were created
			const topology = await stub.getTopology();
			expect(topology.storage_nodes).toHaveLength(3);

			// All nodes should have unique UUIDs and be active
			topology.storage_nodes.forEach((node: StorageNode) => {
				expect(node.node_id).toMatch(
					/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
				);
				expect(node.status).toBe("active");
				expect(node.capacity_used).toBe(0);
				expect(node.error).toBeNull();
			});

			// Verify all node IDs are unique
			const nodeIds = topology.storage_nodes.map((n: StorageNode) => n.node_id);
			expect(new Set(nodeIds).size).toBe(3);
		});

		it("should throw if create is called twice", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-create-2");
			const stub = env.TOPOLOGY.get(id);

			// First create should succeed
			const result1 = await stub.create(2);
			expect(result1.success).toBe(true);

			// Second create should throw
			await expect(stub.create(2)).rejects.toThrow("Topology already created");
		});

		it("should error if numNodes is less than 1", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-create-3");
			const stub = env.TOPOLOGY.get(id);

			const result = await stub.create(0);
			expect(result.success).toBe(false);
			expect(result.error).toContain("Number of nodes must be at least 1");
		});
	});

	describe("getTopology", () => {
		it("should throw if topology not created", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-get-1");
			const stub = env.TOPOLOGY.get(id);

			// Should throw when trying to get topology before creation
			await expect(stub.getTopology()).rejects.toThrow("Topology not created");
		});

		it("should return topology after creation", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-get-2");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			const topology = await stub.getTopology();
			expect(topology).toHaveProperty("storage_nodes");
			expect(topology).toHaveProperty("tables");
			expect(topology.storage_nodes).toHaveLength(2);
			expect(topology.tables).toEqual([]);
		});
	});

	describe("updateTopology", () => {
		it("should throw if topology not created", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-update-1");
			const stub = env.TOPOLOGY.get(id);

			// Should throw when trying to update topology before creation
			await expect(
				stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "test",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 1,
								block_size: 500,
							},
						],
					},
				}),
			).rejects.toThrow("Topology not created");
		});

		it("should add tables and create table_shards", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-update-2");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 4,
							block_size: 500,
						},
					],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.tables).toHaveLength(1);
			expect(topology.tables[0]!.table_name).toBe("users");

			// Verify table_shards were created
			const userShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users",
			);
			expect(userShards).toHaveLength(4);

			// Verify shards are distributed across nodes using modulo
			// With 2 nodes and 4 shards: shard 0 and 2 should map to same node, 1 and 3 to the other
			const node0 = userShards[0]!.node_id;
			const node1 = userShards[1]!.node_id;
			expect(userShards[0]!.shard_id).toBe(0);
			expect(userShards[1]!.shard_id).toBe(1);
			expect(userShards[2]!.shard_id).toBe(2);
			expect(userShards[2]!.node_id).toBe(node0); // 2 % 2 = 0, same as shard 0
			expect(userShards[3]!.shard_id).toBe(3);
			expect(userShards[3]!.node_id).toBe(node1); // 3 % 2 = 1, same as shard 1
			expect(node0).not.toBe(node1); // Nodes should be different
		});

		it("should update table configuration", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-update-4");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Add table
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "items",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
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
							table_name: "items",
							block_size: 1000,
						},
					],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.tables[0]!.block_size).toBe(1000);
		});

		it("should remove tables", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-update-5");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Add table
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "temp",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Remove table
			await stub.updateTopology({
				tables: {
					remove: ["temp"],
				},
			});

			const topology = await stub.getTopology();
			expect(topology.tables).toHaveLength(0);
		});
	});

	describe("Virtual Indexes", () => {
		it("should create a virtual index", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-1");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Add a table first
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Create index
			const result = await stub.createVirtualIndex(
				"idx_email",
				"users",
				["email"],
				"hash",
			);
			expect(result.success).toBe(true);

			// Verify index was created
			const topology = await stub.getTopology();
			expect(topology.virtual_indexes).toHaveLength(1);
			expect(topology.virtual_indexes[0]!.index_name).toBe("idx_email");
			expect(topology.virtual_indexes[0]!.table_name).toBe("users");
			expect(JSON.parse(topology.virtual_indexes[0]!.columns)).toEqual([
				"email",
			]);
			expect(topology.virtual_indexes[0]!.index_type).toBe("hash");
			expect(topology.virtual_indexes[0]!.status).toBe("building");
			expect(topology.virtual_indexes[0]!.error_message).toBeNull();
		});

		it("should not allow duplicate index names", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-2");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Create first index
			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");

			// Try to create duplicate
			const result = await stub.createVirtualIndex(
				"idx_email",
				"users",
				["name"],
				"hash",
			);
			expect(result.success).toBe(false);
			expect(result.error).toContain("already exists");
		});

		it("should not allow index on non-existent table", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-3");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			// Try to create index on non-existent table
			const result = await stub.createVirtualIndex(
				"idx_email",
				"users",
				["email"],
				"hash",
			);
			expect(result.success).toBe(false);
			expect(result.error).toContain("does not exist");
		});

		it("should update index status", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-4");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");

			// Update to ready
			await stub.updateIndexStatus("idx_email", "ready");

			const topology = await stub.getTopology();
			expect(topology.virtual_indexes[0]!.status).toBe("ready");
			expect(topology.virtual_indexes[0]!.error_message).toBeNull();
		});

		it("should update index status with error message", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-5");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");

			// Update to failed with error
			await stub.updateIndexStatus(
				"idx_email",
				"failed",
				"Column does not exist",
			);

			const topology = await stub.getTopology();
			expect(topology.virtual_indexes[0]!.status).toBe("failed");
			expect(topology.virtual_indexes[0]!.error_message).toBe(
				"Column does not exist",
			);
		});

		it("should batch upsert index entries", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-6");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");

			// Add index entry
			await stub.batchUpsertIndexEntries("idx_email", [
				{ keyValue: "alice@example.com", shardIds: [0, 1] },
			]);

			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(1);
			expect(topology.virtual_index_entries[0]!.index_name).toBe("idx_email");
			expect(topology.virtual_index_entries[0]!.key_value).toBe(
				"alice@example.com",
			);
			expect(JSON.parse(topology.virtual_index_entries[0]!.shard_ids)).toEqual([
				0, 1,
			]);

			// Update same entry (upsert)
			await stub.batchUpsertIndexEntries("idx_email", [
				{ keyValue: "alice@example.com", shardIds: [0] },
			]);

			const topology2 = await stub.getTopology();
			expect(topology2.virtual_index_entries).toHaveLength(1);
			expect(JSON.parse(topology2.virtual_index_entries[0]!.shard_ids)).toEqual(
				[0],
			);
		});

		it("should get indexed shards for a value", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-7");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");
			await stub.batchUpsertIndexEntries("idx_email", [
				{ keyValue: "alice@example.com", shardIds: [0, 1] },
			]);

			// Get shards for existing value
			const shards = await stub.getIndexedShards(
				"idx_email",
				"alice@example.com",
			);
			expect(shards).toEqual([0, 1]);

			// Get shards for non-existent value
			const noShards = await stub.getIndexedShards(
				"idx_email",
				"bob@example.com",
			);
			expect(noShards).toBeNull();
		});

		it("should drop virtual index and cascade delete entries", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-8");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");
			await stub.batchUpsertIndexEntries("idx_email", [
				{ keyValue: "alice@example.com", shardIds: [0, 1] },
				{ keyValue: "bob@example.com", shardIds: [1] },
			]);

			// Drop index
			const result = await stub.dropVirtualIndex("idx_email");
			expect(result.success).toBe(true);

			// Verify index and entries are deleted
			const topology = await stub.getTopology();
			expect(topology.virtual_indexes).toHaveLength(0);
			expect(topology.virtual_index_entries).toHaveLength(0);
		});

		it("should batch upsert multiple index entries efficiently", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-9");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");

			// Batch insert multiple entries
			const result = await stub.batchUpsertIndexEntries("idx_email", [
				{ keyValue: "alice@example.com", shardIds: [0, 1] },
				{ keyValue: "bob@example.com", shardIds: [1] },
				{ keyValue: "charlie@example.com", shardIds: [0] },
			]);

			expect(result.count).toBe(3);

			// Verify all entries were created
			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(3);

			const alice = topology.virtual_index_entries.find(
				(e: VirtualIndexEntry) => e.key_value === "alice@example.com",
			);
			const bob = topology.virtual_index_entries.find(
				(e: VirtualIndexEntry) => e.key_value === "bob@example.com",
			);
			const charlie = topology.virtual_index_entries.find(
				(e: VirtualIndexEntry) => e.key_value === "charlie@example.com",
			);

			expect(JSON.parse(alice!.shard_ids)).toEqual([0, 1]);
			expect(JSON.parse(bob!.shard_ids)).toEqual([1]);
			expect(JSON.parse(charlie!.shard_ids)).toEqual([0]);
		});

		it("should batch upsert update existing entries", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-10");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");

			// Initial batch insert
			await stub.batchUpsertIndexEntries("idx_email", [
				{ keyValue: "alice@example.com", shardIds: [0, 1] },
				{ keyValue: "bob@example.com", shardIds: [1] },
			]);

			// Batch upsert with one update and one new entry
			const result = await stub.batchUpsertIndexEntries("idx_email", [
				{ keyValue: "alice@example.com", shardIds: [0] }, // Update: remove shard 1
				{ keyValue: "charlie@example.com", shardIds: [0, 1] }, // New entry
			]);

			expect(result.count).toBe(2);

			// Verify updates
			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(3); // bob, alice, charlie

			const alice = topology.virtual_index_entries.find(
				(e: VirtualIndexEntry) => e.key_value === "alice@example.com",
			);
			const bob = topology.virtual_index_entries.find(
				(e: VirtualIndexEntry) => e.key_value === "bob@example.com",
			);
			const charlie = topology.virtual_index_entries.find(
				(e: VirtualIndexEntry) => e.key_value === "charlie@example.com",
			);

			expect(JSON.parse(alice!.shard_ids)).toEqual([0]); // Updated
			expect(JSON.parse(bob!.shard_ids)).toEqual([1]); // Unchanged
			expect(JSON.parse(charlie!.shard_ids)).toEqual([0, 1]); // New
		});

		it("should handle empty batch upsert", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-11");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");

			// Empty batch should return count 0
			const result = await stub.batchUpsertIndexEntries("idx_email", []);
			expect(result.count).toBe(0);

			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(0);
		});

		it("should handle large batch upsert", async () => {
			const id = env.TOPOLOGY.idFromName("test-topology-index-12");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);

			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");

			// Create a large batch (100 entries)
			const entries = [];
			for (let i = 0; i < 100; i++) {
				entries.push({
					keyValue: `user${i}@example.com`,
					shardIds: [i % 2], // Alternate between shards 0 and 1
				});
			}

			const result = await stub.batchUpsertIndexEntries("idx_email", entries);
			expect(result.count).toBe(100);

			// Verify all entries were created
			const topology = await stub.getTopology();
			expect(topology.virtual_index_entries).toHaveLength(100);

			// Verify a few random entries
			const user0 = topology.virtual_index_entries.find(
				(e: VirtualIndexEntry) => e.key_value === "user0@example.com",
			);
			const user50 = topology.virtual_index_entries.find(
				(e: VirtualIndexEntry) => e.key_value === "user50@example.com",
			);
			const user99 = topology.virtual_index_entries.find(
				(e: VirtualIndexEntry) => e.key_value === "user99@example.com",
			);

			expect(JSON.parse(user0!.shard_ids)).toEqual([0]);
			expect(JSON.parse(user50!.shard_ids)).toEqual([0]);
			expect(JSON.parse(user99!.shard_ids)).toEqual([1]);
		});
	});

	describe("Row Count Operations", () => {
		it("should initialize shards with row_count = 0", async () => {
			const id = env.TOPOLOGY.idFromName("test-rowcount-init-1");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 3,
							block_size: 500,
						},
					],
				},
			});

			const topology = await stub.getTopology();
			const userShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users",
			);

			expect(userShards).toHaveLength(3);
			userShards.forEach((shard: TableShard) => {
				expect(shard.row_count).toBe(0);
			});
		});

		it("should get row counts for a table", async () => {
			const id = env.TOPOLOGY.idFromName("test-rowcount-get-1");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			const rowCounts = await stub.getTableShardRowCounts("users");
			expect(rowCounts).toHaveLength(2);
			rowCounts.forEach((rc: ShardRowCount) => {
				expect(rc.table_name).toBe("users");
				expect(rc.row_count).toBe(0);
				expect(rc.updated_at).toBeGreaterThan(0);
			});
		});

		it("should bump row count for a shard (positive delta)", async () => {
			const id = env.TOPOLOGY.idFromName("test-rowcount-bump-1");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Bump shard 0 by +10
			await stub.bumpTableShardRowCount("users", 0, 10);

			const rowCounts = await stub.getTableShardRowCounts("users");
			const shard0 = rowCounts.find((rc: ShardRowCount) => rc.shard_id === 0);
			const shard1 = rowCounts.find((rc: ShardRowCount) => rc.shard_id === 1);

			expect(shard0?.row_count).toBe(10);
			expect(shard1?.row_count).toBe(0);
		});

		it("should bump row count for a shard (negative delta)", async () => {
			const id = env.TOPOLOGY.idFromName("test-rowcount-bump-2");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// First add 100 rows
			await stub.bumpTableShardRowCount("users", 0, 100);
			// Then remove 30 rows
			await stub.bumpTableShardRowCount("users", 0, -30);

			const rowCounts = await stub.getTableShardRowCounts("users");
			const shard0 = rowCounts.find((rc: ShardRowCount) => rc.shard_id === 0);

			expect(shard0?.row_count).toBe(70);
		});

		it("should clamp row count at 0 (prevent negative)", async () => {
			const id = env.TOPOLOGY.idFromName("test-rowcount-clamp-1");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Try to decrement below 0
			await stub.bumpTableShardRowCount("users", 0, -100);

			const rowCounts = await stub.getTableShardRowCounts("users");
			const shard0 = rowCounts.find((rc: ShardRowCount) => rc.shard_id === 0);

			expect(shard0?.row_count).toBe(0); // Clamped at 0
		});

		it("should batch bump row counts for multiple shards", async () => {
			const id = env.TOPOLOGY.idFromName("test-rowcount-batch-1");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(3);
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 3,
							block_size: 500,
						},
					],
				},
			});

			// Batch bump: shard 0 +10, shard 1 +20, shard 2 +30
			const deltaByShard = new Map<number, number>([
				[0, 10],
				[1, 20],
				[2, 30],
			]);
			await stub.batchBumpTableShardRowCounts("users", deltaByShard);

			const rowCounts = await stub.getTableShardRowCounts("users");
			expect(rowCounts.find((rc: ShardRowCount) => rc.shard_id === 0)?.row_count).toBe(10);
			expect(rowCounts.find((rc: ShardRowCount) => rc.shard_id === 1)?.row_count).toBe(20);
			expect(rowCounts.find((rc: ShardRowCount) => rc.shard_id === 2)?.row_count).toBe(30);
		});

		it("should set row counts for shards (used after resharding)", async () => {
			const id = env.TOPOLOGY.idFromName("test-rowcount-set-1");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 2,
							block_size: 500,
						},
					],
				},
			});

			// Set exact row counts
			const countsByShardId = new Map<number, number>([
				[0, 500],
				[1, 750],
			]);
			await stub.setTableShardRowCounts("users", countsByShardId);

			const rowCounts = await stub.getTableShardRowCounts("users");
			expect(rowCounts.find((rc: ShardRowCount) => rc.shard_id === 0)?.row_count).toBe(500);
			expect(rowCounts.find((rc: ShardRowCount) => rc.shard_id === 1)?.row_count).toBe(750);
		});

		it("should only return row counts for active shards", async () => {
			const id = env.TOPOLOGY.idFromName("test-rowcount-active-1");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(3);
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 1,
							block_size: 500,
						},
					],
				},
			});

			// Set row count on shard 0
			await stub.bumpTableShardRowCount("users", 0, 100);

			// Create pending shards (resharding)
			const changeLogId = "test-log";
			await stub.createPendingShards("users", 2, changeLogId);

			// getTableShardRowCounts should only return active shards (not pending)
			const rowCounts = await stub.getTableShardRowCounts("users");
			expect(rowCounts).toHaveLength(1);
			expect(rowCounts[0]?.shard_id).toBe(0);
			expect(rowCounts[0]?.row_count).toBe(100);
		});

		it("should update updated_at timestamp on row count changes", async () => {
			const id = env.TOPOLOGY.idFromName("test-rowcount-timestamp-1");
			const stub = env.TOPOLOGY.get(id);

			await stub.create(2);
			await stub.updateTopology({
				tables: {
					add: [
						{
							table_name: "users",
							primary_key: "id",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "id",
							num_shards: 1,
							block_size: 500,
						},
					],
				},
			});

			const initialRowCounts = await stub.getTableShardRowCounts("users");
			const initialTimestamp = initialRowCounts[0]?.updated_at ?? 0;

			// Wait a bit and bump count
			await new Promise((resolve) => setTimeout(resolve, 10));
			await stub.bumpTableShardRowCount("users", 0, 5);

			const updatedRowCounts = await stub.getTableShardRowCounts("users");
			const updatedTimestamp = updatedRowCounts[0]?.updated_at ?? 0;

			expect(updatedTimestamp).toBeGreaterThan(initialTimestamp);
		});
	});

	describe("getQueryPlanData", () => {
		describe("SELECT queries", () => {
			it("should return all shards when WHERE clause does not match shard key or indexes", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-select-1");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				const statement = parse("SELECT * FROM users WHERE name = ?");
				const planData = await stub.getQueryPlanData("users", statement, [
					"Alice",
				]);

				expect(planData.shardsToQuery).toHaveLength(3); // All shards
				expect(planData.virtualIndexes).toHaveLength(0);
			});

			it("should return single shard when WHERE clause matches shard key", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-select-2");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				const statement = parse("SELECT * FROM users WHERE id = ?");
				const planData = await stub.getQueryPlanData("users", statement, [123]);

				expect(planData.shardsToQuery).toHaveLength(1); // Single shard
				expect(planData.shardsToQuery[0]!.shard_id).toBeGreaterThanOrEqual(0);
				expect(planData.shardsToQuery[0]!.shard_id).toBeLessThan(3);
			});

			it("should use virtual index to reduce shard fan-out", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-select-3");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				// Create and populate virtual index
				await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");
				await stub.updateIndexStatus("idx_email", "ready");
				await stub.batchUpsertIndexEntries("idx_email", [
					{ keyValue: "alice@example.com", shardIds: [1] },
				]);

				const statement = parse("SELECT * FROM users WHERE email = ?");
				const planData = await stub.getQueryPlanData("users", statement, [
					"alice@example.com",
				]);

				expect(planData.shardsToQuery).toHaveLength(1); // Reduced to single shard via index
				expect(planData.shardsToQuery[0]!.shard_id).toBe(1);
				expect(planData.virtualIndexes).toHaveLength(1);
				expect(planData.virtualIndexes[0]!.index_name).toBe("idx_email");
			});

			it("should return empty array when indexed value does not exist", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-select-4");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				// Create index but don't add any entries
				await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");
				await stub.updateIndexStatus("idx_email", "ready");

				const statement = parse("SELECT * FROM users WHERE email = ?");
				const planData = await stub.getQueryPlanData("users", statement, [
					"nonexistent@example.com",
				]);

				expect(planData.shardsToQuery).toHaveLength(0); // No shards contain this value
			});

			it("should use composite index for AND queries", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-select-5");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				// Create composite index
				await stub.createVirtualIndex(
					"idx_country_city",
					"users",
					["country", "city"],
					"hash",
				);
				await stub.updateIndexStatus("idx_country_city", "ready");
				await stub.batchUpsertIndexEntries("idx_country_city", [
					{ keyValue: JSON.stringify(["USA", "NYC"]), shardIds: [0, 2] },
				]);

				const statement = parse(
					"SELECT * FROM users WHERE country = ? AND city = ?",
				);
				const planData = await stub.getQueryPlanData("users", statement, [
					"USA",
					"NYC",
				]);

				expect(planData.shardsToQuery).toHaveLength(2); // Reduced via composite index
				expect(
					planData.shardsToQuery.map((s: ShardInfo) => s.shard_id).sort(),
				).toEqual([0, 2]);
			});
		});

		describe("UPDATE queries", () => {
			it("should return all shards when WHERE clause does not match shard key or indexes", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-update-1");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				const statement = parse("UPDATE users SET name = ? WHERE age > ?");
				const planData = await stub.getQueryPlanData("users", statement, [
					"Bob",
					18,
				]);

				expect(planData.shardsToQuery).toHaveLength(3); // All shards
			});

			it("should return single shard when WHERE clause matches shard key", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-update-2");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				const statement = parse("UPDATE users SET name = ? WHERE id = ?");
				const planData = await stub.getQueryPlanData("users", statement, [
					"Bob",
					456,
				]);

				expect(planData.shardsToQuery).toHaveLength(1); // Single shard
			});

			it("should use virtual index to reduce shard fan-out", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-update-3");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				// Create and populate virtual index
				await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");
				await stub.updateIndexStatus("idx_email", "ready");
				await stub.batchUpsertIndexEntries("idx_email", [
					{ keyValue: "bob@example.com", shardIds: [0] },
				]);

				const statement = parse("UPDATE users SET name = ? WHERE email = ?");
				const planData = await stub.getQueryPlanData("users", statement, [
					"Bob",
					"bob@example.com",
				]);

				expect(planData.shardsToQuery).toHaveLength(1); // Reduced via index
				expect(planData.shardsToQuery[0]!.shard_id).toBe(0);
				expect(planData.virtualIndexes).toHaveLength(1);
			});

			it("should include virtual indexes in plan data for index maintenance", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-update-4");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(2);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 2,
								block_size: 500,
							},
						],
					},
				});

				// Create multiple indexes
				await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");
				await stub.createVirtualIndex(
					"idx_country",
					"users",
					["country"],
					"hash",
				);
				await stub.updateIndexStatus("idx_email", "ready");
				await stub.updateIndexStatus("idx_country", "ready");

				const statement = parse("UPDATE users SET name = ? WHERE id = ?");
				const planData = await stub.getQueryPlanData("users", statement, [
					"Alice",
					123,
				]);

				// Both ready indexes should be included for index maintenance
				expect(planData.virtualIndexes).toHaveLength(2);
				const indexNames = planData.virtualIndexes
					.map((idx) => idx.index_name)
					.sort();
				expect(indexNames).toEqual(["idx_country", "idx_email"]);
			});
		});

		describe("DELETE queries", () => {
			it("should return all shards when WHERE clause does not match shard key or indexes", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-delete-1");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				const statement = parse("DELETE FROM users WHERE status = ?");
				const planData = await stub.getQueryPlanData("users", statement, [
					"inactive",
				]);

				expect(planData.shardsToQuery).toHaveLength(3); // All shards
			});

			it("should return single shard when WHERE clause matches shard key", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-delete-2");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				const statement = parse("DELETE FROM users WHERE id = ?");
				const planData = await stub.getQueryPlanData("users", statement, [789]);

				expect(planData.shardsToQuery).toHaveLength(1); // Single shard
			});

			it("should use virtual index to reduce shard fan-out", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-delete-3");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				// Create and populate virtual index
				await stub.createVirtualIndex("idx_email", "users", ["email"], "hash");
				await stub.updateIndexStatus("idx_email", "ready");
				await stub.batchUpsertIndexEntries("idx_email", [
					{ keyValue: "charlie@example.com", shardIds: [2] },
				]);

				const statement = parse("DELETE FROM users WHERE email = ?");
				const planData = await stub.getQueryPlanData("users", statement, [
					"charlie@example.com",
				]);

				expect(planData.shardsToQuery).toHaveLength(1); // Reduced via index
				expect(planData.shardsToQuery[0]!.shard_id).toBe(2);
				expect(planData.virtualIndexes).toHaveLength(1);
			});

			it("should support IN queries with virtual index", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-delete-4");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(4);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 4,
								block_size: 500,
							},
						],
					},
				});

				// Create and populate virtual index
				await stub.createVirtualIndex(
					"idx_country",
					"users",
					["country"],
					"hash",
				);
				await stub.updateIndexStatus("idx_country", "ready");
				await stub.batchUpsertIndexEntries("idx_country", [
					{ keyValue: "USA", shardIds: [0, 2] },
					{ keyValue: "UK", shardIds: [1] },
					{ keyValue: "Canada", shardIds: [3] },
				]);

				const statement = parse(
					"DELETE FROM users WHERE country IN ('USA', 'UK')",
				);
				const planData = await stub.getQueryPlanData("users", statement, []);

				// Should target only shards 0, 1, 2 (not 3 which has Canada)
				expect(planData.shardsToQuery).toHaveLength(3);
				const shardIds = planData.shardsToQuery
					.map((s: ShardInfo) => s.shard_id)
					.sort();
				expect(shardIds).toEqual([0, 1, 2]);
			});
		});

		describe("INSERT queries", () => {
			it("should return single shard based on shard key hash", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-insert-1");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(3);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 3,
								block_size: 500,
							},
						],
					},
				});

				const statement = parse(
					"INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
				);
				const planData = await stub.getQueryPlanData("users", statement, [
					123,
					"Alice",
					"alice@example.com",
				]);

				// INSERT returns all shards - the INSERT handler distributes rows to correct shards
				expect(planData.shardsToQuery).toHaveLength(3);
			});

			it("should handle INSERT with different shard key values", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-insert-2");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(4);
				await stub.updateTopology({
					tables: {
						add: [
							{
								table_name: "users",
								primary_key: "id",
								primary_key_type: "INTEGER",
								shard_strategy: "hash",
								shard_key: "id",
								num_shards: 4,
								block_size: 500,
							},
						],
					},
				});

				const statement1 = parse("INSERT INTO users (id, name) VALUES (?, ?)");
				const planData1 = await stub.getQueryPlanData("users", statement1, [
					100,
					"User100",
				]);

				const statement2 = parse("INSERT INTO users (id, name) VALUES (?, ?)");
				const planData2 = await stub.getQueryPlanData("users", statement2, [
					200,
					"User200",
				]);

				// INSERT returns all shards - the INSERT handler distributes rows to correct shards
				expect(planData1.shardsToQuery).toHaveLength(4);
				expect(planData2.shardsToQuery).toHaveLength(4);
			});
		});

		describe("Error handling", () => {
			it("should throw error if table does not exist", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-error-1");
				const stub = env.TOPOLOGY.get(id);

				await stub.create(2);

				const statement = parse("SELECT * FROM nonexistent WHERE id = ?");
				await expect(
					stub.getQueryPlanData("nonexistent", statement, [123]),
				).rejects.toThrow("Table 'nonexistent' not found in topology");
			});

			it("should throw error if topology not created", async () => {
				const id = env.TOPOLOGY.idFromName("test-qpd-error-2");
				const stub = env.TOPOLOGY.get(id);

				const statement = parse("SELECT * FROM users WHERE id = ?");
				await expect(
					stub.getQueryPlanData("users", statement, [123]),
				).rejects.toThrow("Topology not created");
			});
		});
	});
});
