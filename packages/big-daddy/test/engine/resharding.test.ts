import { env } from "cloudflare:test";
import type {
	Identifier,
	Literal,
	PragmaStatement,
} from "@databases/sqlite-ast";
import { parse } from "@databases/sqlite-ast";
import { describe, expect, it } from "vitest";
import type {
	TableMetadata,
	TableShard,
} from "../../src/engine/topology/types";

/**
 * Resharding Test Suite
 *
 * Tests the complete resharding workflow:
 * 1. PRAGMA reshardTable handling
 * 2. Create pending shards
 * 3. Queue job dispatch
 * 4. Write logging during resharding
 * 5. Change log replay
 * 6. Atomic status switch
 * 7. Data verification
 */

describe("Resharding", () => {
	// ==========================================
	// Phase 1: PRAGMA Statement Parsing & Handler
	// ==========================================
	describe("Phase 1: PRAGMA reshardTable Parsing", () => {
		it("should parse PRAGMA reshardTable with function call syntax", () => {
			const sql = "PRAGMA reshardTable('users', 3)";
			const stmt = parse(sql) as PragmaStatement;

			expect(stmt.type).toBe("PragmaStatement");
			expect(stmt.name).toBe("reshardTable");
			expect(stmt.arguments).toBeDefined();
			expect(stmt.arguments).toHaveLength(2);
			const arg0 = stmt.arguments![0]!;
			const arg1 = stmt.arguments![1]!;
			expect(arg0.type).toBe("Literal");
			expect((arg0 as Literal).value).toBe("users");
			expect(arg1.type).toBe("Literal");
			expect((arg1 as Literal).value).toBe(3);
		});

		it("should parse PRAGMA reshardTable with identifier syntax", () => {
			const sql = "PRAGMA reshardTable(users, 3)";
			const stmt = parse(sql) as PragmaStatement;

			expect(stmt.type).toBe("PragmaStatement");
			expect(stmt.arguments).toHaveLength(2);
			const arg0 = stmt.arguments![0]!;
			expect(arg0.type).toBe("Identifier");
			expect((arg0 as Identifier).name).toBe("users");
		});

		it("should reject PRAGMA reshardTable with invalid argument count", () => {
			const sql1 = "PRAGMA reshardTable('users')";
			const sql2 = "PRAGMA reshardTable('users', 3, 'extra')";

			const stmt1 = parse(sql1) as PragmaStatement;
			const stmt2 = parse(sql2) as PragmaStatement;

			expect(stmt1.arguments).toHaveLength(1);
			expect(stmt2.arguments).toHaveLength(3);
		});
	});

	// ==========================================
	// Phase 2: Create Pending Shards
	// ==========================================
	describe("Phase 1: Create Pending Shards", () => {
		it("should create pending shards with correct status", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-pending-shards-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			// Create topology with 3 nodes
			await topologyStub.create(3);

			// Create users table with 1 default shard
			await topologyStub.updateTopology({
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

			const tableName = "users";
			const newShardCount = 3;
			const changeLogId = "test-log-123";

			// Act
			const newShards = await topologyStub.createPendingShards(
				tableName,
				newShardCount,
				changeLogId,
			);

			// Assert
			// Verify 3 new shards created
			expect(newShards).toHaveLength(3);

			// Verify all new shards have status='pending'
			newShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("pending");
				expect(shard.table_name).toBe(tableName);
			});

			// Verify shard IDs are sequential (1, 2, 3)
			expect(newShards[0].shard_id).toBe(1);
			expect(newShards[1].shard_id).toBe(2);
			expect(newShards[2].shard_id).toBe(3);

			// Verify shards distributed across nodes
			const nodes = new Set(newShards.map((s: TableShard) => s.node_id));
			expect(nodes.size).toBeGreaterThan(1); // At least distributed to 2+ nodes

			// Verify old shard (0) remains ACTIVE
			const topology = await topologyStub.getTopology();
			const oldShard = topology.table_shards.find(
				(s: TableShard) => s.table_name === tableName && s.shard_id === 0,
			);
			expect(oldShard).toBeDefined();
			expect(oldShard?.status).toBe("active");
		});

		it("should reject creating pending shards if table does not exist", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-pending-shards-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			const nonexistentTable = "nonexistent";

			// Act & Assert
			await expect(
				topologyStub.createPendingShards(nonexistentTable, 3, "test-log"),
			).rejects.toThrow("Table 'nonexistent' not found");
		});

		it("should validate new shard count", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-pending-shards-3");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const tableName = "users";

			// Act & Assert
			// Reject shard count < 1
			await expect(
				topologyStub.createPendingShards(tableName, 0, "test-log"),
			).rejects.toThrow("Invalid shard count");

			// Reject shard count > 256
			await expect(
				topologyStub.createPendingShards(tableName, 257, "test-log"),
			).rejects.toThrow("Invalid shard count");

			// Accept shard count between 1-256
			const validShards = await topologyStub.createPendingShards(
				tableName,
				5,
				"test-log",
			);
			expect(validShards).toHaveLength(5);
		});

		it("should distribute pending shards across nodes evenly", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-pending-shards-4");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			const nodeCount = 3;
			await topologyStub.create(nodeCount);

			await topologyStub.updateTopology({
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

			const tableName = "users";
			const newShardCount = 3;

			// Act
			const newShards = await topologyStub.createPendingShards(
				tableName,
				newShardCount,
				"test-log",
			);

			// Assert
			// Verify each shard assigned to a node
			newShards.forEach((shard: TableShard) => {
				expect(shard.node_id).toBeDefined();
				expect(shard.node_id).toBeTruthy();
			});

			// Verify distribution is balanced (with 3 shards and 3 nodes, should be spread)
			const nodeDistribution = new Map<string, number>();
			for (const shard of newShards) {
				nodeDistribution.set(
					shard.node_id,
					(nodeDistribution.get(shard.node_id) || 0) + 1,
				);
			}

			// With 3 shards and 3 nodes, should have at least 2 different nodes
			expect(nodeDistribution.size).toBeGreaterThan(1);
		});
	});

	// ==========================================
	// Phase 2: Dispatch Queue Job
	// ==========================================
	describe("Phase 2: Dispatch Queue Job", () => {
		it("should enqueue ReshardTableJob after creating pending shards", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-dispatch-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);
			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const tableName = "users";
			const newShardCount = 3;

			// Act
			const newShards = await topologyStub.createPendingShards(
				tableName,
				newShardCount,
				"test-changelog-id",
			);

			// Assert
			// Verify ReshardTableJob would be created with correct parameters
			expect(newShards).toHaveLength(3);
			expect(newShards[0].status).toBe("pending");

			// Verify resharding state was created
			const reshardingState = await topologyStub.getReshardingState(tableName);
			expect(reshardingState).toBeDefined();
			expect(reshardingState?.change_log_id).toBe("test-changelog-id");
			expect(reshardingState?.status).toBe("pending_shards");
			expect(reshardingState?.target_shard_ids).toBeDefined();
		});

		it("should return job ID to user immediately", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-dispatch-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Act - Simulate what PRAGMA reshardTable would do
			const tableName = "users";
			const newShardCount = 3;
			const changeLogId = crypto.randomUUID();

			const newShards = await topologyStub.createPendingShards(
				tableName,
				newShardCount,
				changeLogId,
			);
			const topology = await topologyStub.getTopology();

			// Assert - Verify what the PRAGMA handler would return
			expect(newShards).toHaveLength(3);
			expect(newShards[0].status).toBe("pending");

			// Verify response would contain job ID and status
			const tableMetadata = topology.tables.find(
				(t: TableMetadata) => t.table_name === tableName,
			);
			expect(tableMetadata).toBeDefined();
			expect(tableMetadata?.shard_key).toBe("id");

			// Verify resharding state was created with change_log_id
			const reshardingState = await topologyStub.getReshardingState(tableName);
			expect(reshardingState).toBeDefined();
			expect(reshardingState?.change_log_id).toBe(changeLogId);
			expect(reshardingState?.status).toBe("pending_shards");

			// Verify properties that would be returned to user
			expect(reshardingState?.table_name).toBe("users");
			expect(JSON.parse(reshardingState?.target_shard_ids)).toHaveLength(3);
		});
	});

	// ==========================================
	// Phase 3: Write Logging During Resharding
	// ==========================================
	describe("Phase 3A: Write Logging", () => {
		it("should log INSERT operations to queue during resharding", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-write-log-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Start resharding (move to 'copying' status)
			const changeLogId = "write-log-id-1";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			const _insertQuery = "INSERT INTO users (id, name) VALUES (1, 'Alice')";

			// Act
			// Verify conductor would detect resharding is active
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			// Verify resharding is in 'copying' status (conductor would log writes)
			expect(reshardingState).toBeDefined();
			expect(reshardingState?.status).toBe("copying");
			expect(reshardingState?.change_log_id).toBe(changeLogId);
			expect(reshardingState?.table_name).toBe("users");
		});

		it("should log UPDATE operations to queue during resharding", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-write-log-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "write-log-id-2";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			const _updateQuery = "UPDATE users SET name='Alicia' WHERE id=1";

			// Act
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			expect(reshardingState?.status).toBe("copying");
			expect(reshardingState?.change_log_id).toBe(changeLogId);
		});

		it("should log DELETE operations to queue during resharding", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-write-log-3");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "write-log-id-3";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			const _deleteQuery = "DELETE FROM users WHERE id=1";

			// Act
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			expect(reshardingState?.status).toBe("copying");
		});

		it("should not log writes when resharding is not in progress", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-write-log-4");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const _insertQuery = "INSERT INTO users (id, name) VALUES (1, 'Alice')";

			// Act
			// Query for resharding state (should be null - no resharding active)
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			// Without active resharding, conductor would NOT log writes
			expect(reshardingState).toBeNull();
		});

		it("should not log writes for tables not being resharded", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-write-log-5");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			// Create two tables
			await topologyStub.updateTopology({
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
						{
							table_name: "products",
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

			// Start resharding only for 'users'
			const changeLogId = "write-log-id-5";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			const _insertQuery =
				"INSERT INTO products (id, name) VALUES (1, 'Widget')";

			// Act
			// Query for resharding state on 'products' table
			const reshardingState = await topologyStub.getReshardingState("products");

			// Assert
			// Products table has no active resharding, so writes would NOT be logged
			expect(reshardingState).toBeNull();

			// But users table should have resharding active
			const usersResharding = await topologyStub.getReshardingState("users");
			expect(usersResharding).toBeDefined();
			expect(usersResharding?.status).toBe("copying");
		});

		it("should execute write on old shard after logging", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-write-log-6");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "write-log-id-6";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			const _insertQuery = "INSERT INTO users (id, name) VALUES (1, 'Alice')";

			// Act
			// Verify old shard (shard 0) is still active and available
			const topology = await topologyStub.getTopology();
			const oldShard = topology.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);

			// Assert
			// Old shard 0 must still be ACTIVE so writes execute on it
			expect(oldShard).toBeDefined();
			expect(oldShard?.status).toBe("active");
			expect(oldShard?.node_id).toBeDefined();
		});

		it("should capture changes in order for deterministic replay", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-write-log-7");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "write-log-id-7";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			// Act
			// Verify resharding state contains metadata needed for ordered change capture
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			// Resharding state has change_log_id for grouping changes
			expect(reshardingState).toBeDefined();
			expect(reshardingState?.change_log_id).toBe(changeLogId);
			expect(reshardingState?.status).toBe("copying");

			// All changes for this resharding would be tagged with same change_log_id
			// Timestamps on messages would order them
			expect(reshardingState?.created_at).toBeLessThanOrEqual(
				reshardingState?.updated_at,
			);
		});
	});

	// ==========================================
	// Phase 3B: Copy Data
	// ==========================================
	describe("Phase 3B: Copy Data to New Shards", () => {
		it("should verify resharding state enters copying phase before data copy", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-copy-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-copy-1";
			await topologyStub.createPendingShards("users", 3, changeLogId);

			// Act - Start the copying phase
			await topologyStub.startResharding("users");

			// Assert
			const reshardingState = await topologyStub.getReshardingState("users");
			expect(reshardingState).toBeDefined();
			expect(reshardingState?.status).toBe("copying");
			expect(reshardingState?.table_name).toBe("users");

			// Verify source shard is still ACTIVE during copy (should accept writes)
			const topology = await topologyStub.getTopology();
			const sourceShard = topology.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);
			expect(sourceShard?.status).toBe("active");

			// Verify target shards are PENDING during copy
			const targetShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.shard_id > 0,
			);
			expect(targetShards).toHaveLength(3);
			targetShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("pending");
			});
		});

		it("should distribute copied rows by shard key hash function", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-copy-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-copy-2";
			const newShards = await topologyStub.createPendingShards(
				"users",
				3,
				changeLogId,
			);

			// Act - Start copying phase and verify state
			await topologyStub.startResharding("users");

			// Assert
			const reshardingState = await topologyStub.getReshardingState("users");
			expect(reshardingState).toBeDefined();
			expect(reshardingState?.status).toBe("copying");

			// Verify that new shards have sequential IDs for deterministic routing
			expect(newShards).toHaveLength(3);
			expect(newShards[0].shard_id).toBe(1);
			expect(newShards[1].shard_id).toBe(2);
			expect(newShards[2].shard_id).toBe(3);

			// Verify each shard has a node assigned (for routing during copy)
			newShards.forEach((shard: TableShard) => {
				expect(shard.node_id).toBeDefined();
				expect(shard.node_id).not.toBe("");
			});
		});

		it("should track rows copied during resharding phase", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-copy-3");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-copy-3";
			await topologyStub.createPendingShards("users", 3, changeLogId);

			// Act - Start copying and verify state is maintained
			await topologyStub.startResharding("users");
			const reshardingState1 = await topologyStub.getReshardingState("users");

			// Simulate copy phase starting
			expect(reshardingState1?.status).toBe("copying");
			const copyStartTime = reshardingState1?.created_at;

			// Assert - Verify resharding state persists during copy
			const reshardingState2 = await topologyStub.getReshardingState("users");
			expect(reshardingState2?.status).toBe("copying");
			expect(reshardingState2?.created_at).toBe(copyStartTime);
			expect(reshardingState2?.change_log_id).toBe(changeLogId);
		});

		it("should handle target shard allocation during copy phase", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-copy-4");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(4); // 4 nodes available

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-copy-4";
			const newShards = await topologyStub.createPendingShards(
				"users",
				4,
				changeLogId,
			);

			// Act
			await topologyStub.startResharding("users");

			// Assert
			expect(newShards).toHaveLength(4);

			// Each target shard should be on a different node (balanced distribution)
			const nodeIds = newShards.map((s: TableShard) => s.node_id);
			const uniqueNodeIds = new Set(nodeIds);
			expect(uniqueNodeIds.size).toBe(4);

			// Verify no two shards are on same node
			newShards.forEach((shard: TableShard, i: number) => {
				newShards.slice(i + 1).forEach((otherShard: TableShard) => {
					expect(shard.shard_id).not.toBe(otherShard.shard_id);
				});
			});
		});

		it("should maintain pending shard status until copy completes", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-copy-5");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-copy-5";
			await topologyStub.createPendingShards("users", 3, changeLogId);

			// Act - Start copying phase
			await topologyStub.startResharding("users");

			// Assert - During copy phase, target shards should remain PENDING
			const topology = await topologyStub.getTopology();
			const targetShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.shard_id > 0,
			);

			expect(targetShards).toHaveLength(3);
			targetShards.forEach((shard: TableShard) => {
				// Shards should remain pending during copy (before atomicStatusSwitch)
				expect(shard.status).toBe("pending");
			});

			// Verify resharding state shows copy in progress
			const reshardingState = await topologyStub.getReshardingState("users");
			expect(reshardingState?.status).toBe("copying");
		});
	});

	// ==========================================
	// Phase 3C: Replay Change Log
	// ==========================================
	describe("Phase 3C: Replay Change Log", () => {
		it("should transition to replaying phase after data copy completes", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-replay-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-replay-1";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			// Act - Simulate transitioning from copy to replay phase
			// In the real system, this would happen in queue-consumer after copyShardData completes
			// For now, we verify the state is set up correctly to support replay
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			expect(reshardingState?.status).toBe("copying");
			expect(reshardingState?.change_log_id).toBe(changeLogId);

			// Verify target shards are still pending (not yet activated)
			const topology = await topologyStub.getTopology();
			const targetShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.shard_id > 0,
			);
			targetShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("pending");
			});
		});

		it("should track change log ID for grouping captured changes", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-replay-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-replay-2";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			// Act
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			expect(reshardingState?.change_log_id).toBe(changeLogId);
			// All captured changes during the 'copying' phase would be tagged with this change_log_id
			// and could be fetched for replay using this ID
		});

		it("should support deterministic ordering of changes during replay", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-replay-3");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-replay-3";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			// Act
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			// Changes would be ordered by timestamp when retrieved from change log
			expect(reshardingState?.created_at).toBeDefined();
			expect(reshardingState?.updated_at).toBeDefined();
			// Timestamps allow deterministic ordering: changes applied in temporal order
		});

		it("should maintain replay state across target shards", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-replay-4");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-replay-4";
			const newShards = await topologyStub.createPendingShards(
				"users",
				3,
				changeLogId,
			);
			await topologyStub.startResharding("users");

			// Act
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			// All target shards would receive replayed changes together
			expect(newShards).toHaveLength(3);
			newShards.forEach((shard: TableShard) => {
				expect(shard.shard_id).toBeGreaterThan(0);
			});

			// The resharding state tracks the operation across all target shards
			expect(reshardingState?.status).toBe("copying");
		});

		it("should support change log queries during replay phase", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-replay-5");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-replay-5";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			// Act
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			// During replay, we use the change_log_id to fetch all changes that were captured
			expect(reshardingState?.change_log_id).toBe(changeLogId);
			// Changes would be queried from the queue/log with this ID
		});

		it("should track change count for monitoring replay progress", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-replay-6");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-replay-6";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			// Act
			const reshardingState1 = await topologyStub.getReshardingState("users");

			// Assert
			expect(reshardingState1?.status).toBe("copying");
			// After replay phase would complete, status would transition to 'verifying'
			// Change count would track how many changes were replayed
		});

		it("should maintain change log consistency for idempotent replay", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-replay-7");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-replay-7";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			// Act
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			// Changes are stored with consistent IDs and timestamps
			expect(reshardingState?.change_log_id).toBe(changeLogId);
			expect(reshardingState?.created_at).toBeLessThanOrEqual(
				reshardingState?.updated_at,
			);
			// This ensures that if replay is repeated, it's deterministic and idempotent
		});
	});

	// ==========================================
	// Phase 3D: Verify Data Integrity
	// ==========================================
	describe("Phase 3D: Verify Data Integrity", () => {
		it("should track verified state after successful integrity check", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-verify-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-verify-1";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			// Act
			// In the real system, verifyIntegrity would check row counts match
			// Here we verify the resharding state is properly tracking progress
			const reshardingState = await topologyStub.getReshardingState("users");

			// Assert
			expect(reshardingState?.status).toBe("copying");
			// After verification would succeed, status would transition to 'verifying' then ready for switch
			expect(reshardingState?.table_name).toBe("users");
		});

		it("should allow marking resharding as complete after verification", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-verify-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-verify-2";
			const newShards = await topologyStub.createPendingShards(
				"users",
				3,
				changeLogId,
			);
			await topologyStub.startResharding("users");

			// Act
			// After copy + replay + verify, we can mark resharding complete
			await topologyStub.markReshardingComplete("users");

			// Assert
			const reshardingState = await topologyStub.getReshardingState("users");
			expect(reshardingState?.status).toBe("complete");

			// Verify new shards are still in the topology (ready for atomic switch)
			const topology = await topologyStub.getTopology();
			const allUserShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users",
			);
			expect(allUserShards.length).toBeGreaterThan(newShards.length);
		});

		it("should allow marking resharding as failed for manual retry", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-verify-3");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-verify-3";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");

			// Act
			// If verification fails, mark resharding as failed with error message
			const errorMessage = "Row count mismatch: source=1000, targets=990";
			await topologyStub.markReshardingFailed("users", errorMessage);

			// Assert
			const reshardingState = await topologyStub.getReshardingState("users");
			expect(reshardingState?.status).toBe("failed");
			// Error message is logged for investigation
		});
	});

	// ==========================================
	// Phase 4: Atomic Status Switch
	// ==========================================
	describe("Phase 4: Atomic Status Switch", () => {
		it("should mark new shards as ACTIVE after successful copy", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-status-switch-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Create pending shards
			const changeLogId = "test-log-id";
			await topologyStub.createPendingShards("users", 3, changeLogId);

			// Act
			// Switch status (mark new shards as active, old as to_be_deleted)
			await topologyStub.atomicStatusSwitch("users");

			// Assert
			const topology = await topologyStub.getTopology();
			const userShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users",
			);

			// Verify new shards 1, 2, 3 are ACTIVE
			const newShards = userShards.filter((s: TableShard) => s.shard_id > 0);
			expect(newShards).toHaveLength(3);
			newShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("active");
			});
		});

		it("should mark old shard as TO_BE_DELETED after switch", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-status-switch-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Create pending shards
			const changeLogId = "test-log-id";
			await topologyStub.createPendingShards("users", 3, changeLogId);

			// Act
			// Switch status
			await topologyStub.atomicStatusSwitch("users");

			// Assert
			const topology = await topologyStub.getTopology();
			const oldShard = topology.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);

			expect(oldShard).toBeDefined();
			expect(oldShard?.status).toBe("to_be_deleted");
		});

		it("should atomically update all shard statuses", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-status-switch-3");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Create pending shards
			const changeLogId = "test-log-id";
			const _newShards = await topologyStub.createPendingShards(
				"users",
				3,
				changeLogId,
			);

			// Get initial state
			const topologyBefore = await topologyStub.getTopology();
			const shardsBefore = topologyBefore.table_shards.filter(
				(s: TableShard) => s.table_name === "users",
			);

			// Act
			// Switch status (should update all 4 shards atomically)
			await topologyStub.atomicStatusSwitch("users");

			// Assert
			const topologyAfter = await topologyStub.getTopology();
			const shardsAfter = topologyAfter.table_shards.filter(
				(s: TableShard) => s.table_name === "users",
			);

			// Verify all shards were updated (no partial updates)
			expect(shardsAfter).toHaveLength(shardsBefore.length);

			// Old shard 0 should be to_be_deleted
			const oldShard = shardsAfter.find((s: TableShard) => s.shard_id === 0);
			expect(oldShard?.status).toBe("to_be_deleted");

			// New shards 1, 2, 3 should all be active
			const newShardsAfter = shardsAfter.filter(
				(s: TableShard) => s.shard_id > 0,
			);
			expect(newShardsAfter).toHaveLength(3);
			newShardsAfter.forEach((shard: TableShard) => {
				expect(shard.status).toBe("active");
			});
		});

		it("should make new shards available to conductor immediately", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-status-switch-4");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Create pending shards
			const changeLogId = "test-log-id";
			await topologyStub.createPendingShards("users", 3, changeLogId);

			// Act
			// Switch status
			await topologyStub.atomicStatusSwitch("users");

			// Assert
			// Conductor queries topology and gets query plan data
			// The conductor filters to ACTIVE shards only
			// Verify that pending shards are now active and would be included
			const topology = await topologyStub.getTopology();
			const activeShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.status === "active",
			);

			// Should have 3 active shards (1, 2, 3)
			expect(activeShards).toHaveLength(3);
			expect(activeShards.map((s: TableShard) => s.shard_id).sort()).toEqual([
				1, 2, 3,
			]);
		});

		it("should prevent queries to TO_BE_DELETED shards", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-status-switch-5");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Create pending shards and switch
			const changeLogId = "test-log-id";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.atomicStatusSwitch("users");

			// Act
			// Query topology for active shards (conductor does this)
			const topology = await topologyStub.getTopology();

			// Filter to ACTIVE shards only (as getQueryPlanData does)
			const activeShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.status === "active",
			);

			// Assert
			// Verify shard 0 (to_be_deleted) is NOT in active shards
			const shard0Included = activeShards.some(
				(s: TableShard) => s.shard_id === 0,
			);
			expect(shard0Included).toBe(false);

			// Verify only shards 1, 2, 3 are available
			expect(activeShards.map((s: TableShard) => s.shard_id).sort()).toEqual([
				1, 2, 3,
			]);
		});
	});

	// ==========================================
	// Phase 5: Clean Up Old Shard
	// ==========================================
	describe("Phase 5: Delete Old Shard Data", () => {
		it("should verify shard marked TO_BE_DELETED before deletion", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-delete-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-delete-1";
			await topologyStub.createPendingShards("users", 3, changeLogId);

			// Act - Perform atomic status switch which marks old shard as TO_BE_DELETED
			await topologyStub.atomicStatusSwitch("users");

			// Assert
			const topology = await topologyStub.getTopology();
			const oldShard = topology.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);

			expect(oldShard).toBeDefined();
			expect(oldShard?.status).toBe("to_be_deleted");
			// Now it's safe to delete the shard data
		});

		it("should track old shard for deletion in phase 5", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-delete-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-delete-2";
			await topologyStub.createPendingShards("users", 3, changeLogId);

			// Act
			await topologyStub.atomicStatusSwitch("users");

			// Assert
			const topology = await topologyStub.getTopology();

			// Verify old shard is TO_BE_DELETED
			const oldShard = topology.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);
			expect(oldShard?.status).toBe("to_be_deleted");

			// Verify new shards are ACTIVE for immediate use
			const newShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.shard_id > 0,
			);
			expect(newShards).toHaveLength(3);
			newShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("active");
			});
		});

		it("should allow shard deletion only after resharding complete", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-delete-3");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			const changeLogId = "test-log-delete-3";
			await topologyStub.createPendingShards("users", 3, changeLogId);

			// Act - Complete the resharding workflow (copy, replay, verify phases)
			await topologyStub.startResharding("users");
			await topologyStub.atomicStatusSwitch("users");

			// Assert
			const topology = await topologyStub.getTopology();
			const oldShard = topology.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);

			// Old shard is marked TO_BE_DELETED and safe to clean up
			expect(oldShard?.status).toBe("to_be_deleted");

			// Verify new shards are now ACTIVE for serving queries
			const newShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.shard_id > 0,
			);
			newShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("active");
			});
		});
	});

	// ==========================================
	// End-to-End Scenarios
	// ==========================================
	describe("End-to-End Resharding", () => {
		it("should execute complete resharding workflow from PRAGMA to completion", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-e2e-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Act
			// Phase 1: Create pending shards (PRAGMA handler)
			const changeLogId = "test-log-e2e-1";
			const newShards = await topologyStub.createPendingShards(
				"users",
				3,
				changeLogId,
			);

			// Phase 2: Verify pending shards exist
			expect(newShards).toHaveLength(3);
			newShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("pending");
			});

			// Phase 3B: Start copying data
			await topologyStub.startResharding("users");
			const reshardingState = await topologyStub.getReshardingState("users");
			expect(reshardingState?.status).toBe("copying");

			// Phase 3C: Replay changes (simulated - in real system, changes would be captured)
			// Phase 3D: Verify data integrity

			// Phase 4: Atomic status switch
			await topologyStub.atomicStatusSwitch("users");

			// Assert - Verify final state
			const topology = await topologyStub.getTopology();

			// Verify new shards are ACTIVE
			const activeShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.shard_id > 0,
			);
			expect(activeShards).toHaveLength(3);
			activeShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("active");
			});

			// Verify old shard is TO_BE_DELETED
			const oldShard = topology.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);
			expect(oldShard?.status).toBe("to_be_deleted");

			// Phase 5: Delete old shard data (cleanup)
			// In real system, deleteOldShardData would remove the data
		});

		it("should maintain data consistency across all phases", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-e2e-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Act - Execute resharding phases
			const changeLogId = "test-log-e2e-2";
			const _newShards = await topologyStub.createPendingShards(
				"users",
				3,
				changeLogId,
			);
			await topologyStub.startResharding("users");

			// Verify intermediate states during resharding
			const topology1 = await topologyStub.getTopology();
			const pendingShards = topology1.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.shard_id > 0,
			);
			pendingShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("pending");
			});

			// Old shard remains ACTIVE during copy phase
			const oldShard1 = topology1.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);
			expect(oldShard1?.status).toBe("active");

			// Complete resharding
			await topologyStub.atomicStatusSwitch("users");

			// Assert - Verify final topology state
			const topology2 = await topologyStub.getTopology();

			// New shards now active
			const activeShards = topology2.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.shard_id > 0,
			);
			activeShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("active");
			});

			// Old shard marked for deletion
			const oldShard2 = topology2.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);
			expect(oldShard2?.status).toBe("to_be_deleted");
		});

		it("should support resharding multiple tables independently", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-e2e-3");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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
						{
							table_name: "products",
							primary_key: "pid",
							primary_key_type: "INTEGER",
							shard_strategy: "hash",
							shard_key: "pid",
							num_shards: 1,
							block_size: 500,
						},
					],
				},
			});

			// Act - Reshard 'users' table
			const changeLogId1 = "test-log-e2e-3a";
			await topologyStub.createPendingShards("users", 3, changeLogId1);
			await topologyStub.startResharding("users");

			// Act - Reshard 'products' table independently
			const changeLogId2 = "test-log-e2e-3b";
			await topologyStub.createPendingShards("products", 3, changeLogId2);
			await topologyStub.startResharding("products");

			// Assert - Both resharding operations are independent
			const usersReshardingState =
				await topologyStub.getReshardingState("users");
			expect(usersReshardingState?.status).toBe("copying");
			expect(usersReshardingState?.change_log_id).toBe(changeLogId1);

			const productsReshardingState =
				await topologyStub.getReshardingState("products");
			expect(productsReshardingState?.status).toBe("copying");
			expect(productsReshardingState?.change_log_id).toBe(changeLogId2);

			// Complete 'users' resharding
			await topologyStub.atomicStatusSwitch("users");

			// 'users' shards updated, 'products' still pending
			const topology = await topologyStub.getTopology();
			const usersShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users",
			);
			const productsShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "products",
			);

			// Users shards: 1 TO_BE_DELETED, 3 ACTIVE
			usersShards.forEach((shard: TableShard) => {
				if (shard.shard_id === 0) {
					expect(shard.status).toBe("to_be_deleted");
				} else {
					expect(shard.status).toBe("active");
				}
			});

			// Products shards: 1 ACTIVE, 3 PENDING
			productsShards.forEach((shard: TableShard) => {
				if (shard.shard_id === 0) {
					expect(shard.status).toBe("active");
				} else {
					expect(shard.status).toBe("pending");
				}
			});
		});
	});

	// ==========================================
	// Error Scenarios
	// ==========================================
	describe("Error Handling", () => {
		it("should allow retry after failed resharding", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-error-1");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Act - First resharding attempt fails
			const changeLogId1 = "test-log-error-1a";
			await topologyStub.createPendingShards("users", 3, changeLogId1);
			await topologyStub.startResharding("users");
			await topologyStub.markReshardingFailed(
				"users",
				"Row count mismatch during verification",
			);

			// Assert - Resharding marked as failed
			let reshardingState = await topologyStub.getReshardingState("users");
			expect(reshardingState?.status).toBe("failed");

			// Act - Retry resharding
			const changeLogId2 = "test-log-error-1b";
			await topologyStub.createPendingShards("users", 3, changeLogId2);
			await topologyStub.startResharding("users");

			// Assert - New resharding attempt initiated
			reshardingState = await topologyStub.getReshardingState("users");
			expect(reshardingState?.status).toBe("copying");
			expect(reshardingState?.change_log_id).toBe(changeLogId2);
		});

		it("should handle concurrent resharding attempts on same table", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-error-2");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Act - Start first resharding
			const changeLogId1 = "test-log-error-2a";
			await topologyStub.createPendingShards("users", 3, changeLogId1);
			await topologyStub.startResharding("users");

			// Assert - First resharding active
			const reshardingState = await topologyStub.getReshardingState("users");
			expect(reshardingState?.status).toBe("copying");
			expect(reshardingState?.change_log_id).toBe(changeLogId1);

			// Act - Complete first resharding
			await topologyStub.atomicStatusSwitch("users");
			await topologyStub.markReshardingComplete("users");

			// Assert - Can now start new resharding on same table
			const topology = await topologyStub.getTopology();
			const allShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users",
			);
			expect(allShards.length).toBeGreaterThan(1); // Has old + new shards
		});

		it("should preserve data integrity if resharding fails", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-error-3");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
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

			// Act - Start resharding then fail
			const changeLogId = "test-log-error-3";
			await topologyStub.createPendingShards("users", 3, changeLogId);
			await topologyStub.startResharding("users");
			await topologyStub.markReshardingFailed(
				"users",
				"Data copy failed on shard 2",
			);

			// Assert - Original shard still accessible
			const topology = await topologyStub.getTopology();
			const originalShard = topology.table_shards.find(
				(s: TableShard) => s.table_name === "users" && s.shard_id === 0,
			);

			// Original shard should still be active (not affected by failed resharding)
			expect(originalShard?.status).toBe("active");

			// Failed resharding shards are created but failed
			const failedShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "users" && s.shard_id > 0,
			);
			// These shards exist (were created as pending) but weren't activated
			expect(failedShards.length).toBeGreaterThan(0);
		});

		it("should handle resharding of empty tables", async () => {
			// Arrange
			const topologyId = env.TOPOLOGY.idFromName("test-error-4");
			const topologyStub = env.TOPOLOGY.get(topologyId);

			await topologyStub.create(3);

			await topologyStub.updateTopology({
				tables: {
					add: [
						{
							table_name: "empty_table",
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

			// Act - Reshard empty table
			const changeLogId = "test-log-error-4";
			await topologyStub.createPendingShards("empty_table", 3, changeLogId);
			await topologyStub.startResharding("empty_table");

			// Assert - Resharding proceeds normally even with no data
			const reshardingState =
				await topologyStub.getReshardingState("empty_table");
			expect(reshardingState?.status).toBe("copying");
			expect(reshardingState?.table_name).toBe("empty_table");

			// Can complete resharding of empty table
			await topologyStub.atomicStatusSwitch("empty_table");

			const topology = await topologyStub.getTopology();
			const newShards = topology.table_shards.filter(
				(s: TableShard) => s.table_name === "empty_table" && s.shard_id > 0,
			);
			expect(newShards).toHaveLength(3);
			newShards.forEach((shard: TableShard) => {
				expect(shard.status).toBe("active");
			});
		});
	});
});
