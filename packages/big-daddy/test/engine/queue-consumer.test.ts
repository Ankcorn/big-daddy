import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type {
	MessageBatch,
	ReshardTableJob,
} from "../../src/engine/queue/types";
import type { TableShard } from "../../src/engine/topology/types";
import { createConnection } from "../../src/index";
import { queueHandler } from "../../src/queue-consumer";

/**
 * End-to-End Resharding Tests
 *
 * These tests verify the complete resharding workflow using the conductor
 * for data insertion, which is how data actually enters the system in production.
 * This ensures we catch real issues, not just isolated component bugs.
 */

/**
 * Helper function to manually process resharding queue jobs
 * In production, these would be processed by the queue consumer automatically,
 * but in tests we need to manually trigger them.
 */
async function processReshardingJob(
	_dbId: string,
	job: ReshardTableJob,
): Promise<void> {
	const batch: MessageBatch<ReshardTableJob> = {
		queue: "vitess-index-jobs",
		messages: [
			{
				id: `test-${Date.now()}-${Math.random()}`,
				timestamp: new Date(),
				body: job,
				attempts: 1,
			},
		],
	};

	await queueHandler(batch, env, job.correlation_id);
}

describe("E2E Resharding: Insert via Conductor + Verify", () => {
	it("should insert 100 users via conductor, then reshard 1→3 shards with correct verification", async () => {
		// Setup: Create topology and insert 100 users via conductor
		const dbId = "test-e2e-reshard-100";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		// Create users table via conductor
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`;

		// Insert 100 users via conductor (which handles _virtualShard internally)
		console.log("Inserting 100 users via conductor...");
		const insertPromises = [];
		for (let i = 1; i <= 100; i++) {
			insertPromises.push(
				sql`INSERT INTO users (id, email) VALUES (${i}, ${`user${i}@example.com`})`,
			);
		}
		await Promise.all(insertPromises);

		// Verify all 100 users are readable via conductor (reads should work across all shards)
		const allUsers = await sql`SELECT * FROM users`;
		console.log(`Total users via conductor query: ${allUsers.rows.length}`);
		expect(allUsers.rows).toHaveLength(100);

		// Verify specific users can be found
		const user1 = await sql`SELECT * FROM users WHERE id = 1`;
		expect(user1.rows).toHaveLength(1);
		expect(user1.rows[0]?.email).toBe("user1@example.com");

		const user100 = await sql`SELECT * FROM users WHERE id = 100`;
		expect(user100.rows).toHaveLength(1);
		expect(user100.rows[0]?.email).toBe("user100@example.com");

		// Test: Reshard from 1 → 3 shards
		console.log("Starting resharding from 1 to 3 shards...");
		await sql`PRAGMA reshardTable('users', 3)`;

		// Verify resharding job completed successfully
		const topologyId = env.TOPOLOGY.idFromName(dbId);
		const topologyStub = env.TOPOLOGY.get(topologyId);

		// Get the resharding job that was queued
		let reshardingState = await topologyStub.getReshardingState("users");
		console.log(`Initial resharding state:`, reshardingState);

		// Extract job details from the topology
		const topology = await topologyStub.getTopology();
		const userShards = topology.table_shards.filter(
			(s: TableShard) => s.table_name === "users",
		);
		const sourceShardId =
			userShards.find(
				(s: TableShard) => s.status === "active" && s.shard_id === 0,
			)?.shard_id || 0;
		const targetShardIds = userShards
			.filter((s: TableShard) => s.status === "pending")
			.map((s: TableShard) => s.shard_id);

		// Create and manually process the resharding job
		if (reshardingState && targetShardIds.length > 0) {
			const reshardJob: ReshardTableJob = {
				type: "reshard_table",
				database_id: dbId,
				table_name: "users",
				source_shard_id: sourceShardId,
				target_shard_ids: targetShardIds,
				shard_key: "id",
				shard_strategy: "hash",
				change_log_id: reshardingState.change_log_id || `reshard-${Date.now()}`,
				created_at: new Date().toISOString(),
				correlation_id: crypto.randomUUID(),
			};

			console.log("Manually processing resharding job...");
			await processReshardingJob(dbId, reshardJob);

			reshardingState = await topologyStub.getReshardingState("users");
			console.log(
				`Resharding job status after processing: ${reshardingState?.status}`,
			);
		}

		expect(reshardingState?.status).toBe("complete");
		expect(reshardingState?.error_message).toBeNull();

		// Verify topology shows new shards
		const topologyAfter = await topologyStub.getTopology();
		const userShardsAfterReshard = topologyAfter.table_shards.filter(
			(s: TableShard) => s.table_name === "users",
		);
		console.log(
			`Shard count after resharding: ${userShardsAfterReshard.length}`,
		);
		expect(userShardsAfterReshard.length).toBeGreaterThanOrEqual(4); // Original 1 + new 3

		// Verify the new shards were created and are active
		const newShards = userShardsAfterReshard.filter(
			(s: TableShard) => s.shard_id > 0 && s.status === "active",
		);
		console.log(`New active shards: ${newShards.length}`);
		expect(newShards.length).toBeGreaterThanOrEqual(3);

		// After resharding, verify all data is still accessible and correct
		const afterReshard = await sql`SELECT * FROM users`;
		console.log(`Total users after resharding: ${afterReshard.rows.length}`);
		expect(afterReshard.rows).toHaveLength(100);

		// Verify distribution across shards (sampling)
		const user50 = await sql`SELECT * FROM users WHERE id = 50`;
		expect(user50.rows).toHaveLength(1);
		expect(user50.rows[0]?.email).toBe("user50@example.com");

		const user75 = await sql`SELECT * FROM users WHERE id = 75`;
		expect(user75.rows).toHaveLength(1);
		expect(user75.rows[0]?.email).toBe("user75@example.com");

		// Verify UPDATE works across new shards
		await sql`UPDATE users SET email = ${"updated@example.com"} WHERE id = 50`;
		const updated = await sql`SELECT * FROM users WHERE id = 50`;
		expect(updated.rows[0]?.email).toBe("updated@example.com");

		// Verify DELETE works across new shards
		const deleteResult = await sql`DELETE FROM users WHERE id = 100`;
		expect(deleteResult.rowsAffected).toBe(1);

		const remaining = await sql`SELECT * FROM users`;
		expect(remaining.rows).toHaveLength(99);
	});

	it("should handle resharding from 1→5 shards with data integrity", async () => {
		const dbId = "test-e2e-reshard-1-to-5";
		const sql = await createConnection(dbId, { nodes: 5 }, env);

		// Create and populate
		await sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL)`;

		console.log("Inserting 50 products via conductor...");
		for (let i = 1; i <= 50; i++) {
			await sql`INSERT INTO products (id, name, price) VALUES (${i}, ${`Product ${i}`}, ${i * 9.99})`;
		}

		// Verify initial state
		let products = await sql`SELECT * FROM products`;
		expect(products.rows).toHaveLength(50);

		// Reshard
		console.log("Resharding from 1 to 5 shards...");
		await sql`PRAGMA reshardTable('products', 5)`;

		// Verify resharding job completed successfully
		const topologyId = env.TOPOLOGY.idFromName(dbId);
		const topologyStub = env.TOPOLOGY.get(topologyId);

		// Get the resharding job that was queued
		let reshardingState = await topologyStub.getReshardingState("products");
		console.log(`Initial resharding state:`, reshardingState);

		// Extract job details from the topology
		const topology = await topologyStub.getTopology();
		const productShards = topology.table_shards.filter(
			(s: TableShard) => s.table_name === "products",
		);
		const sourceShardId =
			productShards.find(
				(s: TableShard) => s.status === "active" && s.shard_id === 0,
			)?.shard_id || 0;
		const targetShardIds = productShards
			.filter((s: TableShard) => s.status === "pending")
			.map((s: TableShard) => s.shard_id);

		// Create and manually process the resharding job
		if (reshardingState && targetShardIds.length > 0) {
			const reshardJob: ReshardTableJob = {
				type: "reshard_table",
				database_id: dbId,
				table_name: "products",
				source_shard_id: sourceShardId,
				target_shard_ids: targetShardIds,
				shard_key: "id",
				shard_strategy: "hash",
				change_log_id: reshardingState.change_log_id || `reshard-${Date.now()}`,
				created_at: new Date().toISOString(),
				correlation_id: crypto.randomUUID(),
			};

			console.log("Manually processing resharding job...");
			await processReshardingJob(dbId, reshardJob);

			reshardingState = await topologyStub.getReshardingState("products");
			console.log(
				`Resharding job status after processing: ${reshardingState?.status}`,
			);
		}

		expect(reshardingState?.status).toBe("complete");
		expect(reshardingState?.error_message).toBeNull();

		// Verify topology shows new shards
		const topologyAfter = await topologyStub.getTopology();
		const productShardsAfterReshard = topologyAfter.table_shards.filter(
			(s: TableShard) => s.table_name === "products",
		);
		console.log(
			`Shard count after resharding: ${productShardsAfterReshard.length}`,
		);
		expect(productShardsAfterReshard.length).toBeGreaterThanOrEqual(6); // Original 1 + new 5

		// Verify the new shards were created and are active
		const newShardsProducts = productShardsAfterReshard.filter(
			(s: TableShard) => s.shard_id > 0 && s.status === "active",
		);
		console.log(`New active shards: ${newShardsProducts.length}`);
		expect(newShardsProducts.length).toBeGreaterThanOrEqual(5);

		// Verify post-reshard
		products = await sql`SELECT * FROM products`;
		expect(products.rows).toHaveLength(50);

		// Verify data integrity
		const product1 = await sql`SELECT * FROM products WHERE id = 1`;
		expect(product1.rows[0]?.name).toBe("Product 1");
		expect(product1.rows[0]?.price).toBeCloseTo(9.99);

		const product50 = await sql`SELECT * FROM products WHERE id = 50`;
		expect(product50.rows[0]?.name).toBe("Product 50");
		expect(product50.rows[0]?.price).toBeCloseTo(499.5);

		// Verify operations still work
		await sql`UPDATE products SET price = ${199.99} WHERE id = 20`;
		const updated = await sql`SELECT * FROM products WHERE id = 20`;
		expect(updated.rows[0]?.price).toBeCloseTo(199.99);
	});

	it("should handle SELECT with WHERE clause after resharding", async () => {
		const dbId = "test-e2e-reshard-with-where";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER NOT NULL)`;

		// Insert 50 users with varying ages
		console.log("Inserting 50 users with ages...");
		for (let i = 1; i <= 50; i++) {
			const age = 20 + (i % 50);
			await sql`INSERT INTO users (id, name, age) VALUES (${i}, ${`User ${i}`}, ${age})`;
		}

		// Test WHERE before resharding
		let adults = await sql`SELECT * FROM users WHERE age >= 25`;
		const countBefore = adults.rows.length;
		console.log(`Users with age >= 25 before resharding: ${countBefore}`);
		expect(countBefore).toBeGreaterThan(0);

		// Reshard
		console.log("Resharding to 3 shards...");
		await sql`PRAGMA reshardTable('users', 3)`;

		// Verify resharding job completed successfully
		const topologyId = env.TOPOLOGY.idFromName(dbId);
		const topologyStub = env.TOPOLOGY.get(topologyId);

		// Get the resharding job that was queued
		let reshardingState = await topologyStub.getReshardingState("users");
		console.log(`Initial resharding state:`, reshardingState);

		// Extract job details from the topology
		const topology = await topologyStub.getTopology();
		const userShards = topology.table_shards.filter(
			(s: TableShard) => s.table_name === "users",
		);
		const sourceShardId =
			userShards.find(
				(s: TableShard) => s.status === "active" && s.shard_id === 0,
			)?.shard_id || 0;
		const targetShardIds = userShards
			.filter((s: TableShard) => s.status === "pending")
			.map((s: TableShard) => s.shard_id);

		// Create and manually process the resharding job
		if (reshardingState && targetShardIds.length > 0) {
			const reshardJob: ReshardTableJob = {
				type: "reshard_table",
				database_id: dbId,
				table_name: "users",
				source_shard_id: sourceShardId,
				target_shard_ids: targetShardIds,
				shard_key: "id",
				shard_strategy: "hash",
				change_log_id: reshardingState.change_log_id || `reshard-${Date.now()}`,
				created_at: new Date().toISOString(),
				correlation_id: crypto.randomUUID(),
			};

			console.log("Manually processing resharding job...");
			await processReshardingJob(dbId, reshardJob);

			reshardingState = await topologyStub.getReshardingState("users");
			console.log(
				`Resharding job status after processing: ${reshardingState?.status}`,
			);
		}

		expect(reshardingState?.status).toBe("complete");
		expect(reshardingState?.error_message).toBeNull();

		// Verify topology shows new shards
		const topologyAfter = await topologyStub.getTopology();
		const userShardsAfter = topologyAfter.table_shards.filter(
			(s: TableShard) => s.table_name === "users",
		);
		console.log(`Shard count after resharding: ${userShardsAfter.length}`);
		expect(userShardsAfter.length).toBeGreaterThanOrEqual(4); // Original 1 + new 3

		// Verify the new shards were created and are active
		const newShards = userShardsAfter.filter(
			(s: TableShard) => s.shard_id > 0 && s.status === "active",
		);
		console.log(`New active shards: ${newShards.length}`);
		expect(newShards.length).toBeGreaterThanOrEqual(3);

		// Test WHERE after resharding - CRITICAL TEST
		adults = await sql`SELECT * FROM users WHERE age >= 25`;
		console.log(`Users with age >= 25 after resharding: ${adults.rows.length}`);
		expect(adults.rows.length).toBe(countBefore);

		// Verify all results have age >= 25
		for (const user of adults.rows) {
			expect(user.age).toBeGreaterThanOrEqual(25);
		}

		// Test more complex WHERE
		const specific =
			await sql`SELECT * FROM users WHERE age = ${25} AND id > ${10}`;
		console.log(`Users with age=25 and id>10: ${specific.rows.length}`);
		for (const user of specific.rows) {
			expect(user.age).toBe(25);
			expect(user.id).toBeGreaterThan(10);
		}
	});
});
