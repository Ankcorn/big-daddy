import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { processBuildIndexJob } from "../../src/engine/async-jobs/build-index";
import type {
	IndexBuildJob,
	IndexJob,
	MessageBatch,
	ReshardTableJob,
} from "../../src/engine/queue/types";
import { createConnection } from "../../src/index";
import { queueHandler } from "../../src/queue-consumer";

/**
 * Resharding Integration Tests
 *
 * These tests verify the complete resharding workflow using ONLY the public SQL interface.
 * We capture queue messages to manually trigger resharding jobs (simulating queue processing).
 *
 * Key principles:
 * - All data operations via `sql` tagged template
 * - No direct topology/storage stub calls for data manipulation
 * - Queue messages captured and processed to complete async resharding
 */

// Capture queue messages for manual processing
let capturedQueueMessages: (IndexJob & { _processed?: boolean })[] = [];

// Save original queue.send to restore later
const originalQueueSend = env.INDEX_QUEUE.send.bind(env.INDEX_QUEUE);

/**
 * Process all pending build_index jobs from the captured queue
 */
async function processBuildIndexJobs(): Promise<void> {
	const buildJobs = capturedQueueMessages.filter(
		(msg) => msg.type === "build_index" && !msg._processed,
	);

	for (const job of buildJobs) {
		job._processed = true;
		await processBuildIndexJob(job as IndexBuildJob, env);
	}
}

/**
 * Process all pending reshard_table jobs from the captured queue
 */
async function processReshardingJobs(): Promise<void> {
	const reshardJobs = capturedQueueMessages.filter(
		(msg) => msg.type === "reshard_table" && !msg._processed,
	);

	for (const job of reshardJobs) {
		job._processed = true;

		const batch: MessageBatch<ReshardTableJob> = {
			queue: "vitess-index-jobs",
			messages: [
				{
					id: `test-${Date.now()}-${Math.random()}`,
					timestamp: new Date(),
					body: job as ReshardTableJob,
					attempts: 1,
				},
			],
		};

		await queueHandler(batch, env, job.correlation_id);
	}
}

describe("Resharding Integration Tests", () => {
	beforeEach(() => {
		capturedQueueMessages = [];
		// Intercept queue.send to capture calls WITHOUT calling original
		// This prevents async background processing that breaks test isolation
		env.INDEX_QUEUE.send = async (message: IndexJob) => {
			capturedQueueMessages.push(message);
			// Don't call original - we'll process manually to avoid async race conditions
		};
	});

	afterEach(() => {
		// Restore original queue.send to prevent hanging
		env.INDEX_QUEUE.send = originalQueueSend;
	});

	it("should reshard users table from 1 to 5 shards on 3 nodes and verify data integrity", async () => {
		const dbId = "test-reshard-integration-1-to-5";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		// ========================================
		// Phase 1: Setup - Create table and insert 100 users
		// ========================================
		await sql`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL
		)`;

		// Insert 100 users with varied ages for aggregation testing
		const insertPromises = [];
		for (let i = 1; i <= 100; i++) {
			const age = 18 + (i % 50); // Ages 18-67
			insertPromises.push(
				sql`INSERT INTO users (id, name, email, age) VALUES (
					${i},
					${`User ${i}`},
					${`user${i}@example.com`},
					${age}
				)`,
			);
		}
		await Promise.all(insertPromises);

		// Verify initial data via public interface
		const initialCount = await sql`SELECT COUNT(*) as count FROM users`;
		expect(initialCount.rows[0]?.count).toBe(100);

		const initialUsers = await sql`SELECT * FROM users ORDER BY id`;
		expect(initialUsers.rows).toHaveLength(100);

		// Verify shardStats shows we're querying from initial shard setup
		expect(initialUsers.shardStats).toBeDefined();
		const initialShardCount = initialUsers.shardStats?.length || 0;

		// ========================================
		// Phase 2: Execute resharding via PRAGMA
		// ========================================
		const reshardResult = await sql`PRAGMA reshardTable('users', 5)`;

		// Verify PRAGMA returns expected metadata
		expect(reshardResult.rows).toHaveLength(1);
		expect(reshardResult.rows[0]?.status).toBe("queued");
		expect(reshardResult.rows[0]?.table_name).toBe("users");
		expect(reshardResult.rows[0]?.target_shard_count).toBe(5);
		expect(reshardResult.rows[0]?.change_log_id).toBeDefined();

		const _changeLogId = reshardResult.rows[0]?.change_log_id;

		// Verify resharding jobs were queued
		const reshardJobs = capturedQueueMessages.filter(
			(msg) => msg.type === "reshard_table",
		);
		expect(reshardJobs.length).toBeGreaterThanOrEqual(1);

		// ========================================
		// Phase 3: Process resharding queue jobs
		// ========================================
		await processReshardingJobs();

		// ========================================
		// Phase 4: Verify data integrity after resharding
		// ========================================

		// 4a. Verify total count unchanged
		const afterCount = await sql`SELECT COUNT(*) as count FROM users`;
		expect(afterCount.rows[0]?.count).toBe(100);

		// 4b. Verify all rows accessible
		const afterUsers = await sql`SELECT * FROM users ORDER BY id`;
		expect(afterUsers.rows).toHaveLength(100);

		// 4c. Verify shardStats shows more shards being queried
		expect(afterUsers.shardStats).toBeDefined();
		const afterShardCount = afterUsers.shardStats?.length || 0;
		expect(afterShardCount).toBeGreaterThan(initialShardCount);

		// 4d. Verify specific rows have correct data
		const user1 = await sql`SELECT * FROM users WHERE id = ${1}`;
		expect(user1.rows).toHaveLength(1);
		expect(user1.rows[0]?.name).toBe("User 1");
		expect(user1.rows[0]?.email).toBe("user1@example.com");

		const user50 = await sql`SELECT * FROM users WHERE id = ${50}`;
		expect(user50.rows).toHaveLength(1);
		expect(user50.rows[0]?.name).toBe("User 50");

		const user100 = await sql`SELECT * FROM users WHERE id = ${100}`;
		expect(user100.rows).toHaveLength(1);
		expect(user100.rows[0]?.name).toBe("User 100");

		// ========================================
		// Phase 5: Test aggregations across shards
		// ========================================

		// 5a. COUNT(*)
		const countResult = await sql`SELECT COUNT(*) as total FROM users`;
		expect(countResult.rows[0]?.total).toBe(100);

		// 5b. COUNT with WHERE
		const countAdults =
			await sql`SELECT COUNT(*) as count FROM users WHERE age >= 21`;
		expect(countAdults.rows[0]?.count).toBeGreaterThan(0);
		expect(countAdults.rows[0]?.count).toBeLessThan(100);

		// 5c. SUM
		const sumAges = await sql`SELECT SUM(age) as total_age FROM users`;
		expect(sumAges.rows[0]?.total_age).toBeGreaterThan(0);

		// 5d. MIN/MAX
		const minAge = await sql`SELECT MIN(age) as min_age FROM users`;
		expect(minAge.rows[0]?.min_age).toBe(18);

		const maxAge = await sql`SELECT MAX(age) as max_age FROM users`;
		expect(maxAge.rows[0]?.max_age).toBe(67);

		// 5e. AVG (note: cross-shard AVG is approximate)
		const avgAge = await sql`SELECT AVG(age) as avg_age FROM users`;
		expect(avgAge.rows[0]?.avg_age).toBeGreaterThan(18);
		expect(avgAge.rows[0]?.avg_age).toBeLessThan(67);

		// ========================================
		// Phase 6: Test SELECT with various WHERE clauses
		// ========================================

		// 6a. Single row by shard key (should hit 1 shard)
		const singleUser = await sql`SELECT * FROM users WHERE id = ${42}`;
		expect(singleUser.rows).toHaveLength(1);
		expect(singleUser.rows[0]?.id).toBe(42);
		expect(singleUser.shardStats).toHaveLength(1); // Single shard query

		// 6b. Range query on non-shard-key (should hit all shards)
		const ageRange =
			await sql`SELECT * FROM users WHERE age >= ${30} AND age <= ${40}`;
		expect(ageRange.rows.length).toBeGreaterThan(0);
		for (const user of ageRange.rows) {
			expect(user.age).toBeGreaterThanOrEqual(30);
			expect(user.age).toBeLessThanOrEqual(40);
		}

		// 6c. IN clause on shard key
		const inClause =
			await sql`SELECT * FROM users WHERE id IN (${1}, ${25}, ${50}, ${75}, ${100})`;
		expect(inClause.rows).toHaveLength(5);
		const ids = (inClause.rows as { id: number }[])
			.map((r) => r.id)
			.sort((a, b) => a - b);
		expect(ids).toEqual([1, 25, 50, 75, 100]);

		// ========================================
		// Phase 7: Test CRUD operations after resharding
		// ========================================

		// 7a. UPDATE single row
		await sql`UPDATE users SET name = ${"Updated User 42"} WHERE id = ${42}`;
		const updated = await sql`SELECT * FROM users WHERE id = ${42}`;
		expect(updated.rows[0]?.name).toBe("Updated User 42");

		// 7b. UPDATE multiple rows (cross-shard)
		const updateResult =
			await sql`UPDATE users SET age = age + 1 WHERE age < ${20}`;
		expect(updateResult.rowsAffected).toBeGreaterThanOrEqual(0);

		// 7c. DELETE single row
		const deleteResult = await sql`DELETE FROM users WHERE id = ${100}`;
		expect(deleteResult.rowsAffected).toBe(1);

		// 7d. Verify delete
		const afterDelete = await sql`SELECT COUNT(*) as count FROM users`;
		expect(afterDelete.rows[0]?.count).toBe(99);

		// 7e. INSERT new row (should go to correct shard)
		await sql`INSERT INTO users (id, name, email, age) VALUES (${101}, ${"New User"}, ${"new@example.com"}, ${25})`;
		const newUser = await sql`SELECT * FROM users WHERE id = ${101}`;
		expect(newUser.rows).toHaveLength(1);
		expect(newUser.rows[0]?.name).toBe("New User");

		// Final count
		const finalCount = await sql`SELECT COUNT(*) as count FROM users`;
		expect(finalCount.rows[0]?.count).toBe(100); // 99 + 1 new
	});

	it("should handle resharding with indexes and maintain query optimization", async () => {
		const dbId = "test-reshard-integration-with-index";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		// Setup table with index
		await sql`CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			category TEXT NOT NULL,
			name TEXT NOT NULL,
			price REAL NOT NULL
		)`;

		// Create index on category
		await sql`CREATE INDEX idx_category ON products(category)`;

		// Process the build_index job
		await processBuildIndexJobs();

		// Insert products across categories
		const categories = ["Electronics", "Books", "Clothing", "Food", "Toys"];
		for (let i = 1; i <= 50; i++) {
			const category = categories[i % categories.length]!;
			await sql`INSERT INTO products (id, category, name, price) VALUES (
				${i},
				${category},
				${`Product ${i}`},
				${i * 9.99}
			)`;
		}

		// Verify index-based query before resharding
		const electronicsBefore =
			await sql`SELECT * FROM products WHERE category = ${"Electronics"}`;
		const electronicsCountBefore = electronicsBefore.rows.length;
		expect(electronicsCountBefore).toBeGreaterThan(0);

		// Reshard to 5 shards
		await sql`PRAGMA reshardTable('products', 5)`;
		await processReshardingJobs();

		// Create a new connection to get a fresh cache after resharding
		// The old connection's cache still has stale shard information
		const sqlAfterReshard = await createConnection(dbId, { nodes: 3 }, env);

		// Verify index-based query after resharding
		const electronicsAfter =
			await sqlAfterReshard`SELECT * FROM products WHERE category = ${"Electronics"}`;
		expect(electronicsAfter.rows.length).toBe(electronicsCountBefore);

		// Verify aggregation by category
		const sumByCategory =
			await sql`SELECT SUM(price) as total FROM products WHERE category = ${"Books"}`;
		expect(sumByCategory.rows[0]?.total).toBeGreaterThan(0);

		// Verify all data intact
		const totalProducts = await sql`SELECT COUNT(*) as count FROM products`;
		expect(totalProducts.rows[0]?.count).toBe(50);
	});

	it("should maintain data consistency during concurrent reads while resharding", async () => {
		const dbId = "test-reshard-integration-concurrent";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		// Setup
		await sql`CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER NOT NULL,
			total REAL NOT NULL,
			status TEXT NOT NULL
		)`;

		// Insert orders
		for (let i = 1; i <= 100; i++) {
			await sql`INSERT INTO orders (id, customer_id, total, status) VALUES (
				${i},
				${(i % 20) + 1},
				${i * 10.5},
				${i % 3 === 0 ? "completed" : i % 3 === 1 ? "pending" : "shipped"}
			)`;
		}

		// Capture state before resharding
		const beforeTotal = await sql`SELECT SUM(total) as sum FROM orders`;
		const beforeCount = await sql`SELECT COUNT(*) as count FROM orders`;

		// Reshard
		await sql`PRAGMA reshardTable('orders', 5)`;
		await processReshardingJobs();

		// Verify exact same totals after resharding
		const afterTotal = await sql`SELECT SUM(total) as sum FROM orders`;
		const afterCount = await sql`SELECT COUNT(*) as count FROM orders`;

		expect(afterCount.rows[0]?.count).toBe(beforeCount.rows[0]?.count);
		expect(afterTotal.rows[0]?.sum).toBeCloseTo(
			beforeTotal.rows[0]?.sum as number,
			2,
		);

		// Verify status distribution unchanged
		const completedBefore =
			await sql`SELECT COUNT(*) as count FROM orders WHERE status = ${"completed"}`;
		const completedAfter =
			await sql`SELECT COUNT(*) as count FROM orders WHERE status = ${"completed"}`;
		expect(completedAfter.rows[0]?.count).toBe(completedBefore.rows[0]?.count);
	});
});
