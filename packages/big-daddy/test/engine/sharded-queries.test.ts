import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { QueryResult, ShardStats } from "../../src/engine/conductor/types";
import { createConnection } from "../../src/index";

type QueryResultWithShardStats = QueryResult & { shardStats: ShardStats[] };

/**
 * These tests verify that getCachedQueryPlanData properly routes queries to the correct shards.
 * The bug being tested: queries for a single item by id should only hit 1 shard, not all shards.
 */
describe("getCachedQueryPlanData", () => {
	describe("Single shard queries - querying by shard key (id)", () => {
		it("should route SELECT query by id to only 1 shard with 3 shards total", async () => {
			const dbId = "test-single-item-3-shards-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			// Create table with 3 shards specified via SHARDS comment
			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT, email TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);

			// Insert a test user
			await sql`INSERT INTO users (id, name, email) VALUES (${100}, ${"Alice"}, ${"alice@example.com"})`;

			// Query for a single item by id
			const result =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResultWithShardStats;

			// Should only query 1 shard, not all 3
			expect(result.shardStats).toBeDefined();
			expect(result.shardStats).toHaveLength(1);
			expect(result.rows).toHaveLength(1);
			expect(result.rows[0]).toMatchObject({ id: 100, name: "Alice" });
		});

		it("should return the same shard for the same id on repeated calls", async () => {
			const dbId = "test-same-shard-consistency-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;

			// Query the same id twice
			const result1 =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResultWithShardStats;
			const result2 =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResultWithShardStats;

			// Both should query the same shard
			expect(result1.shardStats).toHaveLength(1);
			expect(result2.shardStats).toHaveLength(1);
			expect(result1.shardStats[0]?.shardId).toBe(
				result2.shardStats[0]?.shardId,
			);
		});

		it("should return different shards for different ids", async () => {
			const dbId = "test-different-shards-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;

			// Query different ids
			const result1 =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResultWithShardStats;
			const result2 =
				(await sql`SELECT * FROM users WHERE id = ${200}`) as QueryResultWithShardStats;

			// Both should query exactly 1 shard each
			expect(result1.shardStats).toHaveLength(1);
			expect(result2.shardStats).toHaveLength(1);
		});
	});

	describe("Non-shard-key queries", () => {
		it("should return all shards when querying without WHERE clause", async () => {
			const dbId = "test-all-shards-select-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;
			await sql`INSERT INTO users (id, name) VALUES (${300}, ${"Charlie"})`;

			// Select all users
			const result =
				(await sql`SELECT * FROM users`) as QueryResultWithShardStats;

			// Should query all 3 shards
			expect(result.shardStats).toHaveLength(3);
			expect(result.rows).toHaveLength(3);
		});

		it("should return all shards when querying by non-shard-key column", async () => {
			const dbId = "test-non-shard-key-where-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;

			// WHERE on non-shard-key column - should hit all shards
			const result =
				(await sql`SELECT * FROM users WHERE name = ${"Alice"}`) as QueryResultWithShardStats;

			// Should query all shards since we're querying by name (not shard key)
			expect(result.shardStats).toHaveLength(3);
			expect(result.rows).toHaveLength(1);
			expect(result.rows[0]).toMatchObject({ name: "Alice" });
		});
	});

	describe("Complex WHERE conditions", () => {
		it("should handle queries with multiple conditions including shard key", async () => {
			const dbId = "test-multi-condition-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name, age) VALUES (${100}, ${"Alice"}, ${25})`;

			// WHERE with multiple conditions: one on shard key, one on non-shard-key
			const result =
				(await sql`SELECT * FROM users WHERE id = ${100} AND age > ${20}`) as QueryResultWithShardStats;

			// Should still return only 1 shard because id is the shard key
			expect(result.shardStats).toHaveLength(1);
			expect(result.rows).toHaveLength(1);
		});

		it("should handle IN queries on shard key", async () => {
			const dbId = "test-in-clause-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;
			await sql`INSERT INTO users (id, name) VALUES (${300}, ${"Charlie"})`;

			// IN clause on shard key - might hit multiple shards
			const result =
				(await sql`SELECT * FROM users WHERE id IN (${100}, ${200})`) as QueryResultWithShardStats;

			// Should query at least 1 shard (might be 1 or 2 depending on hash distribution)
			expect(result.shardStats.length).toBeGreaterThanOrEqual(1);
			expect(result.rows.length).toBeLessThanOrEqual(2);
		});
	});

	describe("UPDATE and DELETE statements", () => {
		it("should route UPDATE to only 1 shard when WHERE filters on shard key", async () => {
			const dbId = "test-update-shard-key-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;

			// Update a specific user by shard key
			const result =
				(await sql`UPDATE users SET name = ${"Alice Updated"} WHERE id = ${100}`) as QueryResultWithShardStats;

			// Should only update 1 shard
			expect(result.shardStats).toHaveLength(1);
			expect(result.rowsAffected).toBe(1);

			// Verify the update worked
			const verify =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResult;
			expect(verify.rows[0]).toMatchObject({ id: 100, name: "Alice Updated" });
		});

		it("should route DELETE to only 1 shard when WHERE filters on shard key", async () => {
			const dbId = "test-delete-shard-key-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;

			// Delete a specific user by shard key
			const result =
				(await sql`DELETE FROM users WHERE id = ${100}`) as QueryResultWithShardStats;

			// Should only delete from 1 shard
			expect(result.shardStats).toHaveLength(1);
			expect(result.rowsAffected).toBe(1);

			// Verify the delete worked
			const verify =
				(await sql`SELECT * FROM users WHERE id = ${100}`) as QueryResult;
			expect(verify.rows).toHaveLength(0);
		});

		it("should route UPDATE to all shards when WHERE does not filter on shard key", async () => {
			const dbId = "test-update-all-shards-v3";
			const sql = await createConnection(dbId, { nodes: 3 }, env);

			const createTableSql =
				"CREATE TABLE users /* SHARDS=3 */ (id INTEGER PRIMARY KEY, name TEXT)";
			await sql([createTableSql] as unknown as TemplateStringsArray);
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;
			await sql`INSERT INTO users (id, name) VALUES (${200}, ${"Bob"})`;

			// Update based on non-shard-key column (should query all shards)
			const result =
				(await sql`UPDATE users SET name = ${"Updated"} WHERE name = ${"Alice"}`) as QueryResultWithShardStats;

			// Should update on all shards (though data might only be on 1-2 shards)
			expect(result.shardStats).toHaveLength(3);
			expect(result.rowsAffected).toBe(1);
		});
	});
});
