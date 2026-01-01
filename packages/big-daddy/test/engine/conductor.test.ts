import { env } from "cloudflare:test";
import { describe, expect, it, test } from "vitest";
import type { QueryResult } from "../../src/engine/storage";
import type { TableShard } from "../../src/engine/topology/types";
import { createConnection } from "../../src/index";

describe("Conductor", () => {
	it("should execute queries with parameters", async () => {
		const dbId = "test-query";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`;

		const userId = 123;
		const name = "John's";
		const age = 25;

		await sql`SELECT * FROM users WHERE id = ${userId}`;
		await sql`SELECT * FROM users WHERE name = ${name} AND age > ${age}`;
		await sql`SELECT * FROM users`;
	});

	it("should create tables and update topology", async () => {
		const dbId = "test-create";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		await sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`;

		// Verify topology
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.tables).toHaveLength(1);
		expect(topology.tables[0]).toMatchObject({
			table_name: "products",
			primary_key: "id",
			shard_key: "id",
			num_shards: 1,
		});

		// Verify table exists in all storage nodes (get node IDs from topology)
		for (const node of topology.storage_nodes) {
			const storageStub = env.STORAGE.get(env.STORAGE.idFromName(node.node_id));
			const result = await storageStub.executeQuery({
				query:
					'SELECT name FROM sqlite_master WHERE type="table" AND name="products"',
			});
			const queryResult = result as { rows: unknown[] };
			if (queryResult.rows) {
				expect(queryResult.rows).toHaveLength(1);
			}
		}
	});

	it("should route INSERT to correct shard based on shard key", async () => {
		const dbId = "test-insert-routing";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table with 4 shards (distributed across 2 nodes)
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Get topology to see shard distribution
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();
		const _userShards = topology.table_shards.filter(
			(s: TableShard) => s.table_name === "users",
		);

		// Insert users with different IDs
		const userId1 = 100;
		const userId2 = 200;

		await sql`INSERT INTO users (id, name, email) VALUES (${userId1}, ${"Alice"}, ${"alice@example.com"})`;
		await sql`INSERT INTO users (id, name, email) VALUES (${userId2}, ${"Bob"}, ${"bob@example.com"})`;

		// Query all shards to verify data distribution
		const allUsers = await sql`SELECT * FROM users ORDER BY id`;

		expect(allUsers.rows).toHaveLength(2);
		expect(allUsers.rows[0]).toMatchObject({ id: userId1, name: "Alice" });
		expect(allUsers.rows[1]).toMatchObject({ id: userId2, name: "Bob" });
	});

	it("should route UPDATE to correct shard when WHERE filters on shard key", async () => {
		const dbId = "test-update-routing";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		const userId1 = 100;
		const userId2 = 200;
		await sql`INSERT INTO users (id, name, email) VALUES (${userId1}, ${"Alice"}, ${"alice@example.com"})`;
		await sql`INSERT INTO users (id, name, email) VALUES (${userId2}, ${"Bob"}, ${"bob@example.com"})`;

		// Update a specific user by shard key (should route to one shard)
		const result =
			await sql`UPDATE users SET name = ${"Alice Updated"} WHERE id = ${userId1}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the update worked
		const allUsers = await sql`SELECT * FROM users ORDER BY id`;
		expect(allUsers.rows).toHaveLength(2);
		expect(allUsers.rows[0]).toMatchObject({
			id: userId1,
			name: "Alice Updated",
		});
		expect(allUsers.rows[1]).toMatchObject({ id: userId2, name: "Bob" });
	});

	it("should route UPDATE to all shards when WHERE does not filter on shard key", async () => {
		const dbId = "test-update-all-shards";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		await sql`INSERT INTO users (id, name, email) VALUES (${100}, ${"Alice"}, ${"alice@example.com"})`;
		await sql`INSERT INTO users (id, name, email) VALUES (${200}, ${"Bob"}, ${"bob@example.com"})`;

		// Update based on non-shard-key column (should query all shards)
		const result =
			await sql`UPDATE users SET email = ${"updated@example.com"} WHERE name = ${"Alice"}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the update worked
		const alice = await sql`SELECT * FROM users WHERE id = ${100}`;
		expect(alice.rows[0]).toMatchObject({ email: "updated@example.com" });
	});

	it("should route DELETE to correct shard when WHERE filters on shard key", async () => {
		const dbId = "test-delete-routing";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		const userId1 = 100;
		const userId2 = 200;
		await sql`INSERT INTO users (id, name, email) VALUES (${userId1}, ${"Alice"}, ${"alice@example.com"})`;
		await sql`INSERT INTO users (id, name, email) VALUES (${userId2}, ${"Bob"}, ${"bob@example.com"})`;

		// Delete a specific user by shard key (should route to one shard)
		const result = await sql`DELETE FROM users WHERE id = ${userId1}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the delete worked
		const allUsers = await sql`SELECT * FROM users ORDER BY id`;
		expect(allUsers.rows).toHaveLength(1);
		expect(allUsers.rows[0]).toMatchObject({ id: userId2, name: "Bob" });
	});

	it("should route DELETE to all shards when WHERE does not filter on shard key", async () => {
		const dbId = "test-delete-all-shards";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		await sql`INSERT INTO users (id, name, email) VALUES (${100}, ${"Alice"}, ${"alice@example.com"})`;
		await sql`INSERT INTO users (id, name, email) VALUES (${200}, ${"Bob"}, ${"bob@example.com"})`;
		await sql`INSERT INTO users (id, name, email) VALUES (${300}, ${"Alice"}, ${"alice2@example.com"})`;

		// Delete based on non-shard-key column (should query all shards)
		const result = await sql`DELETE FROM users WHERE name = ${"Alice"}`;

		expect(result.rowsAffected).toBe(2);

		// Verify the delete worked
		const remaining = await sql`SELECT * FROM users ORDER BY id`;
		expect(remaining.rows).toHaveLength(1);
		expect(remaining.rows[0]).toMatchObject({ id: 200, name: "Bob" });
	});

	it("should maintain per-shard row_count on INSERT and DELETE", async () => {
		const dbId = "test-rowcount-maintenance";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`;

		await sql`INSERT INTO users (id, name) VALUES (${1}, ${"Alice"})`;
		await sql`INSERT INTO users (id, name) VALUES (${2}, ${"Bob"})`;
		await sql`INSERT INTO users (id, name) VALUES (${3}, ${"Charlie"})`;

		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology1 = await topologyStub.getTopology();
		const userShards1 = topology1.table_shards.filter(
			(s: TableShard) => s.table_name === "users",
		);
		const sum1 = userShards1.reduce(
			(acc: number, s: TableShard) => acc + s.row_count,
			0,
		);
		const countResult1 = await sql`SELECT COUNT(*) as cnt FROM users`;
		const count1 = countResult1.rows[0]?.cnt;
		expect(sum1).toBe(count1);

		await sql`DELETE FROM users WHERE id = ${2}`;

		const topology2 = await topologyStub.getTopology();
		const userShards2 = topology2.table_shards.filter(
			(s: TableShard) => s.table_name === "users",
		);
		const sum2 = userShards2.reduce(
			(acc: number, s: TableShard) => acc + s.row_count,
			0,
		);
		const countResult2 = await sql`SELECT COUNT(*) as cnt FROM users`;
		const count2 = countResult2.rows[0]?.cnt;
		expect(sum2).toBe(count2);
	});

	it("should route SELECT to correct shard when WHERE filters on shard key", async () => {
		const dbId = "test-select-routing";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		const userId1 = 100;
		const userId2 = 200;
		await sql`INSERT INTO users (id, name, email) VALUES (${userId1}, ${"Alice"}, ${"alice@example.com"})`;
		await sql`INSERT INTO users (id, name, email) VALUES (${userId2}, ${"Bob"}, ${"bob@example.com"})`;

		// Select a specific user by shard key (should route to one shard)
		const result = await sql`SELECT * FROM users WHERE id = ${userId1}`;

		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]).toMatchObject({
			id: userId1,
			name: "Alice",
			email: "alice@example.com",
		});
	});

	it("should route SELECT to all shards when WHERE does not filter on shard key", async () => {
		const dbId = "test-select-all-shards";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data with same name on different shards
		await sql`INSERT INTO users (id, name, email) VALUES (${100}, ${"Alice"}, ${"alice1@example.com"})`;
		await sql`INSERT INTO users (id, name, email) VALUES (${200}, ${"Alice"}, ${"alice2@example.com"})`;

		// Select based on non-shard-key column (should query all shards)
		const result =
			await sql`SELECT * FROM users WHERE name = ${"Alice"} ORDER BY id`;

		expect(result.rows).toHaveLength(2);
		expect(result.rows[0]).toMatchObject({ id: 100, name: "Alice" });
		expect(result.rows[1]).toMatchObject({ id: 200, name: "Alice" });
	});

	it("should handle complex WHERE with multiple placeholders - shard key last", async () => {
		const dbId = "test-complex-where-1";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`;

		// Insert test data
		await sql`INSERT INTO users (id, name, age) VALUES (${100}, ${"Alice"}, ${25})`;
		await sql`INSERT INTO users (id, name, age) VALUES (${200}, ${"Bob"}, ${30})`;

		// SELECT with multiple placeholders: WHERE age > ? AND id = ?
		// This tests that we use the correct parameter (id=100, not age=20)
		const result =
			await sql`SELECT * FROM users WHERE age > ${20} AND id = ${100}`;

		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]).toMatchObject({ id: 100, name: "Alice" });
	});

	it("should handle UPDATE with multiple placeholders in SET and WHERE", async () => {
		const dbId = "test-update-complex";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`;

		// Insert test data
		await sql`INSERT INTO users (id, name, age) VALUES (${100}, ${"Alice"}, ${25})`;
		await sql`INSERT INTO users (id, name, age) VALUES (${200}, ${"Bob"}, ${30})`;

		// UPDATE with SET placeholders and WHERE placeholders: SET name = ?, age = ? WHERE id = ?
		const result =
			await sql`UPDATE users SET name = ${"Alice Updated"}, age = ${26} WHERE id = ${100}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the update
		const check = await sql`SELECT * FROM users WHERE id = ${100}`;
		expect(check.rows[0]).toMatchObject({
			id: 100,
			name: "Alice Updated",
			age: 26,
		});
	});

	it("should handle DELETE with complex WHERE clause", async () => {
		const dbId = "test-delete-complex";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`;

		// Insert test data
		await sql`INSERT INTO users (id, name, age) VALUES (${100}, ${"Alice"}, ${25})`;
		await sql`INSERT INTO users (id, name, age) VALUES (${200}, ${"Bob"}, ${30})`;

		// DELETE with multiple placeholders: WHERE age < ? AND id = ?
		const result =
			await sql`DELETE FROM users WHERE age < ${28} AND id = ${100}`;

		expect(result.rowsAffected).toBe(1);

		// Verify the delete
		const remaining = await sql`SELECT * FROM users ORDER BY id`;
		expect(remaining.rows).toHaveLength(1);
		expect(remaining.rows[0]).toMatchObject({ id: 200, name: "Bob" });
	});

	it("should handle UPDATE and DELETE without WHERE clause (affects all shards)", async () => {
		const dbId = "test-no-where";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert test data
		await sql`INSERT INTO users (id, name, email) VALUES (${100}, ${"Alice"}, ${"alice@example.com"})`;
		await sql`INSERT INTO users (id, name, email) VALUES (${200}, ${"Bob"}, ${"bob@example.com"})`;

		// Update all users (no WHERE clause)
		const updateResult =
			await sql`UPDATE users SET email = ${"everyone@example.com"}`;
		expect(updateResult.rowsAffected).toBe(2);

		// Verify updates
		const allUsers = await sql`SELECT * FROM users ORDER BY id`;
		expect(allUsers.rows.every((u) => u.email === "everyone@example.com")).toBe(
			true,
		);

		// Delete all users (no WHERE clause)
		const deleteResult = await sql`DELETE FROM users`;
		expect(deleteResult.rowsAffected).toBe(2);

		// Verify deletes
		const remaining = await sql`SELECT * FROM users`;
		expect(remaining.rows).toHaveLength(0);
	});

	it("should handle queries with more than 7 shards (batching)", async () => {
		const dbId = "test-many-shards";
		// Create 10 nodes (more than the 7 parallel query limit)
		const sql = await createConnection(dbId, { nodes: 10 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Insert data that will be distributed across shards
		const inserts = [];
		for (let i = 0; i < 20; i++) {
			inserts.push(
				sql`INSERT INTO users (id, name, email) VALUES (${i}, ${`User ${i}`}, ${`user${i}@example.com`})`,
			);
		}
		await Promise.all(inserts);

		// Query all shards - should work with batching
		const allUsers = await sql`SELECT * FROM users ORDER BY id`;

		expect(allUsers.rows).toHaveLength(20);
		expect(allUsers.rows[0]).toMatchObject({ id: 0, name: "User 0" });
		expect(allUsers.rows[19]).toMatchObject({ id: 19, name: "User 19" });
	});

	it("should create a virtual index", async () => {
		const dbId = "test-create-index-1";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		// Create table
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Create index
		const result = await sql`CREATE INDEX idx_email ON users(email)`;
		expect(result.rows).toHaveLength(0);
		expect(result.rowsAffected).toBe(0);

		// Verify index was created in topology
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes).toHaveLength(1);
		expect(topology.virtual_indexes[0]!.index_name).toBe("idx_email");
		expect(topology.virtual_indexes[0]!.table_name).toBe("users");
		expect(JSON.parse(topology.virtual_indexes[0]!.columns)).toEqual(["email"]);
		expect(topology.virtual_indexes[0]!.index_type).toBe("hash");
		// In test environment, queue processing happens automatically, so status is 'ready'
		expect(topology.virtual_indexes[0]!.status).toBe("ready");
	});

	it("should create a unique index", async () => {
		const dbId = "test-create-index-2";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Create unique index
		await sql`CREATE UNIQUE INDEX idx_email ON users(email)`;

		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes[0]!.index_type).toBe("unique");
	});

	it("should handle CREATE INDEX IF NOT EXISTS", async () => {
		const dbId = "test-create-index-3";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		// Create index
		await sql`CREATE INDEX idx_email ON users(email)`;

		// Create again with IF NOT EXISTS - should succeed silently
		const result =
			await sql`CREATE INDEX IF NOT EXISTS idx_email ON users(email)`;
		expect(result.rows).toHaveLength(0);

		// Verify still only one index
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();
		expect(topology.virtual_indexes).toHaveLength(1);
	});

	it("should error on duplicate index without IF NOT EXISTS", async () => {
		const dbId = "test-create-index-4";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;

		await sql`CREATE INDEX idx_email ON users(email)`;

		// Try to create duplicate without IF NOT EXISTS
		await expect(sql`CREATE INDEX idx_email ON users(email)`).rejects.toThrow(
			"already exists",
		);
	});

	it("should error on index for non-existent table", async () => {
		const dbId = "test-create-index-5";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		// Try to create index on non-existent table
		await expect(sql`CREATE INDEX idx_email ON users(email)`).rejects.toThrow(
			"not found",
		);
	});

	it("should create composite (multi-column) indexes", async () => {
		const dbId = "test-create-index-6";
		const sql = await createConnection(dbId, { nodes: 3 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, country TEXT)`;

		// Create multi-column index
		await sql`CREATE INDEX idx_country_email ON users(country, email)`;

		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		const topology = await topologyStub.getTopology();

		expect(topology.virtual_indexes).toHaveLength(1);
		expect(topology.virtual_indexes[0]!.index_name).toBe("idx_country_email");
		expect(JSON.parse(topology.virtual_indexes[0]!.columns)).toEqual([
			"country",
			"email",
		]);
		// In test environment, queue processing happens automatically, so status is 'ready'
		expect(topology.virtual_indexes[0]!.status).toBe("ready");
	});

	it("should use composite indexes for AND WHERE clauses", async () => {
		const dbId = "test-composite-where-1";
		const sql = await createConnection(dbId, { nodes: 10 }, env); // Use many shards to see shard reduction

		// Create table with composite index
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, country TEXT, city TEXT, name TEXT)`;
		await sql`CREATE INDEX idx_country_city ON users(country, city)`;

		// Insert test data
		await sql`INSERT INTO users (id, country, city, name) VALUES (1, ${"USA"}, ${"NYC"}, ${"Alice"})`;
		await sql`INSERT INTO users (id, country, city, name) VALUES (2, ${"USA"}, ${"LA"}, ${"Bob"})`;
		await sql`INSERT INTO users (id, country, city, name) VALUES (3, ${"UK"}, ${"London"}, ${"Charlie"})`;

		// Build the index
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.batchUpsertIndexEntries("idx_country_city", [
			{ keyValue: JSON.stringify(["USA", "NYC"]), shardIds: [0] },
			{ keyValue: JSON.stringify(["USA", "LA"]), shardIds: [1] },
			{ keyValue: JSON.stringify(["UK", "London"]), shardIds: [2] },
		]);
		await topologyStub.updateIndexStatus("idx_country_city", "ready");

		// Query with composite WHERE - should use the index
		const result =
			await sql`SELECT * FROM users WHERE country = ${"USA"} AND city = ${"NYC"}`;

		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]?.name).toBe("Alice");
	});

	// TODO: Implement leftmost prefix matching for composite indexes
	// Currently, virtual indexes use exact key matching. For a 3-column composite index (a, b, c),
	// querying with only 2 columns (a, b) won't match the 3-column key stored in the index.
	// See VIRTUAL_INDEXES.md "Known Limitations" section for detailed explanation and implementation plan.
	// To fix: Store all prefix combinations during index maintenance (e.g., for (a,b,c) store (a), (a,b), (a,b,c))
	test.skip("should use composite index for leftmost prefix queries", async () => {
		const dbId = "test-composite-where-2";
		const sql = await createConnection(dbId, { nodes: 10 }, env);

		// Create table with 3-column composite index
		await sql`CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, subcategory TEXT, brand TEXT, name TEXT)`;
		await sql`CREATE INDEX idx_cat_subcat_brand ON products(category, subcategory, brand)`;

		// Insert test data
		await sql`INSERT INTO products (id, category, subcategory, brand, name) VALUES (1, ${"Electronics"}, ${"Phones"}, ${"Apple"}, ${"iPhone"})`;
		await sql`INSERT INTO products (id, category, subcategory, brand, name) VALUES (2, ${"Electronics"}, ${"Laptops"}, ${"Dell"}, ${"XPS"})`;

		// Build the index
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.batchUpsertIndexEntries("idx_cat_subcat_brand", [
			{
				keyValue: JSON.stringify(["Electronics", "Phones", "Apple"]),
				shardIds: [0],
			},
			{
				keyValue: JSON.stringify(["Electronics", "Laptops", "Dell"]),
				shardIds: [1],
			},
		]);
		await topologyStub.updateIndexStatus("idx_cat_subcat_brand", "ready");

		// Query with full composite key
		const result1 =
			await sql`SELECT * FROM products WHERE category = ${"Electronics"} AND subcategory = ${"Phones"} AND brand = ${"Apple"}`;
		expect(result1.rows).toHaveLength(1);
		expect(result1.rows[0]?.name).toBe("iPhone");

		// Query with leftmost 2-column prefix (category, subcategory)
		const result2 =
			await sql`SELECT * FROM products WHERE category = ${"Electronics"} AND subcategory = ${"Laptops"}`;
		expect(result2.rows).toHaveLength(1);
		expect(result2.rows[0]?.name).toBe("XPS");
	});

	it("should use indexes for IN queries", async () => {
		const dbId = "test-in-query-1";
		const sql = await createConnection(dbId, { nodes: 10 }, env); // Use many shards to see optimization

		// Create table with indexed column
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, country TEXT, name TEXT)`;
		await sql`CREATE INDEX idx_country ON users(country)`;

		// Insert test data
		await sql`INSERT INTO users (id, country, name) VALUES (1, ${"USA"}, ${"Alice"})`;
		await sql`INSERT INTO users (id, country, name) VALUES (2, ${"UK"}, ${"Bob"})`;
		await sql`INSERT INTO users (id, country, name) VALUES (3, ${"Canada"}, ${"Charlie"})`;
		await sql`INSERT INTO users (id, country, name) VALUES (4, ${"USA"}, ${"David"})`;
		await sql`INSERT INTO users (id, country, name) VALUES (5, ${"Germany"}, ${"Eve"})`;

		// Build the index
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.batchUpsertIndexEntries("idx_country", [
			{ keyValue: "USA", shardIds: [0, 1] },
			{ keyValue: "UK", shardIds: [2] },
			{ keyValue: "Canada", shardIds: [3] },
			{ keyValue: "Germany", shardIds: [4] },
		]);
		await topologyStub.updateIndexStatus("idx_country", "ready");

		// Query with IN - should only hit shards 0, 1, 2 (not all 10)
		const result =
			await sql`SELECT * FROM users WHERE country IN (${"USA"}, ${"UK"})`;

		// Should find 3 users: Alice, David (USA), and Bob (UK)
		expect(result.rows).toHaveLength(3);

		const names = result.rows.map((r) => r.name).sort();
		expect(names).toEqual(["Alice", "Bob", "David"]);
	});

	it("should handle IN queries with non-existent values", async () => {
		const dbId = "test-in-query-2";
		const sql = await createConnection(dbId, { nodes: 5 }, env);

		// Create table with indexed column
		await sql`CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, name TEXT)`;
		await sql`CREATE INDEX idx_category ON products(category)`;

		// Insert test data
		await sql`INSERT INTO products (id, category, name) VALUES (1, ${"Electronics"}, ${"Phone"})`;

		// Build the index
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.batchUpsertIndexEntries("idx_category", [
			{ keyValue: "Electronics", shardIds: [0] },
		]);
		await topologyStub.updateIndexStatus("idx_category", "ready");

		// Query with IN containing non-existent values
		const result =
			await sql`SELECT * FROM products WHERE category IN (${"NonExistent"}, ${"AlsoMissing"})`;

		// Should return empty result set
		expect(result.rows).toHaveLength(0);
	});

	it("should handle IN queries with mix of existent and non-existent values", async () => {
		const dbId = "test-in-query-3";
		const sql = await createConnection(dbId, { nodes: 5 }, env);

		// Create table with indexed column
		await sql`CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, name TEXT)`;
		await sql`CREATE INDEX idx_category ON products(category)`;

		// Insert test data
		await sql`INSERT INTO products (id, category, name) VALUES (1, ${"Electronics"}, ${"Phone"})`;
		await sql`INSERT INTO products (id, category, name) VALUES (2, ${"Books"}, ${"Novel"})`;

		// Build the index
		const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
		await topologyStub.batchUpsertIndexEntries("idx_category", [
			{ keyValue: "Electronics", shardIds: [0] },
			{ keyValue: "Books", shardIds: [1] },
		]);
		await topologyStub.updateIndexStatus("idx_category", "ready");

		// Query with IN containing both existent and non-existent values
		const result =
			await sql`SELECT * FROM products WHERE category IN (${"Electronics"}, ${"NonExistent"}, ${"Books"})`;

		// Should return only the 2 matching products
		expect(result.rows).toHaveLength(2);
		const names = result.rows.map((r) => r.name).sort();
		expect(names).toEqual(["Novel", "Phone"]);
	});

	describe("Composite Primary Key with _virtualShard", () => {
		it("should create table with _virtualShard as first part of composite primary key", async () => {
			const dbId = "test-composite-pk-1";
			const sql = await createConnection(dbId, { nodes: 2 }, env);

			await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`;

			// Verify topology stores original primary key
			const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
			const topology = await topologyStub.getTopology();

			expect(topology.tables[0]).toMatchObject({
				table_name: "users",
				primary_key: "id",
				shard_key: "id",
			});

			// Verify actual table schema in storage has composite primary key (_virtualShard, id)
			for (const node of topology.storage_nodes) {
				const storageStub = env.STORAGE.get(
					env.STORAGE.idFromName(node.node_id),
				);
				const schemaResult = (await storageStub.executeQuery({
					query:
						'SELECT sql FROM sqlite_master WHERE type="table" AND name="users"',
				})) as { rows: { sql: string }[] };

				if (schemaResult.rows.length > 0) {
					const createTableSQL = schemaResult.rows[0]!.sql;

					// Should contain _virtualShard column
					expect(createTableSQL).toContain("_virtualShard");

					// Should have composite PRIMARY KEY (_virtualShard, id)
					expect(createTableSQL).toMatch(
						/PRIMARY KEY\s*\(\s*_virtualShard\s*,\s*id\s*\)/i,
					);
				}
			}
		});

		it("should preserve existing composite primary key and prepend _virtualShard", async () => {
			const dbId = "test-composite-pk-2";
			const sql = await createConnection(dbId, { nodes: 2 }, env);

			// Create table with existing composite primary key (user_id, tenant_id)
			await sql`CREATE TABLE events (user_id INTEGER, tenant_id INTEGER, event_type TEXT, PRIMARY KEY (user_id, tenant_id))`;

			// Verify topology
			const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
			const topology = await topologyStub.getTopology();

			expect(topology.tables[0]).toMatchObject({
				table_name: "events",
				primary_key: "user_id,tenant_id", // Should store composite key
				shard_key: "user_id", // Uses first column of composite key
			});

			// Verify actual table schema has (_virtualShard, user_id, tenant_id) as PRIMARY KEY
			for (const node of topology.storage_nodes) {
				const storageStub = env.STORAGE.get(
					env.STORAGE.idFromName(node.node_id),
				);
				const schemaResult = (await storageStub.executeQuery({
					query:
						'SELECT sql FROM sqlite_master WHERE type="table" AND name="events"',
				})) as { rows: { sql: string }[] };

				if (schemaResult.rows.length > 0) {
					const createTableSQL = schemaResult.rows[0]!.sql;

					// Should have _virtualShard prepended to composite primary key
					expect(createTableSQL).toMatch(
						/PRIMARY KEY\s*\(\s*_virtualShard\s*,\s*user_id\s*,\s*tenant_id\s*\)/i,
					);
				}
			}
		});

		it("should allow duplicate primary keys on different shards (same node)", async () => {
			const dbId = "test-duplicate-pk";
			const sql = await createConnection(dbId, { nodes: 2 }, env);

			await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`;

			// Insert same id=100 on different virtual shards (they'll end up on same physical node during resharding)
			// This tests the composite key (_virtualShard, id) prevents conflicts
			await sql`INSERT INTO users (id, name) VALUES (${100}, ${"Alice"})`;

			// Get topology to manually insert to different shard
			const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName(dbId));
			const topology = await topologyStub.getTopology();

			// Manually insert directly to storage with different _virtualShard value
			// This simulates what happens during resharding when copying data
			const storageStub = env.STORAGE.get(
				env.STORAGE.idFromName(topology.storage_nodes[0]!.node_id),
			);

			// Insert with _virtualShard=1 (different from default shard 0)
			await storageStub.executeQuery({
				query: "INSERT INTO users (_virtualShard, id, name) VALUES (?, ?, ?)",
				params: [1, 100, "Bob"],
			});

			// Both rows should exist (no primary key conflict)
			const allRows = await storageStub.executeQuery({
				query: "SELECT * FROM users WHERE id = ?",
				params: [100],
			});

			expect((allRows as QueryResult).rows).toHaveLength(2);
		});
	});

	it("should handle multi-row INSERT statements", async () => {
		const dbId = "test-multi-row-insert";
		const sql = await createConnection(dbId, { nodes: 1 }, env);

		await sql`CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL, description TEXT)`;

		// Test multi-row insert with multiple VALUE tuples
		const result = await sql`
			INSERT INTO notes (id, title, description) VALUES
			(${1}, ${"Note 1"}, ${"Description for note 1"}),
			(${2}, ${"Note 2"}, ${"Description for note 2"}),
			(${3}, ${"Note 3"}, ${"Description for note 3"}),
			(${4}, ${"Note 4"}, ${"Description for note 4"}),
			(${5}, ${"Note 5"}, ${"Description for note 5"})
		`;

		// Multi-row insert works! The data is inserted correctly
		expect(result.rowsAffected).toBeGreaterThan(0);

		// Verify all rows were inserted
		const allNotes = await sql`SELECT * FROM notes ORDER BY id`;
		expect(allNotes.rows).toHaveLength(5);
		expect(allNotes.rows[0]).toMatchObject({
			id: 1,
			title: "Note 1",
			description: "Description for note 1",
		});
		expect(allNotes.rows[1]).toMatchObject({
			id: 2,
			title: "Note 2",
			description: "Description for note 2",
		});
		expect(allNotes.rows[4]).toMatchObject({
			id: 5,
			title: "Note 5",
			description: "Description for note 5",
		});
	});

	it("should handle multi-row INSERT with literal values (naughty insert)", async () => {
		const dbId = "test-multi-row-insert-literals";
		const sql = await createConnection(dbId, { nodes: 1 }, env);

		await sql`CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL, description TEXT)`;

		// Test multi-row insert with literal values (no placeholders)
		const result = await sql`
			INSERT INTO notes (id, title, description) VALUES
			(1, 'Note 1', 'Description for note 1'),
			(2, 'Note 2', 'Description for note 2'),
			(3, 'Note 3', 'Description for note 3'),
			(4, 'Note 4', 'Description for note 4'),
			(5, 'Note 5', 'Description for note 5')
		`;

		// Multi-row insert with literals works!
		expect(result.rowsAffected).toBeGreaterThan(0);

		// Verify all rows were inserted
		const allNotes = await sql`SELECT * FROM notes ORDER BY id`;
		expect(allNotes.rows).toHaveLength(5);
		expect(allNotes.rows[0]).toMatchObject({
			id: 1,
			title: "Note 1",
			description: "Description for note 1",
		});
		expect(allNotes.rows[4]).toMatchObject({
			id: 5,
			title: "Note 5",
			description: "Description for note 5",
		});
	});

	it("should handle COUNT(*) aggregation across shards", async () => {
		const dbId = "test-count-aggregation";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		// Create table with 3 shards
		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`;

		// Insert some users
		await sql`INSERT INTO users (id, name) VALUES (${1}, ${"Alice"})`;
		await sql`INSERT INTO users (id, name) VALUES (${2}, ${"Bob"})`;
		await sql`INSERT INTO users (id, name) VALUES (${3}, ${"Charlie"})`;

		// Test COUNT(*) aggregation
		const countResult = await sql`SELECT COUNT(*) FROM users`;
		expect(countResult.rows).toHaveLength(1);
		// The column name should be COUNT(*) based on SQLite convention
		const countValue =
			countResult.rows[0]?.["COUNT(*)"] ?? countResult.rows[0]?.COUNT;
		expect(countValue).toBe(3);
	});

	it("should handle COUNT(column) aggregation", async () => {
		const dbId = "test-count-column-aggregation";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		await sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`;

		// Insert some users
		await sql`INSERT INTO users (id, name) VALUES (${1}, ${"Alice"})`;
		await sql`INSERT INTO users (id, name) VALUES (${2}, ${"Bob"})`;
		await sql`INSERT INTO users (id, name) VALUES (${3}, ${"Charlie"})`;

		// Test COUNT(name) aggregation
		const countResult = await sql`SELECT COUNT(name) FROM users`;
		expect(countResult.rows).toHaveLength(1);
		// The column name should be COUNT(name) based on SQLite convention
		const countValue = countResult.rows[0]?.["COUNT(name)"];
		expect(countValue).toBe(3);
	});

	it("should handle aliased aggregation functions", async () => {
		const dbId = "test-aliased-aggregation";
		const sql = await createConnection(dbId, { nodes: 2 }, env);

		await sql`CREATE TABLE potato (id INTEGER PRIMARY KEY, name TEXT)`;

		// Insert some potatoes
		await sql`INSERT INTO potato (id, name) VALUES (${1}, ${"Russet"})`;
		await sql`INSERT INTO potato (id, name) VALUES (${2}, ${"Sweet"})`;
		await sql`INSERT INTO potato (id, name) VALUES (${3}, ${"Red"})`;

		// Test COUNT() with alias
		const countResult = await sql`SELECT COUNT() as yo FROM potato`;
		expect(countResult.rows).toHaveLength(1);
		// The alias should be used as the column name
		const countValue = countResult.rows[0]?.yo;
		expect(countValue).toBe(3);
	});
});
