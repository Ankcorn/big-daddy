import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type {
	BatchQueryResult,
	QueryBatch,
	QueryResult,
} from "../../src/engine/storage";

describe("Storage Durable Object", () => {
	it("should execute a simple query", async () => {
		// Get a Durable Object stub using idFromName
		const id = env.STORAGE.idFromName("test-storage");
		const stub = env.STORAGE.get(id);

		// Execute a simple query
		const query: QueryBatch = {
			query: "SELECT 1 as num",
			params: [],
		};

		const result = await stub.executeQuery(query);

		// Verify the result
		expect(result).toHaveProperty("rows");
		const queryResult = result as QueryResult;
		expect(Array.isArray(queryResult.rows)).toBe(true);
		expect(queryResult.rows[0]).toEqual({ num: 1 });
	});

	it("should create a table and insert data", async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName("test-storage-2");
		const stub = env.STORAGE.get(id);

		// Create table
		const createTable: QueryBatch = {
			query: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
			params: [],
		};

		await stub.executeQuery(createTable);

		// Insert data
		const insert: QueryBatch = {
			query: "INSERT INTO users (id, name) VALUES (?, ?)",
			params: [1, "John Doe"],
		};

		const insertResult = (await stub.executeQuery(insert)) as QueryResult;
		expect(insertResult.rowsAffected).toBe(1);

		// Query data
		const select: QueryBatch = {
			query: "SELECT * FROM users WHERE id = ?",
			params: [1],
		};

		const selectResult = (await stub.executeQuery(select)) as QueryResult;
		expect(selectResult.rows).toHaveLength(1);
		expect(selectResult.rows[0]).toEqual({ id: 1, name: "John Doe" });
	});

	it("should execute batch queries", async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName("test-storage-3");
		const stub = env.STORAGE.get(id);

		// Execute batch queries
		const batch: QueryBatch[] = [
			{
				query:
					"CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
				params: [],
			},
			{
				query: "INSERT INTO products (id, name, price) VALUES (?, ?, ?)",
				params: [1, "Product A", 19.99],
			},
			{
				query: "INSERT INTO products (id, name, price) VALUES (?, ?, ?)",
				params: [2, "Product B", 29.99],
			},
		];

		const result = await stub.executeQuery(batch);

		// Verify batch result
		expect(result).toHaveProperty("results");
		expect(result).toHaveProperty("totalRowsAffected");
		const batchResult = result as BatchQueryResult;
		expect(batchResult.results).toHaveLength(3);
		// CREATE TABLE can count as 2 rows affected (table creation), plus 2 inserts = 4 total
		expect(batchResult.totalRowsAffected).toBeGreaterThanOrEqual(2);
	});

	it("should get database size", async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName("test-storage-4");
		const stub = env.STORAGE.get(id);

		// Get database size
		const size = await stub.getDatabaseSize();

		// Verify size is a number
		expect(typeof size).toBe("number");
		expect(size).toBeGreaterThanOrEqual(0);
	});

	it("should handle UPDATE operations", async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName("test-storage-5");
		const stub = env.STORAGE.get(id);

		// Setup: Create table and insert data
		await stub.executeQuery({
			query: "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)",
			params: [],
		});

		await stub.executeQuery({
			query: "INSERT INTO settings (key, value) VALUES (?, ?)",
			params: ["theme", "dark"],
		});

		// Update the value
		const updateResult = (await stub.executeQuery({
			query: "UPDATE settings SET value = ? WHERE key = ?",
			params: ["light", "theme"],
		})) as QueryResult;

		expect(updateResult.rowsAffected).toBe(1);

		// Verify the update
		const selectResult = (await stub.executeQuery({
			query: "SELECT value FROM settings WHERE key = ?",
			params: ["theme"],
		})) as QueryResult<{ value: string }>;

		expect(selectResult.rows[0]?.value).toBe("light");
	});

	it("should handle DELETE operations", async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName("test-storage-6");
		const stub = env.STORAGE.get(id);

		// Setup: Create table and insert data
		await stub.executeQuery({
			query: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
			params: [],
		});

		await stub.executeQuery({
			query: "INSERT INTO items (id, name) VALUES (?, ?), (?, ?)",
			params: [1, "Item 1", 2, "Item 2"],
		});

		// Delete one item
		const deleteResult = (await stub.executeQuery({
			query: "DELETE FROM items WHERE id = ?",
			params: [1],
		})) as QueryResult;

		expect(deleteResult.rowsAffected).toBe(1);

		// Verify the delete
		const selectResult = (await stub.executeQuery({
			query: "SELECT * FROM items",
			params: [],
		})) as QueryResult<{ id: number; name: string }>;

		expect(selectResult.rows).toHaveLength(1);
		expect(selectResult.rows[0]?.id).toBe(2);
	});
});
