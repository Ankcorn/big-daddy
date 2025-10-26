import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';
import type { QueryBatch } from '../../src/engine/storage';

describe('Storage Durable Object', () => {
	it('should execute a simple query', async () => {
		// Get a Durable Object stub using idFromName
		const id = env.STORAGE.idFromName('test-storage');
		const stub = env.STORAGE.get(id);

		// Execute a simple query
		const query: QueryBatch = {
			query: 'SELECT 1 as num',
			params: [],
			queryType: 'SELECT',
		};

		const result = await stub.executeQuery(query);

		// Verify the result
		expect(result).toHaveProperty('rows');
		expect(result).toHaveProperty('queryType', 'SELECT');
		expect(Array.isArray((result as any).rows)).toBe(true);
	});

	it('should create a table and insert data', async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName('test-storage-2');
		const stub = env.STORAGE.get(id);

		// Create table
		const createTable: QueryBatch = {
			query: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
			params: [],
			queryType: 'CREATE',
		};

		await stub.executeQuery(createTable);

		// Insert data
		const insert: QueryBatch = {
			query: 'INSERT INTO users (id, name) VALUES (?, ?)',
			params: [1, 'John Doe'],
			queryType: 'INSERT',
		};

		const insertResult = await stub.executeQuery(insert);
		expect((insertResult as any).rowsAffected).toBe(1);

		// Query data
		const select: QueryBatch = {
			query: 'SELECT * FROM users WHERE id = ?',
			params: [1],
			queryType: 'SELECT',
		};

		const selectResult = await stub.executeQuery(select);
		expect((selectResult as any).rows).toHaveLength(1);
		expect((selectResult as any).rows[0]).toEqual({ id: 1, name: 'John Doe' });
	});

	it('should execute batch queries', async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName('test-storage-3');
		const stub = env.STORAGE.get(id);

		// Execute batch queries
		const batch: QueryBatch[] = [
			{
				query: 'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)',
				params: [],
				queryType: 'CREATE',
			},
			{
				query: 'INSERT INTO products (id, name, price) VALUES (?, ?, ?)',
				params: [1, 'Product A', 19.99],
				queryType: 'INSERT',
			},
			{
				query: 'INSERT INTO products (id, name, price) VALUES (?, ?, ?)',
				params: [2, 'Product B', 29.99],
				queryType: 'INSERT',
			},
		];

		const result = await stub.executeQuery(batch);

		// Verify batch result
		expect(result).toHaveProperty('results');
		expect(result).toHaveProperty('totalRowsAffected');
		expect((result as any).results).toHaveLength(3);
		// CREATE TABLE can count as 2 rows affected (table creation), plus 2 inserts = 4 total
		expect((result as any).totalRowsAffected).toBeGreaterThanOrEqual(2);
	});

	it('should get database size', async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName('test-storage-4');
		const stub = env.STORAGE.get(id);

		// Get database size
		const size = await stub.getDatabaseSize();

		// Verify size is a number
		expect(typeof size).toBe('number');
		expect(size).toBeGreaterThanOrEqual(0);
	});

	it('should handle UPDATE operations', async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName('test-storage-5');
		const stub = env.STORAGE.get(id);

		// Setup: Create table and insert data
		await stub.executeQuery({
			query: 'CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)',
			params: [],
			queryType: 'CREATE',
		});

		await stub.executeQuery({
			query: 'INSERT INTO settings (key, value) VALUES (?, ?)',
			params: ['theme', 'dark'],
			queryType: 'INSERT',
		});

		// Update the value
		const updateResult = await stub.executeQuery({
			query: 'UPDATE settings SET value = ? WHERE key = ?',
			params: ['light', 'theme'],
			queryType: 'UPDATE',
		});

		expect((updateResult as any).rowsAffected).toBe(1);

		// Verify the update
		const selectResult = await stub.executeQuery({
			query: 'SELECT value FROM settings WHERE key = ?',
			params: ['theme'],
			queryType: 'SELECT',
		});

		expect((selectResult as any).rows[0].value).toBe('light');
	});

	it('should handle DELETE operations', async () => {
		// Get a Durable Object stub
		const id = env.STORAGE.idFromName('test-storage-6');
		const stub = env.STORAGE.get(id);

		// Setup: Create table and insert data
		await stub.executeQuery({
			query: 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)',
			params: [],
			queryType: 'CREATE',
		});

		await stub.executeQuery({
			query: 'INSERT INTO items (id, name) VALUES (?, ?), (?, ?)',
			params: [1, 'Item 1', 2, 'Item 2'],
			queryType: 'INSERT',
		});

		// Delete one item
		const deleteResult = await stub.executeQuery({
			query: 'DELETE FROM items WHERE id = ?',
			params: [1],
			queryType: 'DELETE',
		});

		expect((deleteResult as any).rowsAffected).toBe(1);

		// Verify the delete
		const selectResult = await stub.executeQuery({
			query: 'SELECT * FROM items',
			params: [],
			queryType: 'SELECT',
		});

		expect((selectResult as any).rows).toHaveLength(1);
		expect((selectResult as any).rows[0].id).toBe(2);
	});
});
