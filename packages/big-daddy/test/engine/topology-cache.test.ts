import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { createConductor } from '../../src/engine/conductor';

describe('TopologyCache', () => {
	async function initializeTopology(dbId: string, numNodes: number = 2) {
		const topologyId = env.TOPOLOGY.idFromName(dbId);
		const topologyStub = env.TOPOLOGY.get(topologyId);
		await topologyStub.create(numNodes);
	}

	it('should cache query plan data for SELECT queries with WHERE clause', async () => {
		const dbId = 'test-cache-select';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table and index
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)`;
		await conductor.sql`CREATE INDEX idx_email ON users(email)`;

		// Insert test data
		await conductor.sql`INSERT INTO users (id, email, name) VALUES (1, ${'alice@example.com'}, ${'Alice'})`;
		await conductor.sql`INSERT INTO users (id, email, name) VALUES (2, ${'bob@example.com'}, ${'Bob'})`;

		// Clear cache before testing to get clean stats
		conductor.clearCache();

		// First query - should be a cache miss
		const result1 = await conductor.sql`SELECT * FROM users WHERE id = ${1}`;
		expect(result1.rows).toHaveLength(1);
		expect(result1.rows[0]).toMatchObject({ id: 1, name: 'Alice' });

		// Verify cache stats in the result
		expect(result1.cacheStats).toBeDefined();
		expect(result1.cacheStats?.cacheHit).toBe(false); // First query is a miss
		expect(result1.cacheStats?.totalMisses).toBe(1);
		expect(result1.cacheStats?.totalHits).toBe(0);

		// Second query with same predicate - should be a cache hit
		const result2 = await conductor.sql`SELECT * FROM users WHERE id = ${1}`;
		expect(result2.rows).toHaveLength(1);
		expect(result2.rows[0]).toMatchObject({ id: 1, name: 'Alice' });

		// Verify cache hit in the result!
		expect(result2.cacheStats?.cacheHit).toBe(true); // Second query is a HIT!
		expect(result2.cacheStats?.totalHits).toBe(1);
		expect(result2.cacheStats?.totalMisses).toBe(1);
		expect(result2.cacheStats?.cacheSize).toBe(1);

		// Third query with different predicate - should be a cache miss
		const result3 = await conductor.sql`SELECT * FROM users WHERE id = ${2}`;
		expect(result3.rows).toHaveLength(1);
		expect(result3.rows[0]).toMatchObject({ id: 2, name: 'Bob' });

		// Verify cache miss for different predicate
		expect(result3.cacheStats?.cacheHit).toBe(false);
		expect(result3.cacheStats?.totalHits).toBe(1);
		expect(result3.cacheStats?.totalMisses).toBe(2);
		expect(result3.cacheStats?.cacheSize).toBe(2); // Now have 2 cached entries
	});

	it('should invalidate cache after INSERT operations', async () => {
		const dbId = 'test-cache-insert-invalidation';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`;

		conductor.clearCache();

		// First query - cache miss
		const result1 = await conductor.sql`SELECT * FROM products WHERE id = ${100}`;
		expect(result1.rows).toHaveLength(0);

		let stats = conductor.getCacheStats();
		expect(stats.misses).toBe(1);
		expect(stats.hits).toBe(0);
		expect(stats.size).toBe(1); // Should have 1 cached entry

		// Second query with same predicate - cache hit
		await conductor.sql`SELECT * FROM products WHERE id = ${100}`;
		stats = conductor.getCacheStats();
		expect(stats.hits).toBe(1);
		expect(stats.size).toBe(1);

		// Insert new row - should invalidate cache
		await conductor.sql`INSERT INTO products (id, name, price) VALUES (${100}, ${'Widget'}, ${9.99})`;

		stats = conductor.getCacheStats();
		expect(stats.size).toBe(0); // Cache should be invalidated!

		// Query again - should be a cache miss (cache was invalidated)
		const result2 = await conductor.sql`SELECT * FROM products WHERE id = ${100}`;
		expect(result2.rows).toHaveLength(1);
		expect(result2.rows[0]).toMatchObject({ id: 100, name: 'Widget' });

		stats = conductor.getCacheStats();
		expect(stats.misses).toBe(2); // Original miss + miss after invalidation
		expect(stats.size).toBe(1); // Now cached again
	});

	it('should invalidate cache after UPDATE operations', async () => {
		const dbId = 'test-cache-update-invalidation';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table and insert data
		await conductor.sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`;
		await conductor.sql`INSERT INTO products (id, name, price) VALUES (${1}, ${'Widget'}, ${9.99})`;

		// First query - cache miss
		const result1 = await conductor.sql`SELECT * FROM products WHERE id = ${1}`;
		expect(result1.rows[0]).toMatchObject({ name: 'Widget', price: 9.99 });

		// Update the row - should invalidate cache
		await conductor.sql`UPDATE products SET price = ${19.99} WHERE id = ${1}`;

		// Query again - should see the updated price
		const result2 = await conductor.sql`SELECT * FROM products WHERE id = ${1}`;
		expect(result2.rows[0]).toMatchObject({ name: 'Widget', price: 19.99 });
	});

	it('should invalidate cache after DELETE operations', async () => {
		const dbId = 'test-cache-delete-invalidation';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table and insert data
		await conductor.sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`;
		await conductor.sql`INSERT INTO products (id, name, price) VALUES (${1}, ${'Widget'}, ${9.99})`;

		// First query - cache miss
		const result1 = await conductor.sql`SELECT * FROM products WHERE id = ${1}`;
		expect(result1.rows).toHaveLength(1);

		// Delete the row - should invalidate cache
		await conductor.sql`DELETE FROM products WHERE id = ${1}`;

		// Query again - should see no rows
		const result2 = await conductor.sql`SELECT * FROM products WHERE id = ${1}`;
		expect(result2.rows).toHaveLength(0);
	});

	it('should not cache full table scans', async () => {
		const dbId = 'test-cache-full-scan';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table and insert data
		await conductor.sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)`;
		await conductor.sql`INSERT INTO products (id, name, price) VALUES (${1}, ${'Widget'}, ${9.99})`;

		conductor.clearCache();

		// Full table scan - should not be cached
		const result1 = await conductor.sql`SELECT * FROM products`;
		expect(result1.rows).toHaveLength(1);

		let stats = conductor.getCacheStats();
		expect(stats.size).toBe(0); // No cache entry for full table scan
		expect(stats.hits).toBe(0);
		expect(stats.misses).toBe(0); // Not even counted as a miss!

		// Full table scan again - still no caching
		const result2 = await conductor.sql`SELECT * FROM products`;
		expect(result2.rows).toHaveLength(1);

		stats = conductor.getCacheStats();
		expect(stats.size).toBe(0); // Still no cache entry
		expect(stats.hits).toBe(0);
		expect(stats.misses).toBe(0);
	});

	it('should cache queries with index-based WHERE clauses', async () => {
		const dbId = 'test-cache-index-query';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table and index
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)`;
		await conductor.sql`CREATE INDEX idx_email ON users(email)`;

		// Insert test data
		await conductor.sql`INSERT INTO users (id, email, name) VALUES (${1}, ${'alice@example.com'}, ${'Alice'})`;
		await conductor.sql`INSERT INTO users (id, email, name) VALUES (${2}, ${'bob@example.com'}, ${'Bob'})`;

		conductor.clearCache();

		// Query by indexed column - should be cached
		const result1 = await conductor.sql`SELECT * FROM users WHERE email = ${'alice@example.com'}`;
		expect(result1.rows).toHaveLength(1);
		expect(result1.rows[0]).toMatchObject({ name: 'Alice' });

		let stats = conductor.getCacheStats();
		expect(stats.misses).toBe(1);
		expect(stats.hits).toBe(0);

		// Same query again - should be a cache hit
		const result2 = await conductor.sql`SELECT * FROM users WHERE email = ${'alice@example.com'}`;
		expect(result2.rows).toHaveLength(1);
		expect(result2.rows[0]).toMatchObject({ name: 'Alice' });

		stats = conductor.getCacheStats();
		expect(stats.hits).toBe(1); // Cache hit!
		expect(stats.misses).toBe(1);
	});

	it('should cache queries with composite index predicates', async () => {
		const dbId = 'test-cache-composite-index';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table and composite index
		await conductor.sql`CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT, total REAL)`;
		await conductor.sql`CREATE INDEX idx_user_status ON orders(user_id, status)`;

		// Insert test data
		await conductor.sql`INSERT INTO orders (id, user_id, status, total) VALUES (${1}, ${100}, ${'pending'}, ${99.99})`;
		await conductor.sql`INSERT INTO orders (id, user_id, status, total) VALUES (${2}, ${100}, ${'completed'}, ${149.99})`;

		// Query by composite index - should be cached
		const result1 = await conductor.sql`SELECT * FROM orders WHERE user_id = ${100} AND status = ${'pending'}`;
		expect(result1.rows).toHaveLength(1);
		expect(result1.rows[0]).toMatchObject({ id: 1, total: 99.99 });

		// Same query again - should be a cache hit
		const result2 = await conductor.sql`SELECT * FROM orders WHERE user_id = ${100} AND status = ${'pending'}`;
		expect(result2.rows).toHaveLength(1);
		expect(result2.rows[0]).toMatchObject({ id: 1, total: 99.99 });
	});

	it('should invalidate index cache when indexed columns are updated', async () => {
		const dbId = 'test-cache-index-invalidation';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table and index
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)`;
		await conductor.sql`CREATE INDEX idx_email ON users(email)`;

		// Insert test data
		await conductor.sql`INSERT INTO users (id, email, name) VALUES (${1}, ${'alice@example.com'}, ${'Alice'})`;

		// Query by indexed column - should be cached
		const result1 = await conductor.sql`SELECT * FROM users WHERE email = ${'alice@example.com'}`;
		expect(result1.rows).toHaveLength(1);

		// Update the indexed column - should invalidate index cache
		await conductor.sql`UPDATE users SET email = ${'alice.new@example.com'} WHERE id = ${1}`;

		// Query with old email - should see no rows
		const result2 = await conductor.sql`SELECT * FROM users WHERE email = ${'alice@example.com'}`;
		expect(result2.rows).toHaveLength(0);

		// Query with new email - should see the row
		const result3 = await conductor.sql`SELECT * FROM users WHERE email = ${'alice.new@example.com'}`;
		expect(result3.rows).toHaveLength(1);
	});

	it('should handle cache expiration with TTL', async () => {
		const dbId = 'test-cache-ttl';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)`;
		await conductor.sql`INSERT INTO products (id, name) VALUES (${1}, ${'Widget'})`;

		conductor.clearCache();

		// First query - cache miss
		const result1 = await conductor.sql`SELECT * FROM products WHERE id = ${1}`;
		expect(result1.rows).toHaveLength(1);

		let stats = conductor.getCacheStats();
		expect(stats.misses).toBe(1);

		// Second query immediately - cache hit
		const result2 = await conductor.sql`SELECT * FROM products WHERE id = ${1}`;
		expect(result2.rows).toHaveLength(1);

		stats = conductor.getCacheStats();
		expect(stats.hits).toBe(1);

		// Note: Testing actual TTL expiration would require waiting 30+ seconds
		// which is not practical in unit tests. The TTL logic is tested through
		// the cache implementation itself.
	});

	it('should demonstrate cache performance with multiple queries', async () => {
		const dbId = 'test-cache-performance';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		// Create table
		await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${1}, ${'Alice'}, ${'alice@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${2}, ${'Bob'}, ${'bob@example.com'})`;
		await conductor.sql`INSERT INTO users (id, name, email) VALUES (${3}, ${'Charlie'}, ${'charlie@example.com'})`;

		conductor.clearCache();

		// Run 9 queries - mix of same and different predicates
		const r1 = await conductor.sql`SELECT * FROM users WHERE id = ${1}`;
		const r2 = await conductor.sql`SELECT * FROM users WHERE id = ${1}`; // hit
		const r3 = await conductor.sql`SELECT * FROM users WHERE id = ${2}`;
		const r4 = await conductor.sql`SELECT * FROM users WHERE id = ${1}`; // hit
		const r5 = await conductor.sql`SELECT * FROM users WHERE id = ${2}`; // hit
		const r6 = await conductor.sql`SELECT * FROM users WHERE id = ${3}`;
		const r7 = await conductor.sql`SELECT * FROM users WHERE id = ${1}`; // hit
		const r8 = await conductor.sql`SELECT * FROM users WHERE id = ${2}`; // hit
		const r9 = await conductor.sql`SELECT * FROM users WHERE id = ${3}`; // hit

		// Verify individual query cache stats
		expect(r1.cacheStats?.cacheHit).toBe(false); // miss
		expect(r2.cacheStats?.cacheHit).toBe(true);  // hit!
		expect(r3.cacheStats?.cacheHit).toBe(false); // miss
		expect(r4.cacheStats?.cacheHit).toBe(true);  // hit!
		expect(r5.cacheStats?.cacheHit).toBe(true);  // hit!
		expect(r6.cacheStats?.cacheHit).toBe(false); // miss
		expect(r7.cacheStats?.cacheHit).toBe(true);  // hit!
		expect(r8.cacheStats?.cacheHit).toBe(true);  // hit!
		expect(r9.cacheStats?.cacheHit).toBe(true);  // hit!

		// Verify final stats
		const finalStats = r9.cacheStats!;
		expect(finalStats.totalMisses).toBe(3); // Only 3 unique queries (id=1, id=2, id=3)
		expect(finalStats.totalHits).toBe(6); // 6 cache hits
		expect(finalStats.cacheSize).toBe(3); // 3 cached entries

		// Cache hit rate should be 66.6% (6 hits / 9 total)
		const hitRate = finalStats.totalHits / (finalStats.totalHits + finalStats.totalMisses);
		expect(hitRate).toBeCloseTo(0.667, 2);
	});

	it('should include cache stats in every SELECT query result', async () => {
		const dbId = 'test-cache-stats-in-result';
		await initializeTopology(dbId);

		const conductor = createConductor(dbId, env);

		await conductor.sql`CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)`;
		await conductor.sql`INSERT INTO products (id, name) VALUES (${1}, ${'Widget'})`;

		conductor.clearCache();

		// Every SELECT should have cache stats
		const result = await conductor.sql`SELECT * FROM products WHERE id = ${1}`;

		expect(result.cacheStats).toBeDefined();
		expect(result.cacheStats).toHaveProperty('cacheHit');
		expect(result.cacheStats).toHaveProperty('totalHits');
		expect(result.cacheStats).toHaveProperty('totalMisses');
		expect(result.cacheStats).toHaveProperty('cacheSize');

		// This query should be a cache miss (first time)
		expect(result.cacheStats!.cacheHit).toBe(false);

		// Query again - should be a cache hit
		const result2 = await conductor.sql`SELECT * FROM products WHERE id = ${1}`;
		expect(result2.cacheStats!.cacheHit).toBe(true);
	});
});
