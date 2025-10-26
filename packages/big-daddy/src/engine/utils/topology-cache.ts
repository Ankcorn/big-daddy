import type { Statement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement } from '@databases/sqlite-ast';
import type { QueryPlanData } from '../topology';

/**
 * Cache entry with timestamp and TTL
 */
interface CacheEntry<T> {
	data: T;
	timestamp: number;
	ttl: number; // in milliseconds
}

/**
 * Cache statistics for monitoring
 */
export interface CacheStats {
	hits: number;
	misses: number;
	evictions: number;
	size: number;
}

/**
 * TopologyCache - In-memory cache for query plan data and index lookups
 *
 * This cache reduces the number of calls to the Topology Durable Object by caching:
 * 1. Query plan data (table metadata + shard targets)
 * 2. Index lookup results (key_value → shard_ids)
 * 3. Table metadata (rarely changes)
 *
 * Cache invalidation happens on write operations (INSERT/UPDATE/DELETE) that affect indexed columns.
 */
export class TopologyCache {
	private cache = new Map<string, CacheEntry<any>>();

	// Cache configuration
	private readonly MAX_ENTRIES = 10000; // ~1MB for typical entries
	private readonly TABLE_METADATA_TTL = 5 * 60 * 1000; // 5 minutes (very stable)
	private readonly INDEX_LOOKUP_TTL = 60 * 1000; // 1 minute (moderate)
	private readonly SHARD_ROUTE_TTL = 30 * 1000; // 30 seconds (conservative)

	// Statistics
	private stats = {
		hits: 0,
		misses: 0,
		evictions: 0,
	};

	/**
	 * Get cached query plan data
	 */
	getQueryPlanData(cacheKey: string): QueryPlanData | null {
		return this.get<QueryPlanData>(cacheKey);
	}

	/**
	 * Set query plan data in cache
	 */
	setQueryPlanData(cacheKey: string, data: QueryPlanData, ttl: number): void {
		this.set(cacheKey, data, ttl);
	}

	/**
	 * Get cached index lookup result
	 */
	getIndexLookup(indexName: string, keyValue: string): number[] | null {
		const cacheKey = this.buildIndexCacheKey(indexName, keyValue);
		return this.get<number[]>(cacheKey);
	}

	/**
	 * Set index lookup result in cache
	 */
	setIndexLookup(indexName: string, keyValue: string, shardIds: number[]): void {
		const cacheKey = this.buildIndexCacheKey(indexName, keyValue);
		this.set(cacheKey, shardIds, this.INDEX_LOOKUP_TTL);
	}

	/**
	 * Build cache key for query plan data
	 *
	 * Returns null if the query is not cacheable (e.g., full table scans)
	 */
	buildQueryPlanCacheKey(tableName: string, statement: Statement, params: any[]): string | null {
		// Only cache SELECT queries with WHERE clauses
		if (statement.type === 'SelectStatement') {
			const selectStmt = statement as SelectStatement;
			if (!selectStmt.where) {
				return null; // Full table scan - not cacheable
			}

			// Build a cache key from the WHERE clause structure
			const whereKey = this.serializeWhereClause(selectStmt.where, params);
			if (whereKey) {
				return `qp:${tableName}:${whereKey}`;
			}
		}

		// INSERT/UPDATE/DELETE are not cacheable for query planning
		// (they always need fresh topology data for write operations)
		return null;
	}

	/**
	 * Build cache key for index lookups
	 */
	private buildIndexCacheKey(indexName: string, keyValue: string): string {
		return `idx:${indexName}:${keyValue}`;
	}

	/**
	 * Build cache key for table metadata
	 */
	buildTableMetadataCacheKey(tableName: string): string {
		return `meta:${tableName}`;
	}

	/**
	 * Serialize WHERE clause to a cache key string
	 *
	 * This creates a deterministic string representation of the WHERE clause
	 * that can be used as a cache key.
	 */
	private serializeWhereClause(where: any, params: any[]): string | null {
		if (!where) return null;

		// Handle binary expressions (=, AND, IN, etc.)
		if (where.type === 'BinaryExpression') {
			const operator = where.operator;

			// Equality: column = value
			if (operator === '=') {
				const columnName = this.extractColumnName(where);
				const value = this.extractValue(where, params);
				if (columnName && value !== null && value !== undefined) {
					return `${columnName}:eq:${JSON.stringify(value)}`;
				}
			}

			// AND: serialize both sides and combine
			if (operator === 'AND') {
				const left = this.serializeWhereClause(where.left, params);
				const right = this.serializeWhereClause(where.right, params);
				if (left && right) {
					return `${left}:and:${right}`;
				}
			}
		}

		// Handle IN expressions
		if (where.type === 'InExpression') {
			const columnName = where.expression.type === 'Identifier' ? where.expression.name : null;
			if (columnName && where.values) {
				const values = where.values
					.map((v: any) => this.extractValueFromExpression(v, params))
					.filter((v: any) => v !== null && v !== undefined);
				if (values.length > 0) {
					return `${columnName}:in:${JSON.stringify(values)}`;
				}
			}
		}

		return null;
	}

	/**
	 * Extract column name from a binary expression
	 */
	private extractColumnName(expr: any): string | null {
		if (expr.left?.type === 'Identifier') {
			return expr.left.name;
		} else if (expr.right?.type === 'Identifier') {
			return expr.right.name;
		}
		return null;
	}

	/**
	 * Extract value from a binary expression
	 */
	private extractValue(expr: any, params: any[]): any {
		if (expr.left?.type === 'Identifier') {
			return this.extractValueFromExpression(expr.right, params);
		} else if (expr.right?.type === 'Identifier') {
			return this.extractValueFromExpression(expr.left, params);
		}
		return null;
	}

	/**
	 * Extract value from an expression node
	 */
	private extractValueFromExpression(expression: any, params: any[]): any {
		if (!expression) return null;
		if (expression.type === 'Literal') {
			return expression.value;
		} else if (expression.type === 'Placeholder') {
			return params[expression.parameterIndex];
		}
		return null;
	}

	/**
	 * Invalidate cache entries for a table
	 *
	 * Used when write operations affect the table
	 */
	invalidateTable(tableName: string): void {
		const prefix = `qp:${tableName}:`;
		for (const key of this.cache.keys()) {
			if (key.startsWith(prefix)) {
				this.cache.delete(key);
			}
		}
	}

	/**
	 * Invalidate cache entries for specific index keys
	 *
	 * Used when INSERT/UPDATE/DELETE affects indexed columns
	 */
	invalidateIndexKeys(indexName: string, keyValues: string[]): void {
		for (const keyValue of keyValues) {
			const cacheKey = this.buildIndexCacheKey(indexName, keyValue);
			this.cache.delete(cacheKey);
		}
	}

	/**
	 * Invalidate all cache entries for an index
	 *
	 * Used when an index is dropped or rebuilt
	 */
	invalidateIndex(indexName: string): void {
		const prefix = `idx:${indexName}:`;
		for (const key of this.cache.keys()) {
			if (key.startsWith(prefix)) {
				this.cache.delete(key);
			}
		}
	}

	/**
	 * Clear all cache entries
	 */
	clear(): void {
		this.cache.clear();
		this.stats = {
			hits: 0,
			misses: 0,
			evictions: 0,
		};
	}

	/**
	 * Get cache statistics
	 */
	getStats(): CacheStats {
		return {
			...this.stats,
			size: this.cache.size,
		};
	}

	/**
	 * Generic get from cache
	 */
	private get<T>(cacheKey: string): T | null {
		const entry = this.cache.get(cacheKey);
		if (!entry) {
			this.stats.misses++;
			return null;
		}

		// Check if expired
		if (Date.now() - entry.timestamp > entry.ttl) {
			this.cache.delete(cacheKey);
			this.stats.misses++;
			return null;
		}

		this.stats.hits++;
		return entry.data as T;
	}

	/**
	 * Generic set to cache
	 */
	private set<T>(cacheKey: string, data: T, ttl: number): void {
		// Evict old entries if cache is too large
		if (this.cache.size >= this.MAX_ENTRIES) {
			this.evictOldest();
		}

		this.cache.set(cacheKey, {
			data,
			timestamp: Date.now(),
			ttl,
		});
	}

	/**
	 * Evict oldest entries when cache grows too large
	 *
	 * Uses LRU-like eviction: remove expired entries first, then oldest entries
	 */
	private evictOldest(): void {
		const now = Date.now();
		const entriesToRemove: string[] = [];

		// First pass: remove expired entries
		for (const [key, entry] of this.cache.entries()) {
			if (now - entry.timestamp > entry.ttl) {
				entriesToRemove.push(key);
			}
		}

		// Remove expired entries
		for (const key of entriesToRemove) {
			this.cache.delete(key);
			this.stats.evictions++;
		}

		// If still over limit, remove oldest entries
		if (this.cache.size >= this.MAX_ENTRIES) {
			const entries = Array.from(this.cache.entries()).sort((a, b) => a[1].timestamp - b[1].timestamp);

			const numToRemove = Math.ceil(this.MAX_ENTRIES * 0.1); // Remove 10% of entries
			for (let i = 0; i < numToRemove && i < entries.length; i++) {
				this.cache.delete(entries[i][0]);
				this.stats.evictions++;
			}
		}
	}

	/**
	 * Get TTL for a query based on the statement type
	 */
	getTTLForQuery(statement: Statement): number {
		if (statement.type === 'SelectStatement') {
			return this.SHARD_ROUTE_TTL;
		}
		// Default TTL
		return this.SHARD_ROUTE_TTL;
	}
}
