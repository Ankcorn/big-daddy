/**
 * Schema utility functions for extracting table metadata
 */

import type { CreateTableStatement } from "@databases/sqlite-ast";
import type { TableMetadata } from "../topology";

/**
 * Parse shard count from raw SQL query
 * Looks for comments with SHARDS specification in SQL comments
 */
function parseShardCountFromQuery(rawQuery: string): number | null {
	// Match /* SHARDS=N */ pattern
	const match = rawQuery.match(/\/\*\s*SHARDS\s*=\s*(\d+)\s*\*\//i);
	return match ? parseInt(match[1]!, 10) : null;
}

/**
 * Extract table metadata from a CREATE TABLE statement
 * Infers all sharding configuration from the table structure and optional SQL comments
 *
 * @example
 * // Default 1 shard with single-column PRIMARY KEY
 * const metadata = extractTableMetadata(statement, 'CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)');
 *
 * // Composite PRIMARY KEY
 * const metadata = extractTableMetadata(statement, 'CREATE TABLE events (user_id INTEGER, tenant_id INTEGER, PRIMARY KEY (user_id, tenant_id))');
 *
 * // Specify 3 shards via comment - use format: CREATE TABLE users /&#42; SHARDS=3 &#42;/ (...)
 * const metadata = extractTableMetadata(statement, rawQuery);
 */
export function extractTableMetadata(
	statement: CreateTableStatement,
	rawQuery?: string,
): Omit<TableMetadata, "created_at" | "updated_at"> {
	const tableName = statement.table.name;

	let primaryKey: string;
	let primaryKeyType: string;
	let shardKey: string;

	// Check for column-level PRIMARY KEY constraint
	const primaryKeyColumn = statement.columns.find((col) =>
		col.constraints?.some((c) => c.constraint === "PRIMARY KEY"),
	);

	if (primaryKeyColumn) {
		// Single-column PRIMARY KEY
		primaryKey = primaryKeyColumn.name.name;
		primaryKeyType = primaryKeyColumn.dataType;
		shardKey = primaryKey;
	} else {
		// Check for table-level PRIMARY KEY constraint from the AST
		const tablePKConstraint = statement.constraints?.find(
			(c) => c.constraint === "PRIMARY KEY",
		);

		if (tablePKConstraint?.columns) {
			// Extract column names from the table-level PRIMARY KEY
			const pkColumns = tablePKConstraint.columns.map((col) => col.name);

			// Store as comma-separated list for composite key
			primaryKey = pkColumns.join(",");

			// Use first column of composite key as shard key
			shardKey = pkColumns[0]!;

			// Find the type of the first column
			const firstCol = statement.columns.find(
				(col) => col.name.name === shardKey,
			);
			primaryKeyType = firstCol?.dataType || "INTEGER";
		} else {
			throw new Error(
				`CREATE TABLE ${tableName} must have a PRIMARY KEY column or constraint`,
			);
		}
	}

	// Parse shard count from SQL comment if provided
	const numShards = (rawQuery && parseShardCountFromQuery(rawQuery)) || 1;

	// Validate shard count
	if (numShards < 1 || numShards > 256) {
		throw new Error(
			`Invalid SHARDS value: ${numShards}. Must be between 1 and 256.`,
		);
	}

	// Default sharding configuration
	// - shard_key: use the primary key (or first column of composite key)
	// - num_shards: parsed from /* SHARDS=N */ comment, default 1
	// - shard_strategy: hash (standard approach)
	// - block_size: 500 rows per block
	return {
		table_name: tableName,
		primary_key: primaryKey,
		primary_key_type: primaryKeyType,
		shard_strategy: "hash",
		shard_key: shardKey,
		num_shards: numShards,
		block_size: 500,
	};
}
