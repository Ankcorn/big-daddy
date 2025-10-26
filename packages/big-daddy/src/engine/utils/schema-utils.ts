/**
 * Schema utility functions for extracting table metadata
 */

import type { CreateTableStatement } from '@databases/sqlite-ast';
import type { TableMetadata } from '../topology';

/**
 * Extract table metadata from a CREATE TABLE statement
 * Infers all sharding configuration from the table structure
 */
export function extractTableMetadata(statement: CreateTableStatement): Omit<TableMetadata, 'created_at' | 'updated_at'> {
	const tableName = statement.table.name;

	// Find the primary key column
	const primaryKeyColumn = statement.columns.find((col) => col.constraints?.some((c) => c.constraint === 'PRIMARY KEY'));

	if (!primaryKeyColumn) {
		throw new Error(`CREATE TABLE ${tableName} must have a PRIMARY KEY column`);
	}

	const primaryKey = primaryKeyColumn.name.name;
	const primaryKeyType = primaryKeyColumn.dataType;

	// Default sharding configuration
	// - shard_key: use the primary key
	// - num_shards: 1 (single shard, can be manually updated later via topology)
	// - shard_strategy: hash (standard approach)
	// - block_size: 500 rows per block
	return {
		table_name: tableName,
		primary_key: primaryKey,
		primary_key_type: primaryKeyType,
		shard_strategy: 'hash',
		shard_key: primaryKey,
		num_shards: 1,
		block_size: 500,
	};
}
