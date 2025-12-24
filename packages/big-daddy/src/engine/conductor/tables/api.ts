import type { DropTableStatement, Identifier } from "@databases/sqlite-ast";
import { handleDropIndex, handleShowIndexes } from "../indexes";
import type { QueryHandlerContext, QueryResult } from "../types";
import {
	handleAlterTable,
	handleReshardTable as reshardTableHandler,
} from "./alter";
import { handleDropTable as dropTableHandler } from "./create-drop";
import {
	handleDescribeTable,
	handleShowTables,
	handleTableStats,
} from "./describe";

/**
 * Comprehensive Table Operations API
 *
 * Provides convenient methods for all table operations:
 * - CREATE/DROP tables
 * - ALTER table properties
 * - List and describe tables
 * - Manage indexes
 * - Reshard tables
 *
 * This API is designed to be user-friendly while being implemented on top
 * of the SQL command handlers.
 */
export class TableOperationsAPI {
	constructor(private context: QueryHandlerContext) {}

	/**
	 * List all tables in the database
	 *
	 * @returns Array of table information including name, primary key, shard count
	 * @example
	 * const tables = await tableOps.listTables();
	 * console.log(tables.map(t => t.name));
	 */
	async listTables(): Promise<QueryResult> {
		return handleShowTables(this.context);
	}

	/**
	 * Get detailed information about a table
	 *
	 * Returns:
	 * - Table metadata (primary key, shard key, shard count)
	 * - Shard distribution across nodes
	 * - Virtual indexes and their status
	 * - Column definitions
	 *
	 * @param tableName - Name of the table to describe
	 * @example
	 * const info = await tableOps.describe('users');
	 * console.log(`Table ${info.rows[0].table_name} has ${info.rows[1].total_shards} shards`);
	 */
	async describe(tableName: string): Promise<QueryResult> {
		return handleDescribeTable(tableName, this.context);
	}

	/**
	 * Get statistics about a table
	 *
	 * Returns:
	 * - Total row count
	 * - Row count per shard
	 * - Average rows per shard
	 * - Shard health status
	 *
	 * @param tableName - Name of the table
	 * @example
	 * const stats = await tableOps.getStats('users');
	 * console.log(`Total rows: ${stats.rows[0].total_rows}`);
	 */
	async getStats(tableName: string): Promise<QueryResult> {
		return handleTableStats(tableName, this.context);
	}

	/**
	 * Drop a table
	 *
	 * Removes the table from topology and all storage nodes
	 * Cascades to delete associated indexes
	 *
	 * @param tableName - Name of the table to drop
	 * @param ifExists - If true, silently succeeds if table doesn't exist
	 * @example
	 * await tableOps.drop('users');
	 * // or
	 * await tableOps.drop('users', true); // DROP TABLE IF EXISTS
	 */
	async drop(
		tableName: string,
		ifExists: boolean = false,
	): Promise<QueryResult> {
		const statement: DropTableStatement = {
			type: "DropTableStatement",
			table: { type: "Identifier", name: tableName } as Identifier,
			ifExists,
		};
		return dropTableHandler(statement, this.context);
	}

	/**
	 * Rename a table
	 *
	 * @param oldName - Current table name
	 * @param newName - New table name
	 * @example
	 * await tableOps.rename('users', 'customers');
	 */
	async rename(oldName: string, newName: string): Promise<QueryResult> {
		return handleAlterTable(oldName, "RENAME", newName, this.context);
	}

	/**
	 * Modify table block size
	 *
	 * Block size determines the number of rows loaded at once from disk
	 *
	 * @param tableName - Name of the table
	 * @param blockSize - New block size (1-10000)
	 * @example
	 * await tableOps.setBlockSize('users', 1000);
	 */
	async setBlockSize(
		tableName: string,
		blockSize: number,
	): Promise<QueryResult> {
		return handleAlterTable(
			tableName,
			"MODIFY_BLOCK_SIZE",
			blockSize,
			this.context,
		);
	}

	/**
	 * Reshard a table to a different number of shards
	 *
	 * Initiates an async resharding operation. The operation will progress
	 * in the background and can be monitored via getReshardingStatus()
	 *
	 * Note: Cannot reshard during active resharding operation
	 *
	 * @param tableName - Name of the table
	 * @param newShardCount - Target number of shards (1-256)
	 * @returns Object with change_log_id for tracking progress
	 * @example
	 * const result = await tableOps.reshard('users', 8);
	 * console.log(`Resharding queued with ID: ${result.rows[0].change_log_id}`);
	 */
	async reshard(
		tableName: string,
		newShardCount: number,
	): Promise<QueryResult> {
		return reshardTableHandler(tableName, newShardCount, this.context);
	}

	/**
	 * List all indexes on a table
	 *
	 * @param tableName - Name of the table
	 * @returns Array of index information
	 * @example
	 * const indexes = await tableOps.listIndexes('users');
	 * console.log(indexes.map(i => `${i.index_name} on ${i.columns}`));
	 */
	async listIndexes(tableName: string): Promise<QueryResult> {
		return handleShowIndexes(tableName, this.context);
	}

	/**
	 * Drop an index on a table
	 *
	 * Removes the virtual index from topology and SQLite indexes from shards
	 *
	 * @param indexName - Name of the index to drop
	 * @param tableName - (Optional) Name of the table (for validation)
	 * @param ifExists - If true, silently succeeds if index doesn't exist
	 * @example
	 * await tableOps.dropIndex('idx_email');
	 * // or
	 * await tableOps.dropIndex('idx_email', 'users', true); // IF EXISTS
	 */
	async dropIndex(
		indexName: string,
		tableName?: string,
		ifExists: boolean = false,
	): Promise<QueryResult> {
		return handleDropIndex(indexName, tableName, this.context, ifExists);
	}
}

/**
 * Create a table operations API for a conductor context
 *
 * @param context - Conductor query handler context
 * @returns TableOperationsAPI instance
 * @example
 * const tableOps = createTableOperationsAPI(context);
 * const tables = await tableOps.listTables();
 */
export function createTableOperationsAPI(
	context: QueryHandlerContext,
): TableOperationsAPI {
	return new TableOperationsAPI(context);
}
