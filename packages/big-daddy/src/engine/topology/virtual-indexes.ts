/**
 * Virtual index management for Topology
 *
 * Handles all virtual index operations:
 * - createVirtualIndex: Create a new index
 * - updateIndexStatus: Update index build status
 * - batchUpsertIndexEntries: Bulk update index mappings
 * - getIndexedShards: Look up shards by indexed value
 * - addShardToIndexEntry: Add shard to index entry
 * - removeShardFromIndexEntry: Remove shard from index
 * - dropVirtualIndex: Delete an index
 * - getIndexEntriesForShard: Get all entries for a shard
 * - batchMaintainIndexes: Bulk maintain index entries
 * - getShardsFromIndexedWhere: Extract shards from indexed queries (helper)
 * - maintainIndexesForInsert: Maintain indexes for INSERT operations (helper)
 */

import type {
	Expression,
	InsertStatement,
	Literal,
	Placeholder,
} from "@databases/sqlite-ast";
import type { IndexStatus, SqlParam } from "./types";

export class VirtualIndexOperations {
	constructor(private storage: DurableObjectStorage) {}

	/**
	 * Create a virtual index
	 *
	 * @param indexName - Unique name for the index
	 * @param tableName - Table to index
	 * @param columns - Columns to index
	 * @param indexType - Type of index ('hash' or 'unique')
	 * @returns Success status or error
	 */
	async createVirtualIndex(
		indexName: string,
		tableName: string,
		columns: string[],
		indexType: "hash" | "unique",
	): Promise<{ success: boolean; error?: string }> {
		// Check if index already exists
		const existing = this.storage.sql
			.exec(
				`SELECT index_name FROM virtual_indexes WHERE index_name = ?`,
				indexName,
			)
			.toArray() as unknown as { index_name: string }[];

		if (existing.length > 0) {
			return { success: false, error: `Index '${indexName}' already exists` };
		}

		// Check if table exists
		const tableExists = this.storage.sql
			.exec(`SELECT table_name FROM tables WHERE table_name = ?`, tableName)
			.toArray() as unknown as { table_name: string }[];

		if (tableExists.length === 0) {
			return { success: false, error: `Table '${tableName}' does not exist` };
		}

		const now = Date.now();

		// Create index with 'building' status
		// Store columns as JSON array
		this.storage.sql.exec(
			`INSERT INTO virtual_indexes (index_name, table_name, columns, index_type, status, error_message, created_at, updated_at)
			 VALUES (?, ?, ?, ?, 'building', NULL, ?, ?)`,
			indexName,
			tableName,
			JSON.stringify(columns),
			indexType,
			now,
			now,
		);

		return { success: true };
	}

	/**
	 * Update virtual index status
	 *
	 * @param indexName - Index to update
	 * @param status - New status
	 * @param errorMessage - Optional error message for failed status
	 */
	async updateIndexStatus(
		indexName: string,
		status: IndexStatus,
		errorMessage?: string,
	): Promise<void> {
		const now = Date.now();

		this.storage.sql.exec(
			`UPDATE virtual_indexes SET status = ?, error_message = ?, updated_at = ? WHERE index_name = ?`,
			status,
			errorMessage ?? null,
			now,
			indexName,
		);
	}

	/**
	 * Batch upsert multiple virtual index entries
	 *
	 * Writes entries one at a time to avoid SQLite variable limits and handle variable-length values.
	 * This is safe and efficient because Durable Object writes are extremely fast (single-digit ms).
	 *
	 * @param indexName - Index name
	 * @param entries - Array of entries to upsert, each with keyValue and shardIds
	 * @returns Number of entries upserted
	 */
	async batchUpsertIndexEntries(
		indexName: string,
		entries: Array<{ keyValue: string; shardIds: number[] }>,
	): Promise<{ count: number }> {
		if (entries.length === 0) {
			return { count: 0 };
		}

		const now = Date.now();

		// Write entries one at a time - DO writes are fast enough that this is not a bottleneck
		for (const entry of entries) {
			this.storage.sql.exec(
				`INSERT OR REPLACE INTO virtual_index_entries (index_name, key_value, shard_ids, updated_at)
				 VALUES (?, ?, ?, ?)`,
				indexName,
				entry.keyValue,
				JSON.stringify(entry.shardIds),
				now,
			);
		}

		return { count: entries.length };
	}

	/**
	 * Get shard IDs for a specific indexed value
	 *
	 * @param indexName - Index name
	 * @param keyValue - The indexed value
	 * @returns Array of shard IDs, or null if not found
	 */
	async getIndexedShards(
		indexName: string,
		keyValue: string,
	): Promise<number[] | null> {
		const result = this.storage.sql
			.exec(
				`SELECT shard_ids FROM virtual_index_entries WHERE index_name = ? AND key_value = ?`,
				indexName,
				keyValue,
			)
			.toArray() as unknown as { shard_ids: string }[];

		if (result.length === 0) {
			return null;
		}

		return JSON.parse(result[0]!.shard_ids) as number[];
	}

	/**
	 * Add a shard ID to an index entry for a specific value
	 * Used for synchronous index maintenance during INSERT operations
	 *
	 * @param indexName - Index name
	 * @param keyValue - The indexed value
	 * @param shardId - Shard ID to add
	 */
	async addShardToIndexEntry(
		indexName: string,
		keyValue: string,
		shardId: number,
	): Promise<void> {
		const now = Date.now();

		// Get existing entry
		const existing = await this.getIndexedShards(indexName, keyValue);

		if (existing === null) {
			// Create new entry with this shard
			this.storage.sql.exec(
				`INSERT INTO virtual_index_entries (index_name, key_value, shard_ids, updated_at)
				 VALUES (?, ?, ?, ?)`,
				indexName,
				keyValue,
				JSON.stringify([shardId]),
				now,
			);
		} else {
			// Add shard to existing entry if not already present
			if (!existing.includes(shardId)) {
				const updatedShardIds = [...existing, shardId].sort((a, b) => a - b);
				this.storage.sql.exec(
					`UPDATE virtual_index_entries SET shard_ids = ?, updated_at = ?
					 WHERE index_name = ? AND key_value = ?`,
					JSON.stringify(updatedShardIds),
					now,
					indexName,
					keyValue,
				);
			}
		}
	}

	/**
	 * Remove a shard ID from an index entry for a specific value
	 * Used for synchronous index maintenance during DELETE and UPDATE operations
	 * Deletes the entry if it becomes empty
	 *
	 * @param indexName - Index name
	 * @param keyValue - The indexed value
	 * @param shardId - Shard ID to remove
	 */
	async removeShardFromIndexEntry(
		indexName: string,
		keyValue: string,
		shardId: number,
	): Promise<void> {
		const now = Date.now();

		// Get existing entry
		const existing = await this.getIndexedShards(indexName, keyValue);

		if (existing === null || !existing.includes(shardId)) {
			// Entry doesn't exist or shard not present - nothing to do
			return;
		}

		// Remove shard from list
		const updatedShardIds = existing.filter((id) => id !== shardId);

		if (updatedShardIds.length === 0) {
			// No more shards - delete the entry
			this.storage.sql.exec(
				`DELETE FROM virtual_index_entries WHERE index_name = ? AND key_value = ?`,
				indexName,
				keyValue,
			);
		} else {
			// Update with remaining shards
			this.storage.sql.exec(
				`UPDATE virtual_index_entries SET shard_ids = ?, updated_at = ?
				 WHERE index_name = ? AND key_value = ?`,
				JSON.stringify(updatedShardIds),
				now,
				indexName,
				keyValue,
			);
		}
	}

	/**
	 * Delete a virtual index and all its entries
	 *
	 * @param indexName - Index to delete
	 */
	async dropVirtualIndex(indexName: string): Promise<{ success: boolean }> {
		// CASCADE will automatically delete index entries
		this.storage.sql.exec(
			`DELETE FROM virtual_indexes WHERE index_name = ?`,
			indexName,
		);

		return { success: true };
	}

	/**
	 * Get all index entries that include a specific shard
	 * Used by async index maintenance to rebuild index state
	 */
	async getIndexEntriesForShard(
		indexName: string,
		shardId: number,
	): Promise<Array<{ key_value: string }>> {
		const entries = this.storage.sql
			.exec(
				`SELECT key_value, shard_ids FROM virtual_index_entries
		     WHERE index_name = ?`,
				indexName,
			)
			.toArray() as Array<{ key_value: string; shard_ids: string }>;

		return entries
			.filter((entry) => {
				const shardIds = JSON.parse(entry.shard_ids);
				return shardIds.includes(shardId);
			})
			.map((entry) => ({ key_value: entry.key_value }));
	}

	/**
	 * Apply a batch of index changes in a single call
	 * Used by async index maintenance queue consumer
	 */
	async batchMaintainIndexes(
		changes: Array<{
			operation: "add" | "remove";
			index_name: string;
			key_value: string;
			shard_id: number;
		}>,
	): Promise<void> {
		for (const change of changes) {
			if (change.operation === "add") {
				await this.addShardToIndexEntry(
					change.index_name,
					change.key_value,
					change.shard_id,
				);
			} else {
				await this.removeShardFromIndexEntry(
					change.index_name,
					change.key_value,
					change.shard_id,
				);
			}
		}
	}

	/**
	 * Check if WHERE clause can use a virtual index to reduce shard fan-out
	 * Private helper method used by query planning
	 */
	async getShardsFromIndexedWhere(
		where: Expression,
		_tableName: string,
		tableShards: Array<{
			table_name: string;
			shard_id: number;
			node_id: string;
		}>,
		virtualIndexes: Array<{
			index_name: string;
			columns: string;
			index_type: "hash" | "unique";
		}>,
		params: SqlParam[],
	): Promise<Array<{
		table_name: string;
		shard_id: number;
		node_id: string;
	}> | null> {
		// Try to match composite index from AND conditions
		if (where.type === "BinaryExpression" && where.operator === "AND") {
			const columnValues = this.extractColumnValuesFromAnd(where, params);

			if (columnValues.size > 0) {
				const index = virtualIndexes.find((idx) => {
					const indexColumns = JSON.parse(idx.columns);

					if (indexColumns.length === 1) {
						return columnValues.has(indexColumns[0]);
					}

					// Multi-column index - check leftmost prefix
					let matchCount = 0;
					for (let i = 0; i < indexColumns.length; i++) {
						if (columnValues.has(indexColumns[i])) {
							matchCount++;
						} else {
							break;
						}
					}

					return (
						matchCount > 0 &&
						matchCount === Math.min(columnValues.size, indexColumns.length)
					);
				});

				if (index) {
					const indexColumns = JSON.parse(index.columns);
					let keyValue: string;

					if (indexColumns.length === 1) {
						const value = columnValues.get(indexColumns[0])!;
						if (value === null || value === undefined) {
							return null;
						}
						keyValue = String(value);
					} else {
						const values: SqlParam[] = [];
						for (const col of indexColumns) {
							const value = columnValues.get(col);
							if (value === undefined) break;
							if (value === null) return null;
							values.push(value);
						}
						if (values.length === 0) return null;
						keyValue =
							values.length === 1 ? String(values[0]) : JSON.stringify(values);
					}

					const shardIds = await this.getIndexedShards(
						index.index_name,
						keyValue,
					);
					if (shardIds === null || shardIds.length === 0) {
						return [];
					}

					return tableShards.filter((s) => shardIds.includes(s.shard_id));
				}
			}
		}

		// Try to match single equality condition
		if (where.type === "BinaryExpression" && where.operator === "=") {
			let columnName: string | null = null;
			let valueExpression: Expression | null = null;

			if (where.left.type === "Identifier") {
				columnName = where.left.name;
				valueExpression = where.right;
			} else if (where.right.type === "Identifier") {
				columnName = where.right.name;
				valueExpression = where.left;
			}

			if (!columnName || !valueExpression) {
				return null;
			}

			const index = virtualIndexes.find((idx) => {
				const columns = JSON.parse(idx.columns);
				return columns.length === 1 && columns[0] === columnName;
			});

			if (!index) {
				return null;
			}

			const value = this.extractValueFromExpression(valueExpression, params);
			if (value === null || value === undefined) {
				return null;
			}

			const keyValue = String(value);
			const shardIds = await this.getIndexedShards(index.index_name, keyValue);

			if (shardIds === null || shardIds.length === 0) {
				return [];
			}

			return tableShards.filter((s) => shardIds.includes(s.shard_id));
		}

		// Try to match IN query
		if (where.type === "InExpression") {
			const columnName =
				where.expression.type === "Identifier" ? where.expression.name : null;

			if (!columnName) {
				return null;
			}

			const index = virtualIndexes.find((idx) => {
				const columns = JSON.parse(idx.columns);
				return columns.length === 1 && columns[0] === columnName;
			});

			if (!index) {
				return null;
			}

			const values: SqlParam[] = [];
			for (const item of where.values) {
				const value = this.extractValueFromExpression(item, params);
				if (value !== null && value !== undefined) {
					values.push(value);
				}
			}

			if (values.length === 0) {
				return null;
			}

			const allShardIds = new Set<number>();
			for (const value of values) {
				const keyValue = String(value);
				const shardIds = await this.getIndexedShards(
					index.index_name,
					keyValue,
				);

				if (shardIds && shardIds.length > 0) {
					for (const id of shardIds) {
						allShardIds.add(id);
					}
				}
			}

			if (allShardIds.size === 0) {
				return [];
			}

			return tableShards.filter((s) => allShardIds.has(s.shard_id));
		}

		return null;
	}

	/**
	 * Maintain indexes for INSERT operation
	 * Called from getQueryPlanData before the INSERT executes
	 */
	async maintainIndexesForInsert(
		statement: InsertStatement,
		indexes: Array<{ index_name: string; columns: string }>,
		shard: { table_name: string; shard_id: number; node_id: string },
		params: SqlParam[],
	): Promise<void> {
		// Extract values from the INSERT statement
		if (!statement.columns || statement.values.length === 0) {
			return;
		}

		const row = statement.values[0]; // Only handle single-row inserts for now

		// For each index, extract the value(s) and update the index entry
		for (const index of indexes) {
			const indexColumns = JSON.parse(index.columns);

			// Build composite key from all indexed columns
			const values: SqlParam[] = [];
			let hasNull = false;

			for (const colName of indexColumns) {
				const columnIndex =
					statement.columns?.findIndex((col) => col.name === colName) ?? -1;
				if (columnIndex === -1) {
					// This column is not in the INSERT - skip this index
					hasNull = true;
					break;
				}

				const valueExpression = row?.[columnIndex];
				const value = this.extractValueFromExpression(valueExpression!, params);

				if (value === null || value === undefined) {
					// NULL values are not indexed
					hasNull = true;
					break;
				}

				values.push(value);
			}

			if (hasNull || values.length !== indexColumns.length) {
				continue;
			}

			// Build the key value
			const keyValue =
				indexColumns.length === 1 ? String(values[0]) : JSON.stringify(values);

			// Add this shard to the index entry for this value
			await this.addShardToIndexEntry(
				index.index_name,
				keyValue,
				shard.shard_id,
			);
		}
	}

	/**
	 * Maintain indexes for DELETE operation
	 * Called synchronously after DELETE executes to remove index entries
	 *
	 * @param rows - The rows that were deleted (captured before DELETE)
	 * @param indexes - Virtual indexes on this table
	 * @param shardId - The shard where rows were deleted
	 */
	async maintainIndexesForDelete(
		rows: Record<string, SqlParam>[],
		indexes: Array<{ index_name: string; columns: string }>,
		shardId: number,
	): Promise<void> {
		for (const index of indexes) {
			const indexColumns = JSON.parse(index.columns) as string[];

			for (const row of rows) {
				const keyValue = this.buildKeyValueFromRow(row, indexColumns);
				if (keyValue === null) continue;

				await this.removeShardFromIndexEntry(
					index.index_name,
					keyValue,
					shardId,
				);
			}
		}
	}

	/**
	 * Maintain indexes for UPDATE operation
	 * Called synchronously after UPDATE executes to update index entries
	 *
	 * Compares old vs new values and:
	 * - Removes entries for values that changed (old value no longer in this shard)
	 * - Adds entries for new values
	 *
	 * @param oldRows - Rows before UPDATE
	 * @param newRows - Rows after UPDATE
	 * @param indexes - Virtual indexes on this table
	 * @param shardId - The shard where rows were updated
	 */
	async maintainIndexesForUpdate(
		oldRows: Record<string, SqlParam>[],
		newRows: Record<string, SqlParam>[],
		indexes: Array<{ index_name: string; columns: string }>,
		shardId: number,
	): Promise<void> {
		for (const index of indexes) {
			const indexColumns = JSON.parse(index.columns) as string[];

			// Build sets of old and new key values
			const oldKeyValues = new Set<string>();
			const newKeyValues = new Set<string>();

			for (const row of oldRows) {
				const keyValue = this.buildKeyValueFromRow(row, indexColumns);
				if (keyValue !== null) {
					oldKeyValues.add(keyValue);
				}
			}

			for (const row of newRows) {
				const keyValue = this.buildKeyValueFromRow(row, indexColumns);
				if (keyValue !== null) {
					newKeyValues.add(keyValue);
				}
			}

			// Remove entries for values that are no longer present
			for (const keyValue of oldKeyValues) {
				if (!newKeyValues.has(keyValue)) {
					await this.removeShardFromIndexEntry(
						index.index_name,
						keyValue,
						shardId,
					);
				}
			}

			// Add entries for new values
			for (const keyValue of newKeyValues) {
				if (!oldKeyValues.has(keyValue)) {
					await this.addShardToIndexEntry(index.index_name, keyValue, shardId);
				}
			}
		}
	}

	/**
	 * Build a key value for indexed column(s) from a row
	 * Returns null if any indexed column is NULL (NULL values are not indexed)
	 * Private helper shared by maintainIndexesForDelete/Update
	 */
	private buildKeyValueFromRow(
		row: Record<string, SqlParam>,
		columns: string[],
	): string | null {
		if (columns.length === 1) {
			const value = row[columns[0]!];
			if (value === null || value === undefined) {
				return null;
			}
			return String(value);
		} else {
			// Composite index - build key from all column values
			const values = columns.map((col) => row[col]);
			if (values.some((v) => v === null || v === undefined)) {
				return null;
			}
			return JSON.stringify(values);
		}
	}

	/**
	 * Extract column-value pairs from AND conditions
	 * Private helper
	 */
	private extractColumnValuesFromAnd(
		where: Expression,
		params: SqlParam[],
	): Map<string, SqlParam> {
		const columnValues = new Map<string, SqlParam>();

		const extract = (node: Expression) => {
			if (node.type === "BinaryExpression") {
				if (node.operator === "AND") {
					extract(node.left);
					extract(node.right);
				} else if (node.operator === "=") {
					let columnName: string | null = null;
					let valueExpression: Expression | null = null;

					if (node.left.type === "Identifier") {
						columnName = node.left.name;
						valueExpression = node.right;
					} else if (node.right.type === "Identifier") {
						columnName = node.right.name;
						valueExpression = node.left;
					}

					if (columnName && valueExpression) {
						const value = this.extractValueFromExpression(
							valueExpression,
							params,
						);
						columnValues.set(columnName, value);
					}
				}
			}
		};

		extract(where);
		return columnValues;
	}

	/**
	 * Extract value from an expression node
	 * Private helper
	 */
	private extractValueFromExpression(
		expression: Expression,
		params: SqlParam[],
	): SqlParam {
		if (!expression) {
			return null;
		}
		if (expression.type === "Literal") {
			return (expression as Literal).value;
		} else if (expression.type === "Placeholder") {
			return params[(expression as Placeholder).parameterIndex]!;
		}
		return null;
	}
}
