import { logger } from "../../../logger";
import type { QueryHandlerContext, QueryResult } from "../types";

/** SQLite PRAGMA table_info result row */
interface TableInfoRow {
	cid: number;
	name: string;
	type: string;
	notnull: number;
	dflt_value: string | null;
	pk: number;
}

/**
 * Handle SHOW TABLES query
 *
 * Returns a list of all tables in the database
 */
export async function handleShowTables(
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, topology } = context;

	try {
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		logger.info`SHOW TABLES executed ${{ tableCount: topologyData.tables.length }}`;

		// Return table names
		const rows = topologyData.tables.map((table) => ({
			name: table.table_name,
			primary_key: table.primary_key,
			shard_key: table.shard_key,
			num_shards: table.num_shards,
			shard_strategy: table.shard_strategy,
			created_at: new Date(table.created_at).toISOString(),
		}));

		return {
			rows,
			rowsAffected: rows.length,
		};
	} catch (error) {
		throw new Error(`Failed to list tables: ${(error as Error).message}`);
	}
}

/**
 * Handle DESCRIBE TABLE query or query info function
 *
 * Returns detailed information about a specific table:
 * - Column definitions (from storage)
 * - Shard distribution
 * - Virtual indexes
 * - Table metadata
 */
export async function handleDescribeTable(
	tableName: string,
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, storage, topology } = context;

	try {
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Step 1: Find table metadata
		const tableMetadata = topologyData.tables.find(
			(t) => t.table_name === tableName,
		);
		if (!tableMetadata) {
			throw new Error(`Table '${tableName}' not found`);
		}

		// Step 2: Get table shards
		const tableShards = topologyData.table_shards.filter(
			(s) => s.table_name === tableName,
		);

		// Step 3: Get virtual indexes for this table
		const tableIndexes = topologyData.virtual_indexes.filter(
			(i) => i.table_name === tableName,
		);

		// Step 4: Get schema from first shard (all shards have same schema)
		let schema: TableInfoRow[] = [];
		if (tableShards.length > 0) {
			const firstShard = tableShards[0];
			const storageId = storage.idFromName(firstShard!.node_id);
			const storageStub = storage.get(storageId);

			try {
				const queryResult: unknown = await storageStub.executeQuery({
					query: `PRAGMA table_info("${tableName}")`,
					params: [],
				});
				schema = (queryResult as { rows: TableInfoRow[] }).rows;
			} catch (error) {
				logger.warn`Failed to fetch schema from storage ${{ table: tableName }} ${{ error: (error as Error).message }}`;
			}
		}

		logger.info`DESCRIBE TABLE executed ${{ table: tableName }} ${{ shardCount: tableShards.length }} ${{ indexCount: tableIndexes.length }} ${{ columnCount: schema.length }}`;

		// Build response
		const rows = [
			{
				section: "TABLE_INFO",
				table_name: tableName,
				primary_key: tableMetadata.primary_key,
				shard_key: tableMetadata.shard_key,
				shard_strategy: tableMetadata.shard_strategy,
				num_shards: tableMetadata.num_shards,
				block_size: tableMetadata.block_size,
				created_at: new Date(tableMetadata.created_at).toISOString(),
				updated_at: new Date(tableMetadata.updated_at).toISOString(),
			},
			{
				section: "SHARDS",
				total_shards: tableShards.length,
				shards: tableShards.map((s) => ({
					shard_id: s.shard_id,
					node_id: s.node_id,
					status: s.status,
				})),
			},
			{
				section: "INDEXES",
				total_indexes: tableIndexes.length,
				indexes: tableIndexes.map((idx) => ({
					index_name: idx.index_name,
					columns: JSON.parse(idx.columns),
					index_type: idx.index_type,
					status: idx.status,
				})),
			},
			{
				section: "COLUMNS",
				columns: schema.map((col) => ({
					name: col.name,
					type: col.type,
					notnull: col.notnull === 1,
					default_value: col.dflt_value,
					primary_key: col.pk === 1,
				})),
			},
		];

		return {
			rows,
			rowsAffected: rows.length,
		};
	} catch (error) {
		throw new Error(`Failed to describe table: ${(error as Error).message}`);
	}
}

/**
 * Handle getting table statistics
 *
 * Returns table-level statistics:
 * - Estimated row count per shard
 * - Shard distribution
 * - Index statistics
 */
export async function handleTableStats(
	tableName: string,
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, storage, topology } = context;

	try {
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find table
		const tableMetadata = topologyData.tables.find(
			(t) => t.table_name === tableName,
		);
		if (!tableMetadata) {
			throw new Error(`Table '${tableName}' not found`);
		}

		// Get shards
		const tableShards = topologyData.table_shards.filter(
			(s) => s.table_name === tableName && s.status === "active",
		);

		// Fetch row count from each shard
		const shardStats = await Promise.all(
			tableShards.map(async (shard) => {
				const storageId = storage.idFromName(shard.node_id);
				const storageStub = storage.get(storageId);

				try {
					const queryResult: unknown = await storageStub.executeQuery({
						query: `SELECT COUNT(*) as row_count FROM "${tableName}" WHERE _virtualShard = ?`,
						params: [shard.shard_id],
					});
					const result = queryResult as { rows: { row_count: number }[] };
					const firstRow = result.rows[0];
					const rowCount = firstRow?.row_count || 0;
					return {
						shard_id: shard.shard_id,
						node_id: shard.node_id,
						row_count: rowCount,
						status: shard.status,
					};
				} catch (error) {
					logger.warn`Failed to get shard stats ${{ table: tableName }} ${{ shard: shard.shard_id }} ${{ error: (error as Error).message }}`;
					return {
						shard_id: shard.shard_id,
						node_id: shard.node_id,
						row_count: 0,
						status: "error",
						error: (error as Error).message,
					};
				}
			}),
		);

		const totalRows = shardStats.reduce(
			(sum, s) => sum + (s.row_count || 0),
			0,
		);

		logger.info`TABLE STATS executed ${{ table: tableName }} ${{ totalRows }} ${{ shardCount: tableShards.length }}`;

		return {
			rows: [
				{
					table_name: tableName,
					total_rows: totalRows,
					total_shards: tableShards.length,
					avg_rows_per_shard:
						tableShards.length > 0
							? Math.round(totalRows / tableShards.length)
							: 0,
					shard_stats: shardStats,
				},
			],
			rowsAffected: 1,
		};
	} catch (error) {
		throw new Error(`Failed to get table stats: ${(error as Error).message}`);
	}
}
