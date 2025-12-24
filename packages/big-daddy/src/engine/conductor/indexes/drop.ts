import { logger } from "../../../logger";
import type { QueryHandlerContext, QueryResult } from "../types";

/**
 * Handle DROP INDEX statement execution
 *
 * This method:
 * 1. Fetches topology to check if index exists
 * 2. Removes the virtual index definition from topology
 * 3. Executes DROP INDEX on all storage shards in parallel
 */
export async function handleDropIndex(
	indexName: string,
	tableName: string | undefined,
	context: QueryHandlerContext,
	ifExists: boolean = false,
): Promise<QueryResult> {
	const { databaseId, storage, topology } = context;

	try {
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Step 1: Find the index
		let index = topologyData.virtual_indexes.find(
			(i) => i.index_name === indexName,
		);

		// If tableName provided, also verify it matches
		if (index && tableName && index.table_name !== tableName) {
			index = undefined;
		}

		// If index not found
		if (!index) {
			if (ifExists) {
				logger.info`DROP INDEX IF EXISTS - index not found ${{ indexName }}`;
				return {
					rows: [],
					rowsAffected: 0,
				};
			}
			throw new Error(`Index '${indexName}' not found`);
		}

		const actualTableName = index.table_name;

		// Step 2: Get all shards for this table
		const tableShards = topologyData.table_shards.filter(
			(s) => s.table_name === actualTableName,
		);
		const uniqueNodes = [...new Set(tableShards.map((s) => s.node_id))];

		// Step 3: Remove virtual index from topology
		await topologyStub.dropVirtualIndex(indexName);

		// Step 4: Execute DROP INDEX on all storage nodes
		await Promise.all(
			uniqueNodes.map(async (nodeId) => {
				const storageId = storage.idFromName(nodeId);
				const storageStub = storage.get(storageId);

				try {
					await storageStub.executeQuery({
						query: `DROP INDEX IF EXISTS "${indexName}"`,
						params: [],
					});
				} catch (error) {
					// Log but don't fail - SQLite might not have the index on this shard
					logger.warn`Failed to drop index on storage node ${{ indexName }} ${{ nodeId }} ${{ error: (error as Error).message }}`;
				}
			}),
		);

		logger.info`Index dropped successfully ${{ indexName }} ${{ table: actualTableName }} ${{ nodesAffected: uniqueNodes.length }}`;

		return {
			rows: [],
			rowsAffected: 0,
		};
	} catch (error) {
		throw new Error(`Failed to drop index: ${(error as Error).message}`);
	}
}

/**
 * Handle SHOW INDEXES for a table
 *
 * Lists all indexes on a specific table
 */
export async function handleShowIndexes(
	tableName: string,
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, topology } = context;

	try {
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);
		const topologyData = await topologyStub.getTopology();

		// Find table
		const table = topologyData.tables.find((t) => t.table_name === tableName);
		if (!table) {
			throw new Error(`Table '${tableName}' not found`);
		}

		// Get indexes for this table
		const indexes = topologyData.virtual_indexes.filter(
			(i) => i.table_name === tableName,
		);

		logger.info`SHOW INDEXES executed ${{ table: tableName }} ${{ indexCount: indexes.length }}`;

		const rows = indexes.map((idx) => ({
			index_name: idx.index_name,
			table_name: idx.table_name,
			columns: JSON.parse(idx.columns),
			index_type: idx.index_type,
			status: idx.status,
			created_at: new Date(idx.created_at).toISOString(),
		}));

		return {
			rows,
			rowsAffected: rows.length,
		};
	} catch (error) {
		throw new Error(`Failed to list indexes: ${(error as Error).message}`);
	}
}
