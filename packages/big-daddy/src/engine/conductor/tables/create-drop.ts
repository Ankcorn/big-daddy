import type {
	CreateTableStatement,
	DropTableStatement,
} from "@databases/sqlite-ast";
import { Effect } from "effect";
import { logger } from "../../../logger";
import { extractTableMetadata } from "../../utils/schema-utils";
import type { QueryHandlerContext, QueryResult } from "../types";
import {
	StorageExecutionError,
	TableAlreadyExistsError,
	TopologyFetchError,
	TopologyUpdateError,
} from "../types";
import { injectVirtualShardColumn } from "../utils";

/**
 * Handle CREATE TABLE statement execution
 *
 * This method:
 * 1. Fetches topology to check if table already exists
 * 2. Extracts metadata from the CREATE TABLE statement
 * 3. Adds table metadata to topology
 * 4. Injects _virtualShard column into the query
 * 5. Executes CREATE TABLE on all storage nodes in parallel
 */
export async function handleCreateTable(
	statement: CreateTableStatement,
	query: string,
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const tableName = statement.table.name;
	const { databaseId, storage, topology } = context;

	const effect = Effect.gen(function* () {
		// Step 1: Get topology data
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);

		const topologyData = yield* Effect.tryPromise({
			try: () => topologyStub.getTopology(),
			catch: (error) => new TopologyFetchError(databaseId, error),
		});

		// Step 2: Check if table already exists
		const existingTable = topologyData.tables.find(
			(t) => t.table_name === tableName,
		);

		// If IF NOT EXISTS is specified and table exists, return early
		if (statement.ifNotExists && existingTable) {
			return {
				rows: [],
				rowsAffected: 0,
			};
		}

		// If table exists and IF NOT EXISTS was not specified, fail with typed error
		if (existingTable) {
			return yield* Effect.fail(new TableAlreadyExistsError(tableName));
		}

		// Step 3: Extract metadata from the CREATE TABLE statement
		const metadata = extractTableMetadata(statement, query);

		// Step 4: Add table to topology
		yield* Effect.tryPromise({
			try: () =>
				topologyStub.updateTopology({
					tables: {
						add: [metadata],
					},
				}),
			catch: (error) => new TopologyUpdateError(tableName, error),
		});

		// Step 5: Inject _virtualShard column and create composite primary key
		const modifiedQuery = injectVirtualShardColumn(statement);

		// Step 6: Execute CREATE TABLE on all storage nodes in parallel
		yield* Effect.all(
			topologyData.storage_nodes.map((node) => {
				const storageId = storage.idFromName(node.node_id);
				const storageStub = storage.get(storageId);

				return Effect.tryPromise({
					try: () =>
						storageStub.executeQuery({
							query: modifiedQuery,
							params: [],
						}),
					catch: (error) =>
						new StorageExecutionError(node.node_id, modifiedQuery, error),
				});
			}),
			{ concurrency: "unbounded" }, // Execute in parallel
		);

		// Step 7: Return success result
		return {
			rows: [],
			rowsAffected: 0,
		};
	});

	// Run the Effect and convert errors back to exceptions for now
	return Effect.runPromise(
		effect.pipe(
			Effect.catchAll((error) => {
				// Convert typed errors to standard errors for backward compatibility
				if (error instanceof TableAlreadyExistsError) {
					return Effect.fail(
						new Error(`Table '${error.tableName}' already exists in topology`),
					);
				}
				if (error instanceof TopologyFetchError) {
					return Effect.fail(
						new Error(
							`Failed to fetch topology for database '${error.databaseId}'`,
						),
					);
				}
				if (error instanceof TopologyUpdateError) {
					return Effect.fail(
						new Error(
							`Failed to update topology for table '${error.tableName}'`,
						),
					);
				}
				if (error instanceof StorageExecutionError) {
					return Effect.fail(
						new Error(
							`Failed to execute CREATE TABLE on node '${error.nodeId}'`,
						),
					);
				}
				return Effect.fail(error as Error);
			}),
		),
	);
}

/**
 * Handle DROP TABLE statement execution
 *
 * This method:
 * 1. Fetches topology to check if table exists
 * 2. Validates no active resharding is in progress
 * 3. Deletes table metadata and all associated shards from topology
 * 4. Deletes associated virtual indexes
 * 5. Executes DROP TABLE on all storage nodes in parallel
 */
export async function handleDropTable(
	statement: DropTableStatement,
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const tableName = statement.table.name;
	const { databaseId, storage, topology } = context;

	const effect = Effect.gen(function* () {
		// Step 1: Get topology data
		const topologyId = topology.idFromName(databaseId);
		const topologyStub = topology.get(topologyId);

		const topologyData = yield* Effect.tryPromise({
			try: () => topologyStub.getTopology(),
			catch: (error) => new TopologyFetchError(databaseId, error),
		});

		// Step 2: Check if table exists
		const existingTable = topologyData.tables.find(
			(t) => t.table_name === tableName,
		);

		// If IF EXISTS is specified and table doesn't exist, return early
		if (statement.ifExists && !existingTable) {
			return {
				rows: [],
				rowsAffected: 0,
			};
		}

		// If table doesn't exist, fail
		if (!existingTable) {
			return yield* Effect.fail(new Error(`Table '${tableName}' not found`));
		}

		// Step 3: Check for active resharding
		const reshardingState = topologyData.resharding_states.find(
			(s) => s.table_name === tableName && s.status === "copying",
		);
		if (reshardingState) {
			return yield* Effect.fail(
				new Error(
					`Cannot DROP table '${tableName}' while resharding is in progress`,
				),
			);
		}

		// Step 4: Delete from topology (cascades to shards and indexes via foreign keys)
		yield* Effect.tryPromise({
			try: () =>
				topologyStub.updateTopology({
					tables: {
						remove: [tableName],
					},
				}),
			catch: (error) => new TopologyUpdateError(tableName, error),
		});

		// Step 5: Get the shards for this table to use in DROP
		const tableShards = topologyData.table_shards.filter(
			(s) => s.table_name === tableName,
		);
		const uniqueNodes = [...new Set(tableShards.map((s) => s.node_id))];

		// Step 6: Execute DROP TABLE on all storage nodes that have this table
		yield* Effect.all(
			uniqueNodes.map((nodeId) => {
				const storageId = storage.idFromName(nodeId);
				const storageStub = storage.get(storageId);

				return Effect.tryPromise({
					try: () =>
						storageStub.executeQuery({
							query: `DROP TABLE IF EXISTS "${tableName}"`,
							params: [],
						}),
					catch: (error) =>
						new StorageExecutionError(
							nodeId,
							`DROP TABLE "${tableName}"`,
							error,
						),
				});
			}),
			{ concurrency: "unbounded" }, // Execute in parallel
		);

		// Step 7: Return success result
		logger.info`Table dropped successfully ${{ table: tableName }} ${{ shardsRemoved: tableShards.length }}`;

		return {
			rows: [],
			rowsAffected: 0,
		};
	});

	// Run the Effect and convert errors back to exceptions for now
	return Effect.runPromise(
		effect.pipe(
			Effect.catchAll((error) => {
				if (error instanceof TopologyFetchError) {
					return Effect.fail(
						new Error(
							`Failed to fetch topology for database '${error.databaseId}'`,
						),
					);
				}
				if (error instanceof TopologyUpdateError) {
					return Effect.fail(
						new Error(
							`Failed to update topology for table '${error.tableName}'`,
						),
					);
				}
				if (error instanceof StorageExecutionError) {
					return Effect.fail(
						new Error(`Failed to execute DROP TABLE on node '${error.nodeId}'`),
					);
				}
				return Effect.fail(error as Error);
			}),
		),
	);
}
