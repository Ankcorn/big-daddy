import type {
	Identifier,
	Literal,
	PragmaStatement,
} from "@databases/sqlite-ast";
import { logger } from "../../../logger";
import type { ReshardTableJob } from "../../queue/types";
import type { QueryHandlerContext, QueryResult } from "../types";

/**
 * Handle PRAGMA statements
 */
export async function handlePragma(
	statement: PragmaStatement,
	_query: string,
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const pragmaName = statement.name.toLowerCase();

	if (pragmaName === "reshardtable") {
		return handleReshardTable(statement, context);
	}

	throw new Error(`Unsupported PRAGMA: ${statement.name}`);
}

/**
 * Handle PRAGMA reshardTable(table_name, shard_count)
 * Initiates a resharding operation for a table
 */
async function handleReshardTable(
	stmt: PragmaStatement,
	context: QueryHandlerContext,
): Promise<QueryResult> {
	const { databaseId, topology, indexQueue } = context;

	if (!stmt.arguments || stmt.arguments.length !== 2) {
		throw new Error(
			"PRAGMA reshardTable requires exactly 2 arguments: table name and shard count",
		);
	}

	// Extract table name from first argument
	const tableNameArg = stmt.arguments[0]!;
	let tableName: string;
	if (tableNameArg.type === "Literal") {
		tableName = (tableNameArg as Literal).value as string;
	} else if (tableNameArg.type === "Identifier") {
		tableName = (tableNameArg as Identifier).name;
	} else {
		throw new Error(
			"First argument to PRAGMA reshardTable must be a table name",
		);
	}

	// Extract shard count from second argument
	const shardCountArg = stmt.arguments[1]!;
	let shardCount: number;
	if (shardCountArg.type === "Literal") {
		shardCount = (shardCountArg as Literal).value as number;
	} else if (shardCountArg.type === "Identifier") {
		shardCount = parseInt((shardCountArg as Identifier).name, 10);
	} else {
		throw new Error("Second argument to PRAGMA reshardTable must be a number");
	}

	if (!Number.isInteger(shardCount) || shardCount < 1 || shardCount > 256) {
		throw new Error(
			`Invalid shard count: ${shardCount}. Must be an integer between 1 and 256`,
		);
	}

	// Get topology stub
	const topologyId = topology.idFromName(databaseId);
	const topologyStub = topology.get(topologyId);

	// Phase 1: Create pending shards and resharding state
	const changeLogId = crypto.randomUUID();
	const newShards = await topologyStub.createPendingShards(
		tableName,
		shardCount,
		changeLogId,
	);

	logger.info`Pending shards created for resharding ${{ table: tableName }} ${{ shardCount: newShards.length }} ${{ changeLogId }}`;

	// Get table metadata for the resharding job
	const topologyData = await topologyStub.getTopology();
	const tableMetadata = topologyData.tables.find(
		(t) => t.table_name === tableName,
	);
	if (!tableMetadata) {
		throw new Error(`Table '${tableName}' not found in topology`);
	}

	// Phase 2: Enqueue ReshardTableJob for each active source shard
	// When resharding from multiple shards to a different number, we need to handle each source shard
	const sourceShardIds = topologyData.table_shards
		.filter((s) => s.table_name === tableName && s.status === "active")
		.sort((a, b) => a.shard_id - b.shard_id)
		.map((s) => s.shard_id);

	if (sourceShardIds.length === 0) {
		throw new Error(`No active shards found for table '${tableName}'`);
	}

	// Enqueue a resharding job for each source shard
	if (indexQueue) {
		for (const sourceShardId of sourceShardIds) {
			const jobId = crypto.randomUUID();

			await indexQueue.send({
				type: "reshard_table",
				database_id: databaseId,
				table_name: tableName,
				source_shard_id: sourceShardId,
				target_shard_ids: newShards.map((s) => s.shard_id),
				shard_key: tableMetadata.shard_key,
				shard_strategy: tableMetadata.shard_strategy,
				change_log_id: changeLogId,
				created_at: new Date().toISOString(),
				correlation_id: context.correlationId,
			} as ReshardTableJob);

			logger.info`Resharding job enqueued for source shard ${{ table: tableName }} ${{ jobId }} ${{ sourceShardId }} ${{ targetShardIds: newShards.map((s) => s.shard_id) }}`;
		}
	}

	logger.info`All resharding jobs enqueued ${{ table: tableName }} ${{ sourceShardCount: sourceShardIds.length }} ${{ targetShardCount: newShards.length }}`;

	return {
		rows: [
			{
				change_log_id: changeLogId,
				status: "queued",
				message: `Resharding ${tableName} from ${sourceShardIds.length} shards to ${shardCount} shards`,
				table_name: tableName,
				source_shard_count: sourceShardIds.length,
				target_shard_count: shardCount,
			},
		],
	};
}
