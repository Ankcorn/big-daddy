/**
 * Build Index Job Handler
 *
 * Builds a virtual index from existing data across all shards.
 *
 * Process:
 * 1. Get all shards for this table from topology
 * 2. For each distinct value in the indexed column(s):
 *    - Query all shards to find which contain that value
 *    - Create index entry mapping value → shard_ids
 * 3. Update index status to 'ready' or 'failed'
 */

import { Data, Effect } from "effect";
import { createConductor } from "../../index";
import type { IndexBuildJob } from "../queue/types";
import type { Topology } from "../topology";

/**
 * Error types for build index operations
 */
export class NoShardsFoundError extends Data.TaggedError("NoShardsFoundError")<{
	tableName: string;
}> {}

export class TopologyFetchError extends Data.TaggedError("TopologyFetchError")<{
	databaseId: string;
	cause?: unknown;
}> {}

export class IndexEntriesUpsertError extends Data.TaggedError(
	"IndexEntriesUpsertError",
)<{
	indexName: string;
	cause?: unknown;
}> {}

export class IndexStatusUpdateError extends Data.TaggedError(
	"IndexStatusUpdateError",
)<{
	indexName: string;
	status: "ready" | "failed";
	cause?: unknown;
}> {}

/**
 * Build a composite key value from row data
 */
function buildKeyValue(
	row: Record<string, unknown>,
	columns: string[],
): string | null {
	if (columns.length === 1) {
		const value = row[columns[0]!];
		// Skip NULL values
		if (value === null || value === undefined) {
			return null;
		}
		return String(value);
	} else {
		// Composite index - build key from all column values
		const values = columns.map((col) => row[col]);
		// Skip if any value is NULL
		if (values.some((v) => v === null || v === undefined)) {
			return null;
		}
		// Store as JSON array
		return JSON.stringify(values);
	}
}

/**
 * Build index Effect implementation
 *
 * Queries distinct values via the conductor (which handles shard routing).
 * Then creates index entries mapping values to all shards containing the table.
 */
function processBuildIndexJobEffect(
	job: IndexBuildJob,
	env: Env,
	correlationId?: string,
): Effect.Effect<
	void,
	| NoShardsFoundError
	| TopologyFetchError
	| IndexEntriesUpsertError
	| IndexStatusUpdateError
> {
	const topologyId = env.TOPOLOGY.idFromName(job.database_id);
	const topologyStub = env.TOPOLOGY.get(
		topologyId,
	) as DurableObjectStub<Topology>;

	return Effect.gen(function* () {
		const conductor = createConductor(
			job.database_id,
			correlationId ?? "",
			env,
		);

		// 1. Query distinct values with shard info via conductor
		// The _virtualShard column tracks which shard each row is on
		const columnList = job.columns.join(", ");
		const distinctQuery = `SELECT DISTINCT ${columnList}, _virtualShard FROM ${job.table_name}`;

		type DistinctRow = Record<string, unknown> & { _virtualShard: number };

		const queryResult = yield* Effect.tryPromise({
			try: async () => {
				return await conductor.sql<DistinctRow>([
					distinctQuery,
				] as unknown as TemplateStringsArray);
			},
			catch: (error) => {
				// Extract the original error message from nested Effect/DO errors
				// Effect errors may be wrapped multiple layers deep
				let errorMessage: string;

				// Extract error message - Effect's runPromise may lose TaggedError properties,
				// but we ensure the message contains the original error text
				if (error instanceof Error) {
					errorMessage = error.message;
				} else {
					errorMessage = String(error);
				}
				return new TopologyFetchError({
					databaseId: job.database_id,
					cause: errorMessage,
				});
			},
		});

		// 2. Build index entries mapping distinct values to their shards
		// Group by value to collect all shards that contain each distinct value
		const valueToShards = new Map<string, Set<number>>();

		for (const row of queryResult.rows) {
			const keyValue = buildKeyValue(row, job.columns);
			if (keyValue === null) continue;

			const shardId = row._virtualShard;

			if (!valueToShards.has(keyValue)) {
				valueToShards.set(keyValue, new Set());
			}
			valueToShards.get(keyValue)?.add(shardId);
		}

		const entries = Array.from(valueToShards.entries()).map(
			([keyValue, shardIdSet]) => ({
				keyValue,
				shardIds: Array.from(shardIdSet).sort((a, b) => a - b),
			}),
		);

		if (entries.length > 0) {
			yield* Effect.tryPromise({
				try: () =>
					topologyStub.batchUpsertIndexEntries(job.index_name, entries),
				catch: (error) =>
					new IndexEntriesUpsertError({
						indexName: job.index_name,
						cause: error,
					}),
			});
		}

		// 4. Update index status to 'ready'
		yield* Effect.tryPromise({
			try: () => topologyStub.updateIndexStatus(job.index_name, "ready"),
			catch: (error) =>
				new IndexStatusUpdateError({
					indexName: job.index_name,
					status: "ready",
					cause: error,
				}),
		});
	}).pipe(
		// Add context to all logs and errors in this Effect
		Effect.annotateLogs({
			indexName: job.index_name,
			table: job.table_name,
			databaseId: job.database_id,
			correlationId: correlationId ?? "none",
		}),
	);
}

/**
 * Convert typed error to user-friendly message
 */
function errorToMessage(
	error:
		| NoShardsFoundError
		| TopologyFetchError
		| IndexEntriesUpsertError
		| IndexStatusUpdateError,
): string {
	if (error instanceof NoShardsFoundError) {
		return `No shards found for table '${error.tableName}'`;
	}
	if (error instanceof TopologyFetchError) {
		const causeStr = error.cause ? `: ${error.cause}` : "";
		return `Failed to fetch topology for database '${error.databaseId}'${causeStr}`;
	}
	if (error instanceof IndexEntriesUpsertError) {
		return `Failed to upsert index entries for '${error.indexName}'`;
	}
	// IndexStatusUpdateError
	return `Failed to update index status to '${error.status}' for '${error.indexName}'`;
}

/**
 * Process build index job (Promise wrapper for backward compatibility)
 *
 * This wraps the Effect version and converts it back to a Promise.
 * Errors are caught and the index status is updated to 'failed' on error.
 * Also updates async job status for tracking in topology.
 */
export async function processBuildIndexJob(
	job: IndexBuildJob,
	env: Env,
	correlationId?: string,
): Promise<void> {
	const topologyStub = env.TOPOLOGY.get(
		env.TOPOLOGY.idFromName(job.database_id),
	) as DurableObjectStub<Topology>;

	// Mark async job as running (if job_id was provided)
	if (job.job_id) {
		await topologyStub.startAsyncJob(job.job_id);
	}

	const effect = processBuildIndexJobEffect(job, env, correlationId).pipe(
		Effect.tap(() =>
			Effect.tryPromise({
				try: async () => {
					// Mark async job as completed on success
					if (job.job_id) {
						await topologyStub.completeAsyncJob(job.job_id);
					}
				},
				catch: () => new Error("Failed to complete async job"),
			}),
		),
		Effect.catchAll((error) =>
			Effect.gen(function* () {
				// Handle our custom error types first, then fall back to plain Errors
				// Note: Effect's TaggedError extends Error but its .message is just the tag name
				const errorMessage =
					error instanceof NoShardsFoundError ||
					error instanceof TopologyFetchError ||
					error instanceof IndexEntriesUpsertError ||
					error instanceof IndexStatusUpdateError
						? errorToMessage(error)
						: error instanceof Error
							? error.message
							: String(error);

				// Update index status to 'failed'
				yield* Effect.tryPromise({
					try: () =>
						topologyStub.updateIndexStatus(
							job.index_name,
							"failed",
							errorMessage,
						),
					catch: () => new Error("Failed to update index status to failed"),
				});

				// Mark async job as failed
				if (job.job_id) {
					yield* Effect.tryPromise({
						try: () =>
							topologyStub.failAsyncJob(job.job_id as string, errorMessage),
						catch: () => new Error("Failed to mark async job as failed"),
					});
				}

				// Error will be logged automatically with context annotations
				return yield* Effect.fail(new Error(errorMessage));
			}),
		),
		// Add source annotation to all logs and errors
		Effect.annotateLogs({ source: "QueueConsumer", jobType: "build_index" }),
	);

	return Effect.runPromise(effect);
}
