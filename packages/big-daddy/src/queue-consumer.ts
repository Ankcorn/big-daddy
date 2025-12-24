/**
 * Queue Consumer Worker for Async Operations
 *
 * This worker processes jobs from the vitess-index-jobs queue:
 * - IndexBuildJob: Build a new virtual index from existing data
 * - ReshardTableJob: Reshard a table from source to target shards
 * - ReshardingChangeLog: Change log entries (batched and processed during resharding replay)
 */

import {
	processBuildIndexJob,
	processReshardTableJob,
} from "./engine/async-jobs";
import type {
	IndexJob,
	MessageBatch,
	ReshardingChangeLog,
} from "./engine/queue/types";
import { logger } from "./logger";

/**
 * Queue message handler
 * Receives batches of up to 10 messages and processes them
 */
export async function queueHandler(
	batch: MessageBatch<IndexJob>,
	env: Env,
	batchCorrelationId?: string,
): Promise<void> {
	const cid = batchCorrelationId || crypto.randomUUID();
	const source = "QueueConsumer";
	const component = "QueueConsumer";
	const operation = "queueHandler";
	const batchSize = batch.messages.length;
	const queue = batch.queue;

	logger.info`Processing queue batch ${{ source }} ${{ component }} ${{ operation }} ${{ correlationId: cid }} ${{ requestId: cid }} ${{ batchSize }} ${{ queue }}`;

	// Process messages in parallel where possible
	const results = await Promise.allSettled(
		batch.messages.map(async (message) => {
			const jobCorrelationId = message.body.correlation_id || cid;
			const jobId = message.id;
			const jobType = message.body.type;
			const attempts = message.attempts;

			try {
				logger.info`Processing queue message ${{ source }} ${{ component }} ${{ correlationId: jobCorrelationId }} ${{ requestId: jobCorrelationId }} ${{ jobId }} ${{ jobType }} ${{ attempts }}`;

				await processIndexJob(message.body, env, jobCorrelationId);

				const status = "success";
				logger.info`Successfully processed job ${{ source }} ${{ component }} ${{ jobId }} ${{ jobType }} ${{ status }}`;
			} catch (error) {
				const errorMsg = error instanceof Error ? error.message : String(error);
				const status = "failure";
				logger.error`Failed to process job ${{ source }} ${{ component }} ${{ jobId }} ${{ jobType }} ${{ error: errorMsg }} ${{ status }}`;
				throw error;
			}
		}),
	);

	// Log summary
	const successful = results.filter((r) => r.status === "fulfilled").length;
	const failed = results.filter((r) => r.status === "rejected").length;
	const finalStatus = failed > 0 ? "partial" : "success";

	logger.info`Batch processing complete ${{ source }} ${{ component }} ${{ batchSize }} ${{ successful }} ${{ failed }} ${{ status: finalStatus }}`;
}

/**
 * Process a single job - dispatches to appropriate handler
 */
async function processIndexJob(
	job: IndexJob,
	env: Env,
	correlationId?: string,
): Promise<void> {
	switch (job.type) {
		case "build_index":
			await processBuildIndexJob(job, env, correlationId);
			break;
		case "reshard_table":
			await processReshardTableJob(job, env, correlationId);
			break;
		case "resharding_change_log": {
			const reshardingId = (job as ReshardingChangeLog).resharding_id;
			logger.debug`Change log entry queued for later replay ${{ reshardingId }}`;
			break;
		}
		default:
			throw new Error(`Unknown job type: ${(job as { type: string }).type}`);
	}
}
