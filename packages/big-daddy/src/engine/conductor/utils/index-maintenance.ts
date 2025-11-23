/**
 * Index Maintenance Utilities
 *
 * Shared utilities for generating index maintenance queries and dispatching sync events
 * across INSERT, UPDATE, and DELETE operations.
 */

import type {
	SelectStatement,
	InsertStatement,
	UpdateStatement,
	DeleteStatement,
} from '@databases/sqlite-ast';
import { logger } from '../../../logger';
import type { QueryResult, QueryHandlerContext, ShardInfo } from '../types';
import type { IndexMaintenanceEventJob } from '../../queue/types';
import type { StatementWithParams } from './write';

/**
 * Prepare queries for index maintenance
 *
 * For operations with indexes, generates the necessary queries (SELECT before/after, main operation).
 * For operations without indexes, returns just the main statement.
 *
 * Different operations need different query patterns:
 * - INSERT: [INSERT] (no capture needed, values are in INSERT statement)
 * - DELETE: [SELECT (capture), DELETE]
 * - UPDATE: [SELECT before, UPDATE, SELECT after]
 */
export function prepareIndexMaintenanceQueries(
	hasIndexes: boolean,
	mainStatement:
		| InsertStatement
		| UpdateStatement
		| DeleteStatement
		| SelectStatement,
	selectStatement?: SelectStatement, // For DELETE/UPDATE
	params?: any[],
	selectParams?: any[], // For UPDATE when different from main params
): StatementWithParams[] {
	// No indexes: just return the main statement
	if (!hasIndexes) {
		return [{ statement: mainStatement, params: params || [] }];
	}

	// With indexes: return appropriate query pattern
	const operationType = mainStatement.type;

	if (operationType === 'InsertStatement') {
		// INSERT: no capture needed
		return [{ statement: mainStatement, params: params || [] }];
	}

	if (operationType === 'DeleteStatement') {
		// DELETE: [SELECT capture, DELETE]
		if (!selectStatement) throw new Error('SELECT statement required for DELETE');
		return [
			{ statement: selectStatement, params: params || [] },
			{ statement: mainStatement, params: params || [] },
		];
	}

	if (operationType === 'UpdateStatement') {
		// UPDATE: [SELECT before, UPDATE, SELECT after]
		if (!selectStatement) throw new Error('SELECT statement required for UPDATE');
		return [
			{ statement: selectStatement, params: selectParams || [] },
			{ statement: mainStatement, params: params || [] },
			{ statement: selectStatement, params: selectParams || [] },
		];
	}

	throw new Error(`Unsupported statement type: ${operationType}`);
}

/**
 * Dispatch index synchronization events from query results
 *
 * Takes the results of index maintenance queries and dispatches appropriate
 * index update events to the queue. Handles:
 * - Deduplicating events per shard
 * - Filtering out events with NULL values
 * - Queueing the maintenance job
 *
 * This method is shared across INSERT, DELETE, and UPDATE to reduce boilerplate.
 */
export async function dispatchIndexSyncingFromQueryResults(
	operationType: 'INSERT' | 'DELETE' | 'UPDATE',
	queryResults: QueryResult[][],
	tableName: string,
	shardsToQuery: ShardInfo[],
	virtualIndexes: Array<{ index_name: string; columns: string }>,
	context: QueryHandlerContext,
	rowDataExtractor: (results: QueryResult[][]) => {
		oldRows?: Map<number, Record<string, any>[]>;
		newRows?: Map<number, Record<string, any>[]>;
	},
): Promise<void> {
	const { indexQueue, databaseId, correlationId } = context;

	if (!indexQueue || virtualIndexes.length === 0) {
		return;
	}

	// Extract row data based on operation type
	const { oldRows, newRows } = rowDataExtractor(queryResults);

	// Generate events based on operation
	let events: IndexMaintenanceEventJob['events'] = [];

	if (operationType === 'INSERT') {
		// For INSERT: just the new rows
		if (newRows) {
			events = generateInsertIndexEvents(newRows, virtualIndexes);
		}
	} else if (operationType === 'DELETE') {
		// For DELETE: just the old rows (being removed)
		if (oldRows) {
			events = generateDeleteIndexEvents(oldRows, virtualIndexes);
		}
	} else if (operationType === 'UPDATE') {
		// For UPDATE: both old and new rows
		if (oldRows && newRows) {
			events = generateUpdateIndexEvents(oldRows, newRows, virtualIndexes);
		}
	}

	// Queue if there are events
	if (events.length > 0) {
		const job: IndexMaintenanceEventJob = {
			type: 'maintain_index_events',
			database_id: databaseId,
			table_name: tableName,
			events,
			created_at: new Date().toISOString(),
			correlation_id: correlationId,
		};

		await indexQueue.send(job);

		logger.info('Index maintenance events queued', {
			operationType,
			eventCount: events.length,
		});
	}
}

/**
 * Build a key value for indexed column(s) from a row
 * Returns null if any indexed column is NULL (NULL values are not indexed)
 */
function buildIndexKeyValue(row: Record<string, any>, columns: string[]): string | null {
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
 * Generate INSERT index events from new rows
 */
function generateInsertIndexEvents(
	newRows: Map<number, Record<string, any>[]>,
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): IndexMaintenanceEventJob['events'] {
	const eventMap = new Map<
		string,
		{
			index_name: string;
			key_value: string;
			shard_id: number;
			operation: 'add';
		}
	>();

	for (const [shardId, rows] of newRows.entries()) {
		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns) as string[];

			for (const row of rows) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue === null) continue;

				const eventKey = `${index.index_name}:${keyValue}:${shardId}`;
				if (!eventMap.has(eventKey)) {
					eventMap.set(eventKey, {
						index_name: index.index_name,
						key_value: keyValue,
						shard_id: shardId,
						operation: 'add',
					});
				}
			}
		}
	}

	return Array.from(eventMap.values());
}

/**
 * Generate DELETE index events from old rows
 */
function generateDeleteIndexEvents(
	oldRows: Map<number, Record<string, any>[]>,
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): IndexMaintenanceEventJob['events'] {
	const eventMap = new Map<
		string,
		{
			index_name: string;
			key_value: string;
			shard_id: number;
			operation: 'remove';
		}
	>();

	for (const [shardId, rows] of oldRows.entries()) {
		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns) as string[];

			for (const row of rows) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue === null) continue;

				const eventKey = `${index.index_name}:${keyValue}:${shardId}`;
				if (!eventMap.has(eventKey)) {
					eventMap.set(eventKey, {
						index_name: index.index_name,
						key_value: keyValue,
						shard_id: shardId,
						operation: 'remove',
					});
				}
			}
		}
	}

	return Array.from(eventMap.values());
}

/**
 * Generate UPDATE index events from old and new rows
 *
 * Per-row approach:
 * - For each row, compare old vs new indexed values
 * - Queue 'remove' for values that went away
 * - Queue 'add' for new values that appeared
 *
 * Deduplication is per-shard; global dedup is handled by index handler via idempotent SQLite ops.
 */
function generateUpdateIndexEvents(
	oldRows: Map<number, Record<string, any>[]>,
	newRows: Map<number, Record<string, any>[]>,
	virtualIndexes: Array<{ index_name: string; columns: string }>,
): IndexMaintenanceEventJob['events'] {
	const eventMap = new Map<
		string,
		{
			index_name: string;
			key_value: string;
			shard_id: number;
			operation: 'add' | 'remove';
		}
	>();

	for (const [shardId, oldRowsList] of oldRows.entries()) {
		const newRowsList = newRows.get(shardId) || [];

		for (const index of virtualIndexes) {
			const indexColumns = JSON.parse(index.columns) as string[];

			const oldValues = new Set<string>();
			for (const row of oldRowsList) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue !== null) {
					oldValues.add(keyValue);
				}
			}

			const newValues = new Set<string>();
			for (const row of newRowsList) {
				const keyValue = buildIndexKeyValue(row, indexColumns);
				if (keyValue !== null) {
					newValues.add(keyValue);
				}
			}

			// Removals
			for (const value of oldValues) {
				if (!newValues.has(value)) {
					const eventKey = `${index.index_name}:${value}:${shardId}`;
					eventMap.set(eventKey, {
						index_name: index.index_name,
						key_value: value,
						shard_id: shardId,
						operation: 'remove',
					});
				}
			}

			// Additions
			for (const value of newValues) {
				if (!oldValues.has(value)) {
					const eventKey = `${index.index_name}:${value}:${shardId}`;
					eventMap.set(eventKey, {
						index_name: index.index_name,
						key_value: value,
						shard_id: shardId,
						operation: 'add',
					});
				}
			}
		}
	}

	return Array.from(eventMap.values());
}
