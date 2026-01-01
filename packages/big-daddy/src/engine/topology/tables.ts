/**
 * Database Schema for Topology
 *
 * Defines all SQL table creation statements for the topology Durable Object.
 * These are executed during initialization to set up the persistent storage.
 */

/**
 * Run schema migrations that add new columns to existing tables.
 * These are safe to run multiple times (idempotent).
 */
export function runSchemaMigrations(
	sqlExec: (sql: string) => void,
	tableInfo: (tableName: string) => Array<{ name: string }>,
): void {
	// Migration: Add row_count column to table_shards if not present
	const tableShardsColumns = tableInfo("table_shards");
	const hasRowCount = tableShardsColumns.some((col) => col.name === "row_count");
	if (!hasRowCount && tableShardsColumns.length > 0) {
		// Table exists but row_count column is missing - add it
		sqlExec(
			`ALTER TABLE table_shards ADD COLUMN row_count INTEGER NOT NULL DEFAULT 0`,
		);
	}
}

export function initializeSchemaTables(sqlExec: (sql: string) => void): void {
	// Cluster metadata - stores if cluster is created
	sqlExec(`
		CREATE TABLE IF NOT EXISTS cluster_metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`);

	// Storage nodes table - tracks all available storage DOs
	sqlExec(`
		CREATE TABLE IF NOT EXISTS storage_nodes (
			node_id TEXT PRIMARY KEY,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			status TEXT NOT NULL DEFAULT 'active',
			capacity_used INTEGER DEFAULT 0,
			error TEXT
		)
	`);

	// Tables metadata - stores info about user-created tables
	sqlExec(`
		CREATE TABLE IF NOT EXISTS tables (
			table_name TEXT PRIMARY KEY,
			primary_key TEXT NOT NULL,
			primary_key_type TEXT NOT NULL,
			shard_strategy TEXT NOT NULL CHECK(shard_strategy IN ('hash', 'range')),
			shard_key TEXT NOT NULL,
			num_shards INTEGER NOT NULL DEFAULT 1,
			block_size INTEGER DEFAULT 500,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)
	`);

	// Table shards - maps virtual shards to physical storage nodes
	sqlExec(`
		CREATE TABLE IF NOT EXISTS table_shards (
			table_name TEXT NOT NULL,
			shard_id INTEGER NOT NULL,
			node_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'pending', 'to_be_deleted', 'failed')),
			row_count INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (table_name, shard_id),
			FOREIGN KEY (table_name) REFERENCES tables(table_name) ON DELETE CASCADE,
			FOREIGN KEY (node_id) REFERENCES storage_nodes(node_id)
		)
	`);

	// Resharding state - tracks ongoing resharding operations
	sqlExec(`
		CREATE TABLE IF NOT EXISTS resharding_states (
			table_name TEXT PRIMARY KEY,
			source_shard_id INTEGER NOT NULL,
			target_shard_ids TEXT NOT NULL,
			change_log_id TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending_shards' CHECK(status IN ('pending_shards', 'copying', 'replaying', 'verifying', 'complete', 'failed')),
			error_message TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			FOREIGN KEY (table_name) REFERENCES tables(table_name) ON DELETE CASCADE
		)
	`);

	// Virtual indexes - metadata for indexes that enable query optimization
	sqlExec(`
		CREATE TABLE IF NOT EXISTS virtual_indexes (
			index_name TEXT PRIMARY KEY,
			table_name TEXT NOT NULL,
			columns TEXT NOT NULL,
			index_type TEXT NOT NULL CHECK(index_type IN ('hash', 'unique')),
			status TEXT NOT NULL DEFAULT 'building' CHECK(status IN ('building', 'ready', 'failed', 'rebuilding')),
			error_message TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			FOREIGN KEY (table_name) REFERENCES tables(table_name) ON DELETE CASCADE
		)
	`);

	// Virtual index entries - maps indexed values to shard IDs
	sqlExec(`
		CREATE TABLE IF NOT EXISTS virtual_index_entries (
			index_name TEXT NOT NULL,
			key_value TEXT NOT NULL,
			shard_ids TEXT NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (index_name, key_value),
			FOREIGN KEY (index_name) REFERENCES virtual_indexes(index_name) ON DELETE CASCADE
		)
	`);

	// Async jobs - tracks long-running operations (resharding, index building, etc.)
	sqlExec(`
		CREATE TABLE IF NOT EXISTS async_jobs (
			job_id TEXT PRIMARY KEY,
			job_type TEXT NOT NULL CHECK(job_type IN ('reshard_table', 'build_index', 'maintain_index')),
			table_name TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'running', 'completed', 'failed')),
			error_message TEXT,
			started_at INTEGER NOT NULL,
			ended_at INTEGER,
			duration_ms INTEGER,
			retries INTEGER NOT NULL DEFAULT 0,
			metadata TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			FOREIGN KEY (table_name) REFERENCES tables(table_name) ON DELETE CASCADE
		)
	`);
}
