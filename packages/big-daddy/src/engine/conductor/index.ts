import { parse } from "@databases/sqlite-ast";
import { logger } from "../../logger";
import type { Storage } from "../storage";
import type { Topology } from "../topology/index";
import { buildQuery } from "../utils/ast-utils";
import { TopologyCache } from "../utils/topology-cache";
import { handleDelete, handleInsert, handleSelect, handleUpdate } from "./crud";
import { handleCreateIndex } from "./indexes";
import { handlePragma } from "./pragmas";
import { handleCreateTable, handleDropTable } from "./tables";
import type {
	ConductorAPI,
	QueryHandlerContext,
	QueryResult,
	SqlParam,
} from "./types";

/**
 * Conductor - Routes SQL queries to the appropriate storage shards
 *
 * The Conductor sits between the client and the distributed storage layer,
 * parsing queries, determining which shards to target, and coordinating
 * execution across multiple storage nodes.
 */
export class ConductorClient {
	private cache: TopologyCache;
	private correlationId: string;

	constructor(
		private databaseId: string,
		correlationId: string,
		private storage: DurableObjectNamespace<Storage>,
		private topology: DurableObjectNamespace<Topology>,
		private indexQueue?: Queue,
		private env?: Env, // For test environment queue processing
	) {
		this.cache = new TopologyCache();
		this.correlationId = correlationId;
	}

	/**
	 * Get cache statistics (for testing and monitoring)
	 */
	getCacheStats() {
		return this.cache.getStats();
	}

	/**
	 * Clear the topology cache (for testing)
	 */
	clearCache() {
		this.cache.clear();
	}

	/**
	 * Execute a SQL query using tagged template literals
	 *
	 * @example
	 * const result = await conductor.sql<{ id: number; name: string }>`SELECT id, name FROM users WHERE id = ${userId}`;
	 * await conductor.sql`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`;
	 */
	sql = async <T = Record<string, unknown>>(
		strings: TemplateStringsArray,
		...values: SqlParam[]
	): Promise<QueryResult<T>> => {
		const startTime = Date.now();
		const cid = this.correlationId;
		const source = "Conductor";
		const component = "Conductor";
		const operation = "sql";
		const databaseId = this.databaseId;

		// STEP 1: Parse - Build and parse the SQL query
		const { query, params } = buildQuery(strings, values);
		const statement = parse(query);

		logger.info`Executing query ${{ source }} ${{ component }} ${{ operation }} ${{ correlationId: cid }} ${{ requestId: cid }} ${{ databaseId }}`;

		// Create handler context
		const context: QueryHandlerContext = {
			databaseId: this.databaseId,
			correlationId: cid,
			storage: this.storage,
			topology: this.topology,
			indexQueue: this.indexQueue,
			env: this.env,
			cache: this.cache,
		};

		// STEP 2: Route to appropriate handler based on query type
		let result: QueryResult<T>;

		if (statement.type === "SelectStatement") {
			result = (await handleSelect(
				statement,
				query,
				params,
				context,
			)) as QueryResult<T>;
		} else if (statement.type === "InsertStatement") {
			result = (await handleInsert(
				statement,
				query,
				params,
				context,
			)) as QueryResult<T>;
		} else if (statement.type === "UpdateStatement") {
			result = (await handleUpdate(
				statement,
				query,
				params,
				context,
			)) as QueryResult<T>;
		} else if (statement.type === "DeleteStatement") {
			result = (await handleDelete(
				statement,
				query,
				params,
				context,
			)) as QueryResult<T>;
		} else if (statement.type === "CreateTableStatement") {
			result = (await handleCreateTable(
				statement,
				query,
				context,
			)) as QueryResult<T>;
		} else if (statement.type === "DropTableStatement") {
			result = (await handleDropTable(statement, context)) as QueryResult<T>;
		} else if (statement.type === "CreateIndexStatement") {
			result = (await handleCreateIndex(statement, context)) as QueryResult<T>;
		} else if (statement.type === "PragmaStatement") {
			result = (await handlePragma(
				statement,
				query,
				context,
			)) as QueryResult<T>;
		} else {
			throw new Error(`Unsupported statement type: ${statement.type}`);
		}

		const duration = Date.now() - startTime;
		const rowCount = result.rows.length;
		const rowsAffected = result.rowsAffected;

		logger.info`Query completed ${{ source }} ${{ component }} ${{ databaseId }} ${{ duration }} ${{ rowCount }} ${{ rowsAffected }}`;

		return result;
	};
}

/**
 * Create a Conductor client for a specific database
 *
 * @param databaseId - Unique identifier for the database
 * @param cid - Correlation ID for request tracking
 * @param env - Worker environment with Durable Object bindings
 * @returns A Conductor API with sql method and cache management
 *
 * @example
 * const conductor = createConductor('my-database', correlationId, env);
 * const result = await conductor.sql`SELECT * FROM users WHERE id = ${123}`;
 * const stats = conductor.getCacheStats();
 * conductor.clearCache();
 */
export function createConductor(
	databaseId: string,
	cid: string,
	env: Env,
): ConductorAPI {
	const client = new ConductorClient(
		databaseId,
		cid,
		env.STORAGE as unknown as DurableObjectNamespace<Storage>,
		env.TOPOLOGY as unknown as DurableObjectNamespace<Topology>,
		env.INDEX_QUEUE,
		env,
	);

	return {
		sql: <T = Record<string, unknown>>(
			strings: TemplateStringsArray,
			...values: SqlParam[]
		) => client.sql<T>(strings, ...values),
		getCacheStats: () => client.getCacheStats(),
		clearCache: () => client.clearCache(),
	};
}

// Re-export types for external use
export type { ConductorAPI, QueryResult, SqlParam } from "./types";
