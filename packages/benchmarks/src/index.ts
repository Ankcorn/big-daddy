/**
 * Big Daddy Benchmarks Worker
 *
 * This worker provides HTTP endpoints for load testing the Big Daddy database.
 * It connects to Big Daddy via RPC service binding and exposes various
 * benchmark scenarios that can be tested with artillery.io or similar tools.
 */

import type { QueryResult } from 'big-daddy';

interface Env {
	BIG_DADDY: {
		sql(strings: TemplateStringsArray | string[], values?: any[], databaseId?: string, correlationId?: string): Promise<QueryResult>;
	};
	TOPOLOGY: DurableObjectNamespace;
}

interface TopologyDO {
	create(numNodes: number): Promise<{ success: boolean; error?: string }>;
	getTopology(): Promise<any>;
}

/**
 * Generate a random user ID for testing
 */
function randomUserId(): number {
	return Math.floor(Math.random() * 1000000);
}

/**
 * Generate a random email for testing
 */
function randomEmail(): string {
	return `user${randomUserId()}@example.com`;
}

/**
 * Generate random user data for inserts
 */
function randomUser() {
	const userId = randomUserId();
	return {
		id: userId,
		email: `user${userId}@example.com`,
		name: `User ${userId}`,
		age: Math.floor(Math.random() * 80) + 18,
		country: ['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP'][Math.floor(Math.random() * 7)]
	};
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);
		const correlationId = request.headers.get('x-correlation-id') || crypto.randomUUID();

		try {
			// Route to different benchmark scenarios
			switch (url.pathname) {
				case '/init':
					return await handleInit(env, correlationId);

				case '/':
					return new Response(JSON.stringify({
						name: 'Big Daddy Benchmarks',
						version: '0.0.1',
						endpoints: {
							'/init': 'POST - Initialize Big Daddy cluster (required first step)',
							'/setup': 'POST - Initialize database schema',
							'/reset': 'POST - Drop and recreate tables',
							'/insert': 'POST - Insert a single user',
							'/select-by-id': 'GET - Select user by ID (shard key)',
							'/select-by-email': 'GET - Select user by email (index)',
							'/select-all': 'GET - Full table scan',
							'/update': 'POST - Update a user',
							'/delete': 'POST - Delete a user',
							'/mixed-workload': 'POST - Mixed read/write operations',
							'/stats': 'GET - Get database statistics'
						}
					}), {
						headers: { 'Content-Type': 'application/json' }
					});

				case '/setup':
					return await handleSetup(env, correlationId);

				case '/reset':
					return await handleReset(env, correlationId);

				case '/insert':
					return await handleInsert(env, correlationId);

				case '/select-by-id':
					return await handleSelectById(env, correlationId, url);

				case '/select-by-email':
					return await handleSelectByEmail(env, correlationId, url);

				case '/select-all':
					return await handleSelectAll(env, correlationId);

				case '/update':
					return await handleUpdate(env, correlationId);

				case '/delete':
					return await handleDelete(env, correlationId);

				case '/mixed-workload':
					return await handleMixedWorkload(env, correlationId);

				case '/stats':
					return await handleStats(env, correlationId);

				default:
					return new Response(JSON.stringify({
						error: 'Not found',
						path: url.pathname
					}), {
						status: 404,
						headers: { 'Content-Type': 'application/json' }
					});
			}
		} catch (error) {
			return new Response(JSON.stringify({
				error: error instanceof Error ? error.message : String(error),
				correlationId
			}), {
				status: 500,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	}
};

/**
 * Initialize: Create the Big Daddy cluster topology
 */
async function handleInit(env: Env, correlationId: string): Promise<Response> {
	const startTime = Date.now();

	try {
		// Get the Topology Durable Object
		const topologyId = env.TOPOLOGY.idFromName('default');
		const topologyStub = env.TOPOLOGY.get(topologyId) as unknown as TopologyDO;

		// Create cluster with 8 storage nodes (default)
		const result = await topologyStub.create(8);

		const duration = Date.now() - startTime;

		if (result.success) {
			return new Response(JSON.stringify({
				success: true,
				message: 'Big Daddy cluster initialized with 8 storage nodes',
				duration,
				correlationId
			}), {
				headers: { 'Content-Type': 'application/json' }
			});
		} else {
			return new Response(JSON.stringify({
				success: false,
				error: result.error,
				duration,
				correlationId
			}), {
				status: 400,
				headers: { 'Content-Type': 'application/json' }
			});
		}
	} catch (error) {
		const duration = Date.now() - startTime;
		return new Response(JSON.stringify({
			success: false,
			error: error instanceof Error ? error.message : String(error),
			duration,
			correlationId
		}), {
			status: 500,
			headers: { 'Content-Type': 'application/json' }
		});
	}
}

/**
 * Setup: Create the users table with indexes
 */
async function handleSetup(env: Env, correlationId: string): Promise<Response> {
	const startTime = Date.now();

	// Create users table
	const createTableStrings = ['CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT, name TEXT, age INTEGER, country TEXT)'] as unknown as TemplateStringsArray;
	await env.BIG_DADDY.sql(
		createTableStrings,
		[],
		'default',
		correlationId
	);

	// Create index on email (for non-shard-key lookups)
	const createEmailIndexStrings = ['CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)'] as unknown as TemplateStringsArray;
	await env.BIG_DADDY.sql(
		createEmailIndexStrings,
		[],
		'default',
		correlationId
	);

	// Create index on country (for analytics queries)
	const createCountryIndexStrings = ['CREATE INDEX IF NOT EXISTS idx_users_country ON users(country)'] as unknown as TemplateStringsArray;
	await env.BIG_DADDY.sql(
		createCountryIndexStrings,
		[],
		'default',
		correlationId
	);

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		message: 'Database schema created',
		duration,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}

/**
 * Reset: Drop and recreate tables
 */
async function handleReset(env: Env, correlationId: string): Promise<Response> {
	const startTime = Date.now();

	// Drop table if exists
	const dropTableStrings = ['DROP TABLE IF EXISTS users'] as unknown as TemplateStringsArray;
	await env.BIG_DADDY.sql(
		dropTableStrings,
		[],
		'default',
		correlationId
	);

	// Recreate
	await handleSetup(env, correlationId);

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		message: 'Database reset complete',
		duration,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}

/**
 * Insert: Add a new user
 */
async function handleInsert(env: Env, correlationId: string): Promise<Response> {
	const user = randomUser();
	const startTime = Date.now();

	const insertStrings = ['INSERT INTO users (id, email, name, age, country) VALUES (', ', ', ', ', ', ', ')'] as unknown as TemplateStringsArray;
	const result = await env.BIG_DADDY.sql(
		insertStrings,
		[user.id, user.email, user.name, user.age, user.country],
		'default',
		correlationId
	);

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		userId: user.id,
		rowsAffected: result.rowsAffected,
		duration,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}

/**
 * Select by ID: Query using shard key (single shard)
 */
async function handleSelectById(env: Env, correlationId: string, url: URL): Promise<Response> {
	const userId = url.searchParams.get('id')
		? parseInt(url.searchParams.get('id')!)
		: randomUserId();

	const startTime = Date.now();

	const selectStrings = ['SELECT * FROM users WHERE id = ', ''] as unknown as TemplateStringsArray;
	const result = await env.BIG_DADDY.sql(
		selectStrings,
		[userId],
		'default',
		correlationId
	);

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		userId,
		found: result.rows.length > 0,
		user: result.rows[0] || null,
		duration,
		cacheStats: result.cacheStats,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}

/**
 * Select by Email: Query using index (potentially multiple shards)
 */
async function handleSelectByEmail(env: Env, correlationId: string, url: URL): Promise<Response> {
	const email = url.searchParams.get('email') || randomEmail();

	const startTime = Date.now();

	const selectStrings = ['SELECT * FROM users WHERE email = ', ''] as unknown as TemplateStringsArray;
	const result = await env.BIG_DADDY.sql(
		selectStrings,
		[email],
		'default',
		correlationId
	);

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		email,
		found: result.rows.length > 0,
		user: result.rows[0] || null,
		duration,
		cacheStats: result.cacheStats,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}

/**
 * Select All: Full table scan (all shards)
 */
async function handleSelectAll(env: Env, correlationId: string): Promise<Response> {
	const startTime = Date.now();

	const selectStrings = ['SELECT * FROM users LIMIT 100'] as unknown as TemplateStringsArray;
	const result = await env.BIG_DADDY.sql(
		selectStrings,
		[],
		'default',
		correlationId
	);

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		count: result.rows.length,
		duration,
		cacheStats: result.cacheStats,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}

/**
 * Update: Modify a user record
 */
async function handleUpdate(env: Env, correlationId: string): Promise<Response> {
	const userId = randomUserId();
	const newAge = Math.floor(Math.random() * 80) + 18;

	const startTime = Date.now();

	const updateStrings = ['UPDATE users SET age = ', ' WHERE id = ', ''] as unknown as TemplateStringsArray;
	const result = await env.BIG_DADDY.sql(
		updateStrings,
		[newAge, userId],
		'default',
		correlationId
	);

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		userId,
		rowsAffected: result.rowsAffected,
		duration,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}

/**
 * Delete: Remove a user record
 */
async function handleDelete(env: Env, correlationId: string): Promise<Response> {
	const userId = randomUserId();

	const startTime = Date.now();

	const deleteStrings = ['DELETE FROM users WHERE id = ', ''] as unknown as TemplateStringsArray;
	const result = await env.BIG_DADDY.sql(
		deleteStrings,
		[userId],
		'default',
		correlationId
	);

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		userId,
		rowsAffected: result.rowsAffected,
		duration,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}

/**
 * Mixed Workload: Combination of reads and writes
 */
async function handleMixedWorkload(env: Env, correlationId: string): Promise<Response> {
	const startTime = Date.now();
	const operations: string[] = [];

	// 70% reads, 30% writes
	const op = Math.random();

	if (op < 0.5) {
		// Select by ID (single shard)
		const selectStrings = ['SELECT * FROM users WHERE id = ', ''] as unknown as TemplateStringsArray;
		await env.BIG_DADDY.sql(
			selectStrings,
			[randomUserId()],
			'default',
			correlationId
		);
		operations.push('select-by-id');
	} else if (op < 0.7) {
		// Select by email (indexed)
		const selectStrings = ['SELECT * FROM users WHERE email = ', ''] as unknown as TemplateStringsArray;
		await env.BIG_DADDY.sql(
			selectStrings,
			[randomEmail()],
			'default',
			correlationId
		);
		operations.push('select-by-email');
	} else if (op < 0.85) {
		// Insert
		const user = randomUser();
		const insertStrings = ['INSERT INTO users (id, email, name, age, country) VALUES (', ', ', ', ', ', ', ')'] as unknown as TemplateStringsArray;
		await env.BIG_DADDY.sql(
			insertStrings,
			[user.id, user.email, user.name, user.age, user.country],
			'default',
			correlationId
		);
		operations.push('insert');
	} else if (op < 0.95) {
		// Update
		const updateStrings = ['UPDATE users SET age = ', ' WHERE id = ', ''] as unknown as TemplateStringsArray;
		await env.BIG_DADDY.sql(
			updateStrings,
			[Math.floor(Math.random() * 80) + 18, randomUserId()],
			'default',
			correlationId
		);
		operations.push('update');
	} else {
		// Delete
		const deleteStrings = ['DELETE FROM users WHERE id = ', ''] as unknown as TemplateStringsArray;
		await env.BIG_DADDY.sql(
			deleteStrings,
			[randomUserId()],
			'default',
			correlationId
		);
		operations.push('delete');
	}

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		operations,
		duration,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}

/**
 * Stats: Get database statistics
 */
async function handleStats(env: Env, correlationId: string): Promise<Response> {
	const startTime = Date.now();

	const countStrings = ['SELECT COUNT(*) as count FROM users'] as unknown as TemplateStringsArray;
	const result = await env.BIG_DADDY.sql(
		countStrings,
		[],
		'default',
		correlationId
	);

	const duration = Date.now() - startTime;

	return new Response(JSON.stringify({
		success: true,
		totalUsers: result.rows[0]?.count || 0,
		duration,
		cacheStats: result.cacheStats,
		correlationId
	}), {
		headers: { 'Content-Type': 'application/json' }
	});
}
