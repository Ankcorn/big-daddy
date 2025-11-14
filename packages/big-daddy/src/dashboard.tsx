import { Hono } from 'hono';
import { useWorkersLogger } from 'workers-tagged-logger';
import type { BigDaddyLogTags } from './logger';
import { HomePage } from './dashboard/home';
import { DashboardPage } from './dashboard/database';
import { ErrorPage } from './dashboard/error';
import { createConnection } from './index';
import { parseQueryWithAI } from './dashboard/utils/ai-query';

/**
 * Dashboard - Static HTML interface for viewing database topology
 *
 * Provides:
 * - /dash - Home page to create or load a topology
 * - /dash/:databaseId - View topology information for a database
 */

export const dashboard = new Hono<{ Bindings: Env }>();

// Apply logger middleware to all dashboard routes
dashboard.use('*', useWorkersLogger<BigDaddyLogTags>('Dashboard'));

// Set custom HTML renderer
dashboard.use('*', async (c, next) => {
	c.setRenderer((content) => {
		return c.html(
			<html>
				<head>
					<meta charset="UTF-8" />
					<meta name="viewport" content="width=device-width, initial-scale=1.0" />
					<link rel="stylesheet" href="/styles.css" />
					<link
						rel="stylesheet"
						type="text/css"
						href="https://cdn.jsdelivr.net/npm/@phosphor-icons/web@2.1.1/src/regular/style.css"
					/>
					<link
						rel="stylesheet"
						type="text/css"
						href="https://cdn.jsdelivr.net/npm/@phosphor-icons/web@2.1.1/src/fill/style.css"
					/>
				</head>
				<body>
					<div class="my-6 max-w-7xl px-3 sm:px-5 lg:px-12 mx-auto">{content}</div>
				</body>
			</html>,
		);
	});
	await next();
});

/**
 * Dashboard home page - create or load a topology
 */
dashboard.get('/dash', async (c) => {
	const databaseId = c.req.query('id');
	const nodeCountStr = c.req.query('nodes');

	// If no database ID in query, show the home page form
	if (!databaseId) {
		return c.render(<HomePage />);
	}

	// Parse node count (default to 3)
	const nodeCount = nodeCountStr ? parseInt(nodeCountStr, 10) : 3;

	// Redirect to the database dashboard with node count
	return c.redirect(`/dash/${databaseId}?nodes=${nodeCount}`);
});

/**
 * Dashboard database page - displays topology for a specific database
 */
dashboard.get('/dash/:databaseId', async (c) => {
	const databaseId = c.req.param('databaseId');
	const nodeCountStr = c.req.query('nodes');
	const nodeCount = nodeCountStr ? parseInt(nodeCountStr, 10) : 3;
	const sqlError = c.req.query('sqlError');
	const sqlResult = c.req.query('sqlResult');
	const deleteMsg = c.req.query('deleteMsg');
	const lastQuery = c.req.query('q');

	// Prepare sqlResult - either from sqlResult param or deleteMsg (which doesn't need JSON parsing)
	let parsedResult: any = undefined;
	if (sqlResult) {
		try {
			parsedResult = JSON.parse(sqlResult);
		} catch (e) {
			// If JSON parsing fails, just use the raw string
			parsedResult = sqlResult;
		}
	} else if (deleteMsg) {
		parsedResult = deleteMsg;
	}

	try {
		// Fetch topology data
		const topologyId = c.env.TOPOLOGY.idFromName(databaseId);
		const topologyStub = c.env.TOPOLOGY.get(topologyId);

		try {
			// Try to get existing topology
			const topology = await topologyStub.getTopology();
			return c.render(
				<DashboardPage
					databaseId={databaseId}
					topology={topology}
					sqlError={sqlError}
					sqlResult={parsedResult}
					lastQuery={lastQuery}
				/>
			);
		} catch (err) {
			// If topology doesn't exist, create it with specified node count
			if (err instanceof Error && err.message.includes('Topology not created')) {
				console.log(`Creating new topology for database: ${databaseId} with ${nodeCount} nodes`);
				const createResult = await topologyStub.create(nodeCount);

				if (!createResult.success) {
					throw new Error(`Failed to create topology: ${createResult.error}`);
				}

				// Get the newly created topology
				const topology = await topologyStub.getTopology();
				return c.render(
					<DashboardPage
						databaseId={databaseId}
						topology={topology}
						sqlError={sqlError}
						sqlResult={parsedResult}
						lastQuery={lastQuery}
					/>
				);
			}

			// Re-throw if it's a different error
			throw err;
		}
	} catch (error) {
		console.error('Dashboard error:', error);
		const errorMessage = error instanceof Error ? error.message : String(error);
		const errorStack = error instanceof Error ? error.stack : '';
		console.error('Error stack:', errorStack);
		c.status(500);
		return c.render(<ErrorPage databaseId={databaseId} errorMessage={`${errorMessage}\n${errorStack}`} />);
	}
});

/**
 * Execute SQL query on the database via form submission
 */
dashboard.post('/dash/:databaseId/sql', async (c) => {
	const databaseId = c.req.param('databaseId');
	const formData = await c.req.parseBody();
	const query = formData.query as string;

	try {
		if (!query || query.toString().trim() === '') {
			const error = encodeURIComponent('Query cannot be empty');
			return c.redirect(`/dash/${databaseId}?sqlError=${error}`);
		}

		// Try to parse the query with AI if parsing fails
		let finalQuery = query.toString();
		let displayQuery = query.toString(); // Track what to display to the user
		let usedAI = false;

		if (c.env.AI) {
			try {
				finalQuery = await parseQueryWithAI(query.toString(), c.env.AI);
				// If AI was used (query was transformed), swap the display to show the AI-generated SQL
				if (finalQuery !== query.toString()) {
					usedAI = true;
					displayQuery = finalQuery;
				}
			} catch (aiError) {
				const errorMessage = aiError instanceof Error ? aiError.message : String(aiError);
				const encodedError = encodeURIComponent(errorMessage);
				const encodedQuery = encodeURIComponent(query.toString());
				return c.redirect(`/dash/${databaseId}?sqlError=${encodedError}&q=${encodedQuery}`);
			}
		}

		// Create connection to the database
		const sql = await createConnection(databaseId, { nodes: 3 }, c.env);

		// Parse query string to template literal format
		const strings = [finalQuery] as any as TemplateStringsArray;
		const result = await sql(strings);

		// Log the result (could store in session/flash if needed)
		console.log('SQL execution result:', result);

		// Redirect back to the dashboard with result and the display query
		// If AI was used, show the AI-generated SQL instead of the user's input
		const resultJson = encodeURIComponent(JSON.stringify(result));
		const encodedQuery = encodeURIComponent(displayQuery);
		return c.redirect(`/dash/${databaseId}?sqlResult=${resultJson}&q=${encodedQuery}`);
	} catch (error) {
		console.error('SQL execution error:', error);
		const errorMessage = error instanceof Error ? error.message : String(error);
		const encodedError = encodeURIComponent(errorMessage);
		const encodedQuery = encodeURIComponent(query.toString());
		// Redirect back with error and query
		return c.redirect(`/dash/${databaseId}?sqlError=${encodedError}&q=${encodedQuery}`);
	}
});

/**
 * Delete a virtual shard via form submission
 */
dashboard.post('/dash/:databaseId/delete-shard', async (c) => {
	const databaseId = c.req.param('databaseId');
	const formData = await c.req.parseBody();
	const tableName = formData.table_name as string;
	const shardId = parseInt(formData.shard_id as string, 10);

	try {
		if (!tableName || isNaN(shardId)) {
			const error = encodeURIComponent('Invalid table name or shard ID');
			return c.redirect(`/dash/${databaseId}?sqlError=${error}`);
		}

		// Get topology stub and delete the shard
		const topologyId = c.env.TOPOLOGY.idFromName(databaseId);
		const topologyStub = c.env.TOPOLOGY.get(topologyId);

		const result = await topologyStub.deleteVirtualShard(tableName, shardId);

		if (!result.success) {
			const error = encodeURIComponent(`Failed to delete shard: ${result.error}`);
			return c.redirect(`/dash/${databaseId}?sqlError=${error}`);
		}

		// Redirect back to the dashboard (use deleteMsg instead of sqlResult to avoid JSON parsing)
		const successMsg = encodeURIComponent(`Shard ${shardId} deleted successfully`);
		return c.redirect(`/dash/${databaseId}?deleteMsg=${successMsg}`);
	} catch (error) {
		console.error('Delete shard error:', error);
		const errorMessage = error instanceof Error ? error.message : String(error);
		const encodedError = encodeURIComponent(errorMessage);
		return c.redirect(`/dash/${databaseId}?sqlError=${encodedError}`);
	}
});
