/**
 * Blog Benchmark - A realistic blog application workload
 *
 * Schema:
 * - users: User accounts with profile information
 * - posts: Blog posts with author references
 * - comments: Comments on posts with user references
 * - likes: Post likes with user references
 *
 * This benchmark uses Cloudflare Workflows to seed the database with realistic data.
 */

import {
	WorkerEntrypoint,
	WorkflowEntrypoint,
	type WorkflowEvent,
	type WorkflowStep,
} from "cloudflare:workers";
import type { BigDaddyEnv, ConnectionConfig, SqlFunction } from "big-daddy";
import { createConnection } from "big-daddy";

interface BenchmarkEnv {
	STORAGE: DurableObjectNamespace;
	TOPOLOGY: DurableObjectNamespace;
	INDEX_QUEUE: Queue;
	SEED_WORKFLOW: Workflow;
	AI: Ai;
}

/**
 * Seed configuration for the blog benchmark
 */
interface SeedConfig {
	numUsers: number;
	numPosts: number;
	numComments: number;
	numLikes: number;
}

interface SeedParams {
	databaseId: string;
	config: SeedConfig;
}

/**
 * Generate a random username
 */
function randomUsername(): string {
	const adjectives = [
		"Happy",
		"Clever",
		"Brave",
		"Swift",
		"Wise",
		"Bold",
		"Kind",
		"Cool",
	];
	const nouns = [
		"Panda",
		"Eagle",
		"Tiger",
		"Dolphin",
		"Wolf",
		"Fox",
		"Bear",
		"Hawk",
	];
	const adj = adjectives[Math.floor(Math.random() * adjectives.length)];
	const noun = nouns[Math.floor(Math.random() * nouns.length)];
	const num = Math.floor(Math.random() * 1000);
	return `${adj}${noun}${num}`;
}

/**
 * Generate random blog post title
 */
function randomPostTitle(): string {
	const titles = [
		"Getting Started with Distributed Databases",
		"Understanding Cloudflare Workers",
		"Building Scalable Applications",
		"The Future of Edge Computing",
		"Optimizing Database Performance",
		"Best Practices for Sharding",
		"Introduction to Durable Objects",
		"Mastering SQL at the Edge",
		"Performance Tuning Tips",
		"Building Real-time Applications",
	];
	return titles[Math.floor(Math.random() * titles.length)]!;
}

/**
 * Generate random blog post content
 */
function randomPostContent(): string {
	const paragraphs = [
		"This is an interesting post about distributed systems and how they scale.",
		"In this article, we explore the fundamentals of edge computing.",
		"Performance optimization is crucial for modern web applications.",
		"Let me share some insights from my recent project experience.",
		"Here are some best practices I've learned over the years.",
	];
	return paragraphs[Math.floor(Math.random() * paragraphs.length)]!;
}

/**
 * Generate random comment text
 */
function randomComment(): string {
	const comments = [
		"Great article! Very informative.",
		"Thanks for sharing this.",
		"I learned a lot from this post.",
		"Interesting perspective!",
		"This is exactly what I needed to know.",
		"Well written and easy to follow.",
		"Could you elaborate more on this topic?",
		"I have a question about the implementation.",
	];
	return comments[Math.floor(Math.random() * comments.length)]!;
}

/**
 * Seed Workflow - Populates the database with test data
 */
export class SeedWorkflow extends WorkflowEntrypoint<BenchmarkEnv, SeedParams> {
	override async run(event: WorkflowEvent<SeedParams>, step: WorkflowStep) {
		const { databaseId, config } = event.payload;

		// Step 1: Create SQL connection
		const sql = await step.do("create-connection", async () => {
			const connectionConfig: ConnectionConfig = {
				nodes: 8, // 8 storage nodes for sharding
			};
			return await createConnection(
				databaseId,
				connectionConfig,
				this.env as unknown as BigDaddyEnv,
			);
		});

		// Step 2: Create schema
		await step.do("create-schema", async () => {
			// Users table
			await (sql as SqlFunction)`
				CREATE TABLE IF NOT EXISTS users (
					id INTEGER PRIMARY KEY,
					username TEXT NOT NULL,
					email TEXT NOT NULL,
					bio TEXT,
					created_at INTEGER NOT NULL
				)
			`;

			// Create index on username
			await (sql as SqlFunction)`CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)`;

			// Posts table
			await (sql as SqlFunction)`
				CREATE TABLE IF NOT EXISTS posts (
					id INTEGER PRIMARY KEY,
					user_id INTEGER NOT NULL,
					title TEXT NOT NULL,
					content TEXT NOT NULL,
					published_at INTEGER NOT NULL
				)
			`;

			// Create index on user_id for post lookups
			await (sql as SqlFunction)`CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id)`;

			// Comments table
			await (sql as SqlFunction)`
				CREATE TABLE IF NOT EXISTS comments (
					id INTEGER PRIMARY KEY,
					post_id INTEGER NOT NULL,
					user_id INTEGER NOT NULL,
					content TEXT NOT NULL,
					created_at INTEGER NOT NULL
				)
			`;

			// Create indexes for comment lookups
			await (sql as SqlFunction)`CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id)`;
			await (sql as SqlFunction)`CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id)`;

			// Likes table
			await (sql as SqlFunction)`
				CREATE TABLE IF NOT EXISTS likes (
					id INTEGER PRIMARY KEY,
					post_id INTEGER NOT NULL,
					user_id INTEGER NOT NULL,
					created_at INTEGER NOT NULL
				)
			`;

			// Create composite index for unique likes and lookups
			await (sql as SqlFunction)`CREATE INDEX IF NOT EXISTS idx_likes_post_user ON likes(post_id, user_id)`;

			return { success: true };
		});

		// Step 3: Seed users
		const userIds = await step.do("seed-users", async () => {
			const ids: number[] = [];
			const now = Date.now();

			for (let i = 0; i < config.numUsers; i++) {
				const userId = 1000 + i;
				const username = randomUsername();
				const email = `${username.toLowerCase()}@example.com`;
				const bio = `I'm a blogger who loves to write about technology.`;

				await (sql as SqlFunction)`
					INSERT INTO users (id, username, email, bio, created_at)
					VALUES (${userId}, ${username}, ${email}, ${bio}, ${now})
				`;

				ids.push(userId);
			}

			return ids;
		});

		// Step 4: Seed posts
		const postIds = await step.do("seed-posts", async () => {
			const ids: number[] = [];
			const now = Date.now();

			for (let i = 0; i < config.numPosts; i++) {
				const postId = 2000 + i;
				const userId = userIds[Math.floor(Math.random() * userIds.length)]!;
				const title = randomPostTitle();
				const content = randomPostContent();

				await (sql as SqlFunction)`
					INSERT INTO posts (id, user_id, title, content, published_at)
					VALUES (${postId}, ${userId}, ${title}, ${content}, ${now})
				`;

				ids.push(postId);
			}

			return ids;
		});

		// Step 5: Seed comments
		await step.do("seed-comments", async () => {
			const now = Date.now();

			for (let i = 0; i < config.numComments; i++) {
				const commentId = 3000 + i;
				const postId = postIds[Math.floor(Math.random() * postIds.length)]!;
				const userId = userIds[Math.floor(Math.random() * userIds.length)]!;
				const content = randomComment();

				await (sql as SqlFunction)`
					INSERT INTO comments (id, post_id, user_id, content, created_at)
					VALUES (${commentId}, ${postId}, ${userId}, ${content}, ${now})
				`;
			}

			return { success: true };
		});

		// Step 6: Seed likes
		await step.do("seed-likes", async () => {
			const now = Date.now();
			const existingLikes = new Set<string>();

			for (let i = 0; i < config.numLikes; i++) {
				const likeId = 4000 + i;
				let postId: number;
				let userId: number;
				let key: string;

				// Ensure unique post_id/user_id combinations
				do {
					postId = postIds[Math.floor(Math.random() * postIds.length)]!;
					userId = userIds[Math.floor(Math.random() * userIds.length)]!;
					key = `${postId}-${userId}`;
				} while (existingLikes.has(key));

				existingLikes.add(key);

				await (sql as SqlFunction)`
					INSERT INTO likes (id, post_id, user_id, created_at)
					VALUES (${likeId}, ${postId}, ${userId}, ${now})
				`;
			}

			return { success: true };
		});

		return {
			success: true,
			message: "Database seeded successfully",
			stats: {
				users: config.numUsers,
				posts: config.numPosts,
				comments: config.numComments,
				likes: config.numLikes,
			},
		};
	}
}

/**
 * Blog Benchmark Worker
 */
export default class BlogBenchmarkWorker extends WorkerEntrypoint<BenchmarkEnv> {
	override async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);
		const correlationId =
			request.headers.get("x-correlation-id") || crypto.randomUUID();

		try {
			// Route handlers
			switch (url.pathname) {
				case "/":
					return this.handleRoot();

				case "/setup":
					return await this.handleSetup(correlationId);

				case "/seed":
					return await this.handleSeed(request, correlationId);

				case "/reset":
					return await this.handleReset(correlationId);

				case "/stats":
					return await this.handleStats(correlationId);

				case "/query/users":
					return await this.handleQueryUsers(url, correlationId);

				case "/query/posts":
					return await this.handleQueryPosts(url, correlationId);

				case "/query/post-with-comments":
					return await this.handleQueryPostWithComments(url, correlationId);

				case "/query/user-feed":
					return await this.handleQueryUserFeed(url, correlationId);

				default:
					return new Response(JSON.stringify({ error: "Not found" }), {
						status: 404,
						headers: { "Content-Type": "application/json" },
					});
			}
		} catch (error) {
			return new Response(
				JSON.stringify({
					error: error instanceof Error ? error.message : String(error),
					correlationId,
				}),
				{
					status: 500,
					headers: { "Content-Type": "application/json" },
				},
			);
		}
	}

	/**
	 * Root endpoint - API documentation
	 */
	private handleRoot(): Response {
		return new Response(
			JSON.stringify({
				name: "Blog Benchmark API",
				version: "1.0.0",
				endpoints: {
					"/setup": "POST - Initialize database schema",
					"/seed":
						"POST - Seed database with test data (triggers workflow). Body: { users, posts, comments, likes }",
					"/reset": "POST - Drop all tables",
					"/stats": "GET - Get database statistics",
					"/query/users": "GET - Query users by username",
					"/query/posts": "GET - Query posts by user_id",
					"/query/post-with-comments": "GET - Get post with all comments",
					"/query/user-feed": "GET - Get user feed with posts and likes",
				},
			}),
			{
				headers: { "Content-Type": "application/json" },
			},
		);
	}

	/**
	 * Setup: Create schema (without data)
	 */
	private async handleSetup(correlationId: string): Promise<Response> {
		const startTime = Date.now();
		const databaseId = "blog-benchmark";

		const sql = await createConnection(
			databaseId,
			{ nodes: 8, correlationId },
			this.env as unknown as BigDaddyEnv,
		);

		// Users table
		await sql`
			CREATE TABLE IF NOT EXISTS users (
				id INTEGER PRIMARY KEY,
				username TEXT NOT NULL,
				email TEXT NOT NULL,
				bio TEXT,
				created_at INTEGER NOT NULL
			)
		`;
		await sql`CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)`;

		// Posts table
		await sql`
			CREATE TABLE IF NOT EXISTS posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER NOT NULL,
				title TEXT NOT NULL,
				content TEXT NOT NULL,
				published_at INTEGER NOT NULL
			)
		`;
		await sql`CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id)`;

		// Comments table
		await sql`
			CREATE TABLE IF NOT EXISTS comments (
				id INTEGER PRIMARY KEY,
				post_id INTEGER NOT NULL,
				user_id INTEGER NOT NULL,
				content TEXT NOT NULL,
				created_at INTEGER NOT NULL
			)
		`;
		await sql`CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id)`;
		await sql`CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id)`;

		// Likes table
		await sql`
			CREATE TABLE IF NOT EXISTS likes (
				id INTEGER PRIMARY KEY,
				post_id INTEGER NOT NULL,
				user_id INTEGER NOT NULL,
				created_at INTEGER NOT NULL
			)
		`;
		await sql`CREATE INDEX IF NOT EXISTS idx_likes_post_user ON likes(post_id, user_id)`;

		const duration = Date.now() - startTime;

		return new Response(
			JSON.stringify({
				success: true,
				message: "Blog schema created",
				duration,
				correlationId,
			}),
			{
				headers: { "Content-Type": "application/json" },
			},
		);
	}

	/**
	 * Seed: Trigger workflow to populate database
	 */
	private async handleSeed(
		request: Request,
		correlationId: string,
	): Promise<Response> {
		const startTime = Date.now();
		const databaseId = "blog-benchmark";

		// Parse seed configuration from request body
		let config: SeedConfig;
		try {
			const body = await request.json<Partial<SeedConfig>>();
			config = {
				numUsers: body.numUsers || 10,
				numPosts: body.numPosts || 50,
				numComments: body.numComments || 100,
				numLikes: body.numLikes || 200,
			};
		} catch {
			config = {
				numUsers: 10,
				numPosts: 50,
				numComments: 100,
				numLikes: 200,
			};
		}

		// Trigger the workflow
		const instance = await this.env.SEED_WORKFLOW.create({
			params: {
				databaseId,
				config,
			},
		});

		const duration = Date.now() - startTime;

		return new Response(
			JSON.stringify({
				success: true,
				message: "Seeding workflow started",
				workflowId: instance.id,
				config,
				duration,
				correlationId,
			}),
			{
				headers: { "Content-Type": "application/json" },
			},
		);
	}

	/**
	 * Reset: Drop all tables
	 */
	private async handleReset(correlationId: string): Promise<Response> {
		const startTime = Date.now();
		const databaseId = "blog-benchmark";

		const sql = await createConnection(
			databaseId,
			{ nodes: 8, correlationId },
			this.env as unknown as BigDaddyEnv,
		);

		await sql`DROP TABLE IF EXISTS likes`;
		await sql`DROP TABLE IF EXISTS comments`;
		await sql`DROP TABLE IF EXISTS posts`;
		await sql`DROP TABLE IF EXISTS users`;

		const duration = Date.now() - startTime;

		return new Response(
			JSON.stringify({
				success: true,
				message: "All tables dropped",
				duration,
				correlationId,
			}),
			{
				headers: { "Content-Type": "application/json" },
			},
		);
	}

	/**
	 * Stats: Get database statistics
	 */
	private async handleStats(correlationId: string): Promise<Response> {
		const startTime = Date.now();
		const databaseId = "blog-benchmark";

		const sql = await createConnection(
			databaseId,
			{ nodes: 8, correlationId },
			this.env as unknown as BigDaddyEnv,
		);

		const usersResult = await sql`SELECT COUNT(*) as count FROM users`;
		const postsResult = await sql`SELECT COUNT(*) as count FROM posts`;
		const commentsResult = await sql`SELECT COUNT(*) as count FROM comments`;
		const likesResult = await sql`SELECT COUNT(*) as count FROM likes`;

		const duration = Date.now() - startTime;

		return new Response(
			JSON.stringify({
				success: true,
				stats: {
					users: usersResult.rows[0]?.count || 0,
					posts: postsResult.rows[0]?.count || 0,
					comments: commentsResult.rows[0]?.count || 0,
					likes: likesResult.rows[0]?.count || 0,
				},
				duration,
				correlationId,
			}),
			{
				headers: { "Content-Type": "application/json" },
			},
		);
	}

	/**
	 * Query users by username (index scan)
	 */
	private async handleQueryUsers(
		url: URL,
		correlationId: string,
	): Promise<Response> {
		const startTime = Date.now();
		const databaseId = "blog-benchmark";
		const username = url.searchParams.get("username") || "HappyPanda%";

		const sql = await createConnection(
			databaseId,
			{ nodes: 8, correlationId },
			this.env as unknown as BigDaddyEnv,
		);

		const result =
			await sql`SELECT * FROM users WHERE username LIKE ${username} LIMIT 10`;

		const duration = Date.now() - startTime;

		return new Response(
			JSON.stringify({
				success: true,
				count: result.rows.length,
				users: result.rows,
				cacheStats: result.cacheStats,
				duration,
				correlationId,
			}),
			{
				headers: { "Content-Type": "application/json" },
			},
		);
	}

	/**
	 * Query posts by user_id (index scan)
	 */
	private async handleQueryPosts(
		url: URL,
		correlationId: string,
	): Promise<Response> {
		const startTime = Date.now();
		const databaseId = "blog-benchmark";
		const userId = parseInt(url.searchParams.get("user_id") || "1000", 10);

		const sql = await createConnection(
			databaseId,
			{ nodes: 8, correlationId },
			this.env as unknown as BigDaddyEnv,
		);

		const result =
			await sql`SELECT * FROM posts WHERE user_id = ${userId} LIMIT 10`;

		const duration = Date.now() - startTime;

		return new Response(
			JSON.stringify({
				success: true,
				userId,
				count: result.rows.length,
				posts: result.rows,
				cacheStats: result.cacheStats,
				duration,
				correlationId,
			}),
			{
				headers: { "Content-Type": "application/json" },
			},
		);
	}

	/**
	 * Query post with all comments (join-like query)
	 */
	private async handleQueryPostWithComments(
		url: URL,
		correlationId: string,
	): Promise<Response> {
		const startTime = Date.now();
		const databaseId = "blog-benchmark";
		const postId = parseInt(url.searchParams.get("post_id") || "2000", 10);

		const sql = await createConnection(
			databaseId,
			{ nodes: 8, correlationId },
			this.env as unknown as BigDaddyEnv,
		);

		const postResult = await sql`SELECT * FROM posts WHERE id = ${postId}`;
		const commentsResult =
			await sql`SELECT * FROM comments WHERE post_id = ${postId}`;

		const duration = Date.now() - startTime;

		return new Response(
			JSON.stringify({
				success: true,
				post: postResult.rows[0] || null,
				comments: commentsResult.rows,
				commentCount: commentsResult.rows.length,
				duration,
				correlationId,
			}),
			{
				headers: { "Content-Type": "application/json" },
			},
		);
	}

	/**
	 * Query user feed (posts with like counts)
	 */
	private async handleQueryUserFeed(
		url: URL,
		correlationId: string,
	): Promise<Response> {
		const startTime = Date.now();
		const databaseId = "blog-benchmark";
		const userId = parseInt(url.searchParams.get("user_id") || "1000", 10);

		const sql = await createConnection(
			databaseId,
			{ nodes: 8, correlationId },
			this.env as unknown as BigDaddyEnv,
		);

		// Get user's posts
		const postsResult =
			await sql`SELECT * FROM posts WHERE user_id = ${userId} LIMIT 10`;

		// For each post, get like count (in production, this would be a single JOIN query)
		const postsWithLikes = await Promise.all(
			(postsResult.rows as { id: number }[]).map(async (post) => {
				const likesResult =
					await sql`SELECT COUNT(*) as count FROM likes WHERE post_id = ${post.id}`;
				return {
					...post,
					likeCount: likesResult.rows[0]?.count || 0,
				};
			}),
		);

		const duration = Date.now() - startTime;

		return new Response(
			JSON.stringify({
				success: true,
				userId,
				posts: postsWithLikes,
				duration,
				correlationId,
			}),
			{
				headers: { "Content-Type": "application/json" },
			},
		);
	}
}
