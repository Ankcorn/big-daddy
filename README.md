# Big Daddy

> **Tech Demo** - This is an experimental project exploring distributed SQL on Cloudflare's edge infrastructure. Not for production use.

A distributed database built on Cloudflare Durable Objects, using Vitess-like virtual sharding to scale SQLite across the edge.

![Dashboard](./dash.png)

## What This Demonstrates

This project explores whether you can build a horizontally-scalable SQL database entirely on Cloudflare Workers and Durable Objects. The key idea is **virtual sharding** - partitioning data across multiple SQLite-backed Durable Objects while presenting a single logical database to the client.

Each query is analyzed, routed to the relevant shards, executed in parallel, and the results are merged - all at the edge, coordinated by a Topology Durable Object.



## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Conductor                             │
│            (Query Planner + Tiered Metadata Cache)          │
│                                                             │
│  • Parses SQL using sqlite-ast                              │
│  • Determines which shards to query                         │
│  • Fans out requests, merges results                        │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌───────────────────┐ ┌─────────────┐ ┌─────────────────────┐
│     Topology      │ │   Storage   │ │       Storage       │
│  (Durable Object) │ │   Shard 1   │ │      Shard N        │
│                   │ │             │ │                     │
│ • Table metadata  │ │  • SQLite   │ │  • SQLite           │
│ • Shard mappings  │ │  • Indexes  │ │  • Indexes          │
│ • Index registry  │ │             │ │                     │
└───────────────────┘ └─────────────┘ └─────────────────────┘
```

**Conductor** - Cloudflare Worker that acts as the query gateway. Parses incoming SQL, consults the topology for routing, fans out to storage shards, and merges results.

**Topology** - Single Durable Object that stores metadata: table schemas, shard assignments, and index definitions.

**Storage** - N Durable Objects, each running SQLite. These are "dumb" storage nodes that execute queries they receive.

## Project Structure

```
packages/
├── big-daddy/       # Main application (Conductor, Topology, Storage)
├── sqlite-ast/      # Homegrown SQL parser for query analysis
├── benchmarks/      # Performance testing
└── tsconfig/        # Shared TypeScript config
```

## Getting Started

### Setup

```bash
# Install dependencies
pnpm install

# Login to wrangler (needed to parse SQL w/ AI)
# Or set a CLOUDFLARE_API_TOKEN in the projects .env file
wrangler login

# Run locally
pnpm dev

# Deploy to Cloudflare
pnpm deploy
```

### Binding to Big Daddy

Add a service binding in your worker's `wrangler.jsonc`:

```jsonc
{
  "services": [
    {
      "binding": "BIG_DADDY",
      "service": "big-daddy"
    }
  ]
}
```

### Example Usage

```typescript
// Create a connection via service binding (initializes topology with 10 storage nodes)
const sql = await env.BIG_DADDY.createConnection("my-database", { nodes: 10 });

// Create a table
await sql`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT,
    name TEXT
  )
`;

// Insert data (automatically routed to correct shard)
await sql`
  INSERT INTO users (id, email, name)
  VALUES (${1}, ${"alice@example.com"}, ${"Alice"})
`;

// Query with type safety
const result = await sql<{ id: number; name: string }>`
  SELECT id, name FROM users WHERE id = ${userId}
`;

// Aggregations fan out to all shards and merge results
const count = await sql`SELECT COUNT(*) FROM users`;
```

## Current Limitations

This is a tech demo with known limitations:

- **No cross-shard JOINs** - queries cannot join data across shards
- **No cross-shard transactions** - ACID guarantees only within a single shard
- **Approximate AVG** - averages the per-shard averages (not mathematically correct)
- **SQL subset** - no UNION, CTEs, window functions, or table aliases
