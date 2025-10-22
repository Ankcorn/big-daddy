# Vitess for Durable Objects

A distributed database system built on Cloudflare Durable Objects, inspired by Vitess (Google's MySQL sharding system).

## Architecture

### Three-Layer Design

1. **Storage Nodes** - Physical data storage
   - Each node is a Durable Object with its own SQLite database
   - Immutable once created - the number of nodes is fixed at cluster creation
   - Identified by unique, unguessable UUIDs (only accessible via Topology)

2. **Topology** - Cluster metadata
   - Tracks storage nodes and their health
   - Stores table definitions (schema, sharding strategy, shard count)
   - Single source of truth for cluster configuration

3. **Conductor** - Query router
   - Parses SQL queries using the sqlite-ast parser
   - Routes queries to appropriate storage shards
   - Merges results from multiple shards
   - Provides a simple `sql` tagged template literal API

### Virtual Sharding with table_shards

The system uses a **virtual sharding architecture** that separates logical shards from physical storage nodes.

#### How it works:
- Each table has `num_shards` **logical/virtual shards** (e.g., 1000 shards)
- The `table_shards` table maps each `(table_name, shard_id)` to a physical `node_id`
- When a table is created, shards are distributed across nodes using: `shard_id % num_storage_nodes`
- Queries look up the `table_shards` mapping to find which node to hit

#### Why virtual sharding?
Without virtual sharding, changing the number of storage nodes requires **rehashing all data** because the hash function changes:
- Old: `hash(key) % 3 nodes` → different results than `hash(key) % 5 nodes`
- This means moving data between nodes when scaling

With virtual sharding:
- Set a high fixed shard count (e.g., 1000 virtual shards)
- Remap shards to nodes without rehashing: just update the `table_shards` mapping
- Example: `shard-42` moves from `node-0` to `node-3` by updating one row
- The hash function `hash(key) % 1000` stays the same, only the node assignment changes

#### Query routing:
- **INSERT queries**: Routes to specific shard based on hash of shard key value
- **SELECT queries**:
  - If WHERE clause filters on shard key with equality (e.g., `WHERE id = 123`), routes to specific shard
  - If WHERE clause filters on other columns or uses complex conditions, queries all shards and merges results
  - If no WHERE clause, queries all shards and merges results
- **UPDATE/DELETE queries**:
  - If WHERE clause filters on shard key with equality (e.g., `WHERE id = 123`), routes to specific shard
  - If WHERE clause filters on other columns or uses complex conditions, queries all shards
  - If no WHERE clause, queries all shards

## Usage

```typescript
import { createConductor } from './Conductor/Conductor';

// Initialize the topology and storage nodes
const topologyStub = env.TOPOLOGY.get(env.TOPOLOGY.idFromName('my-database'));
await topologyStub.create(3); // Create 3 storage nodes

// Create a conductor for your database
const conductor = createConductor('my-database', env);

// Create tables - metadata is automatically inferred from the schema
await conductor.sql`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    age INTEGER
  )
`;

// Execute queries using tagged template literals
const userId = 123;
const result = await conductor.sql`SELECT * FROM users WHERE id = ${userId}`;

// Queries are automatically parameterized with ? placeholders
const name = 'John';
const age = 25;
await conductor.sql`
  SELECT * FROM users
  WHERE name = ${name} AND age > ${age}
`;
```

## Features

### Core Database Features
- ✅ **DDL Support** - CREATE TABLE via Conductor with automatic topology updates
- ✅ **SQL parsing** - Full support for `?` placeholders with tagged template literals
- ✅ **Virtual sharding** - Logical shards with explicit table_shards mapping layer for flexible node distribution
- ✅ **Smart INSERT routing** - Hashes shard key values to route INSERTs to correct shard
- ✅ **Smart SELECT routing** - Analyzes WHERE clauses to route to specific shards when filtering on shard key
- ✅ **Smart UPDATE/DELETE routing** - Parses WHERE clauses to route to specific shards when possible
- ✅ **Centralized routing logic** - Single `determineShardTargets()` method handles all query types
- ✅ **Correct placeholder tracking** - `sqlite-ast` tracks parameter positions for complex WHERE clauses
- ✅ **Clean query pipeline** - Four-step architecture: Parse → Route → Execute → Merge
- ✅ **Modular utilities** - Separated concerns: sharding, AST parsing, and schema extraction
- ✅ **Batched parallel execution** - Queries execute in batches of 7 shards to respect Cloudflare limits
- ✅ **Multi-shard result merging** - Combines results from multiple shards
- ✅ **Immutable storage nodes** - Node count is fixed at cluster creation with unguessable UUIDs
- ✅ **Health monitoring** - Periodic alarms check storage node capacity and status

### Virtual Indexes (In Progress - Phase 2)
- ✅ **Queue infrastructure** - Cloudflare Queue configured for async index operations
- ✅ **Index schema** - virtual_indexes and virtual_index_entries tables in Topology
- ✅ **CREATE INDEX parsing** - Handles CREATE INDEX statements with IF NOT EXISTS
- ✅ **Non-blocking index creation** - Returns immediately, builds index in background
- ⏳ **Index building** - Queue consumer processes IndexBuildJob (implementation pending)
- ⏳ **Index-based routing** - Use indexes to optimize query routing (Phase 3)
- ⏳ **Index maintenance** - Update indexes on INSERT/UPDATE/DELETE (Phase 4)

## Roadmap / TODO

### Shard Rebalancing
The table_shards mapping layer is now in place, enabling future enhancements:

- **Manual shard rebalancing** - Move specific shards between nodes to balance load
- **Non-uniform distribution** - Place hot shards on dedicated nodes
- **Data migration** - Move actual data when reassigning shards to new nodes

### Smart Query Routing
- ✅ Parse WHERE clauses to determine which shards contain relevant data (basic equality support)
- ✅ Route SELECT/UPDATE/DELETE to specific shards based on WHERE clause shard key
- ✅ Centralized shard selection logic (refactored into `determineShardTargets()`)
- Support range-based shard key distribution (in addition to hash-based)
- Support complex WHERE clause patterns (AND/OR, range queries, IN clauses)

### Query Optimization
- ✅ Parallel query execution across shards
- Push down WHERE clauses to storage nodes
- Query result streaming for large result sets
- Topology caching (currently fetched on every query)

### DDL Enhancements
- ALTER TABLE propagation across shards
- DROP TABLE cleanup and topology updates
- ✅ CREATE INDEX support - Phase 1-2 complete, background building in progress
- DROP INDEX support

### Advanced Features
- Transactions across multiple shards
- Secondary indexes
- Cross-shard JOINs
- Query planning and optimization

## Testing

```bash
npm test
```

All tests run using `@cloudflare/vitest-pool-workers` with `isolatedStorage: false`.

## License

ISC
