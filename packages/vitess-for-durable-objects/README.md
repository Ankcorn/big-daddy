# Vitess for Durable Objects

A distributed database system built on Cloudflare Durable Objects, inspired by Vitess (Google's MySQL sharding system).

## Architecture

### Three-Layer Design

1. **Storage Nodes** - Physical data storage
   - Each node is a Durable Object with its own SQLite database
   - Immutable once created - the number of nodes is fixed at cluster creation
   - Named `node-0`, `node-1`, `node-2`, etc.

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
- For SELECT queries: queries all shards and merges results
- For INSERT/UPDATE/DELETE: currently uses first shard only (to be improved with WHERE clause parsing)

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

- ✅ **DDL Support** - CREATE TABLE via Conductor with automatic topology updates
- ✅ **SQL parsing** - Full support for `?` placeholders with tagged template literals
- ✅ **Virtual sharding** - Logical shards with explicit table_shards mapping layer for flexible node distribution
- ✅ **Automatic query routing** - Routes queries to appropriate shards based on table_shards mapping
- ✅ **Multi-shard execution** - Executes queries across shards and merges results
- ✅ **Immutable storage nodes** - Node count is fixed at cluster creation for stability
- ✅ **Health monitoring** - Periodic alarms check storage node capacity and status

## Roadmap / TODO

### Shard Rebalancing
The table_shards mapping layer is now in place, enabling future enhancements:

- **Manual shard rebalancing** - Move specific shards between nodes to balance load
- **Non-uniform distribution** - Place hot shards on dedicated nodes
- **Data migration** - Move actual data when reassigning shards to new nodes

### Smart Query Routing
- Parse WHERE clauses to determine which shards contain relevant data
- Extract shard key values from INSERT/UPDATE/DELETE queries
- Route writes to the correct shard based on the shard key
- Support hash-based and range-based shard key distribution

### Query Optimization
- Push down WHERE clauses to storage nodes
- Parallel query execution across shards
- Query result streaming for large result sets

### DDL Enhancements
- ALTER TABLE propagation across shards
- DROP TABLE cleanup and topology updates
- CREATE INDEX support with automatic distribution

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
