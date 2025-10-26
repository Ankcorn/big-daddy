# Engine

The **engine** is the core distributed SQL database system for Vitess for Durable Objects. It provides horizontal scaling for SQLite by sharding data across multiple Durable Objects, inspired by Vitess's architecture.

## Purpose

Build a scalable alternative to Cloudflare D1 that overcomes single-instance limitations:

- **Higher throughput**: Parallel query execution across shards
- **Larger database sizes**: Data distributed across many Durable Objects
- **Horizontal scalability**: Add more shards as your data grows

## Architecture Overview

The engine consists of three core components that work together to provide distributed SQL:

```
┌─────────────────────────────────────────────────────────────┐
│                          Client                              │
│                  conductor.sql`SELECT...`                    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                       Conductor                              │
│  • Parse SQL to AST                                          │
│  • Check topology cache (66%+ hit rate!)                     │
│  • Route queries to shards                                   │
│  • Execute in parallel (batched)                             │
│  • Merge results + cache stats                               │
└──────────────┬─────────────────────────┬────────────────────┘
               │                         │
               │ getQueryPlanData()      │ executeQuery()
               │ (cached!)               │
               ▼                         ▼
┌──────────────────────────┐   ┌─────────────────────────────┐
│       Topology           │   │   Storage (Shards 0-N)      │
│  • Cluster metadata      │   │  • SQLite databases         │
│  • Table schemas         │   │  • Execute queries          │
│  • Shard mappings        │   │  • Return rows              │
│  • Virtual indexes       │   │                             │
│  • Routing decisions     │   │  [Shard 0] [Shard 1] ...    │
└──────────────────────────┘   └─────────────────────────────┘
```

---

## Components

### `conductor.ts` - Query Gateway

**Role**: Client-facing API and query coordinator (analogous to Vitess VTGate)

The Conductor is the entry point for all SQL queries. It orchestrates the complete query lifecycle:

1. **Parse**: Convert SQL to AST using `@databases/sqlite-ast`
2. **Route**: Get query plan from Topology (with caching)
3. **Execute**: Send queries to target Storage shards in parallel
4. **Merge**: Combine results from all shards
5. **Index Maintenance**: Enqueue async jobs to update virtual indexes
6. **Cache Management**: Invalidate cache entries for write operations

**Key Features**:
- Tagged template literal API: `conductor.sql`SELECT * FROM users WHERE id = ${id}``
- Automatic parameter binding and sanitization
- **Topology cache**: Eliminates Topology DO calls for repeated queries
- Parallel execution with batching (respects Cloudflare's 7 subrequest limit)
- Special handling for DDL statements (`CREATE TABLE`, `CREATE INDEX`)
- Non-blocking index maintenance via queue jobs
- **Cache statistics** in every SELECT query result

**Example**:
```typescript
const conductor = createConductor('my-database', env);

// Simple query - routes to single shard
const result = await conductor.sql`
  SELECT * FROM users WHERE id = ${userId}
`;

// Check cache performance
console.log(result.cacheStats);
// { cacheHit: false, totalHits: 0, totalMisses: 1, cacheSize: 1 }

// Same query again - cache hit!
const result2 = await conductor.sql`
  SELECT * FROM users WHERE id = ${userId}
`;
console.log(result2.cacheStats);
// { cacheHit: true, totalHits: 1, totalMisses: 1, cacheSize: 1 }
```

---

### `topology.ts` - Cluster Control Plane

**Role**: Single source of truth for cluster metadata and routing logic

The Topology is a Durable Object that stores all cluster configuration and makes routing decisions. It knows:

- **Storage Nodes**: Which Durable Objects exist in the cluster
- **Tables**: Schema information (primary key, shard key, shard count)
- **Table Shards**: Mapping of shards to storage nodes
- **Virtual Indexes**: Index definitions and value-to-shard mappings

**Key Responsibilities**:

1. **Cluster Initialization**:
   ```typescript
   await topology.create(numNodes); // Create N storage nodes
   ```

2. **Query Routing**:
   ```typescript
   const planData = await topology.getQueryPlanData(
     tableName,
     parsedStatement,
     params
   );
   // Returns: { shardsToQuery: [...], virtualIndexes: [...] }
   ```

3. **Index Management**:
   - Store virtual index metadata
   - Maintain value → shard mappings
   - Optimize queries using index lookups

**Routing Intelligence**:
- Analyzes SQL WHERE clauses to determine target shards
- Uses virtual indexes to reduce query fan-out
- Falls back to all-shard queries when necessary

**Example Routing Scenarios**:
```typescript
// Shard key query → routes to 1 shard
SELECT * FROM users WHERE id = 123

// Indexed column → routes to 2 shards (via virtual index)
SELECT * FROM users WHERE email = 'alice@example.com'

// Non-indexed column → routes to ALL shards
SELECT * FROM users WHERE name = 'Alice'
```

---

### `storage.ts` - Data Storage Layer

**Role**: Individual SQLite database shard

Each Storage is a Durable Object containing a single SQLite database. It's the fundamental unit of data storage and query execution.

**Responsibilities**:
- Execute SQL queries on local SQLite database
- Handle single queries or batches
- Return rows and metadata (rows affected, query type)
- Provide database size information

**API**:
```typescript
interface Storage {
  // Execute query or batch of queries
  executeQuery(queries: QueryBatch | QueryBatch[]):
    Promise<QueryResult | BatchQueryResult>

  // Get current database size in bytes
  getDatabaseSize(): Promise<number>
}
```

**Characteristics**:
- One SQLite database per Storage instance
- Independent query execution (no cross-shard transactions)
- Local SQLite indexes for query performance
- Stateful - data persists in Durable Object storage

---

### `virtual-index.ts` - Cross-Shard Query Optimization

Virtual indexes solve the fan-out problem for non-shard-key queries.

**The Problem**:
Without indexes, a query like `WHERE email = 'user@example.com'` must check ALL shards (expensive):
```
Query → [Shard 0, Shard 1, ..., Shard 99] → Merge results
```

**The Solution**:
Virtual indexes track which shards contain which values:
```typescript
// Index metadata in Topology:
{
  'alice@example.com' → [shard_0, shard_5],
  'bob@example.com' → [shard_12],
  ...
}

// Query only hits 2 shards instead of 100!
Query → [Shard 0, Shard 5] → Merge results
```

**Index Lifecycle**:

1. **Creation**:
   ```sql
   CREATE INDEX idx_email ON users(email)
   ```
   - Creates SQLite indexes on all shards (for local performance)
   - Creates virtual index definition in Topology (status: 'building')
   - Enqueues async build job

2. **Building** (async via queue):
   - Scans all shards: `SELECT DISTINCT email FROM users`
   - Builds value → shard mapping
   - Stores in Topology
   - Updates status to 'ready'

3. **Maintenance** (async for UPDATE/DELETE, eager for INSERT):
   - INSERT: Synchronously adds entry to index
   - UPDATE/DELETE: Async job updates index entries

4. **Query Optimization**:
   - Topology checks for applicable indexes
   - Looks up values in index
   - Returns only relevant shards

**Index Types**:
- **Hash indexes**: Fast equality lookups (`WHERE col = value`)
- **Unique indexes**: Enforces uniqueness across all shards
- **Composite indexes**: Multi-column indexes (`WHERE a = ? AND b = ?`)

---

### Topology Cache - Performance Optimization

The topology cache eliminates redundant calls to the Topology Durable Object by caching query plan data in the Conductor's memory.

**The Problem**:
Every query requires a call to Topology to determine which shards to query. For read-heavy workloads, this adds latency and costs:
```
Query 1: SELECT WHERE id=1 → Call Topology → Query shard 5
Query 2: SELECT WHERE id=1 → Call Topology AGAIN → Query shard 5
```

**The Solution**:
Cache query plan data in the Conductor (Worker memory):
```
Query 1: SELECT WHERE id=1 → Call Topology → Cache result → Query shard 5
Query 2: SELECT WHERE id=1 → Cache HIT! → Query shard 5
```

**Cache Strategy**:

**What Gets Cached**:
- Query plan data (table metadata + shard targets) for SELECT queries with WHERE clauses
- Cache keys generated from table name + WHERE clause structure
- Full table scans (no WHERE) are NOT cached (not beneficial)

**TTL Settings**:
- Shard routing: 30 seconds (conservative)
- Index lookups: 1 minute (moderate)
- Table metadata: 5 minutes (very stable)

**Cache Invalidation**:
- INSERT: Invalidates table cache + specific index entries
- UPDATE: Invalidates table cache + affected index caches
- DELETE: Invalidates table cache + affected index caches
- CREATE/DROP TABLE/INDEX: Clears relevant cache entries

**Performance Impact**:
```typescript
// First query - cache miss
const r1 = await conductor.sql`SELECT * FROM users WHERE id = ${1}`;
console.log(r1.cacheStats);
// { cacheHit: false, totalHits: 0, totalMisses: 1, cacheSize: 1 }

// Second query - cache hit! (no Topology call)
const r2 = await conductor.sql`SELECT * FROM users WHERE id = ${1}`;
console.log(r2.cacheStats);
// { cacheHit: true, totalHits: 1, totalMisses: 1, cacheSize: 1 }
```

**Benefits**:
- **Reduced latency**: Eliminates Topology DO call for cache hits
- **Cost savings**: Fewer subrequests = lower costs
- **Better throughput**: 66%+ cache hit rate for repeated query patterns
- **Visibility**: Every SELECT query returns cache statistics

**Cache Stats in Results**:
Every SELECT query includes a `cacheStats` object:
```typescript
interface QueryCacheStats {
  cacheHit: boolean;      // Was THIS query a cache hit?
  totalHits: number;      // Total hits since conductor started
  totalMisses: number;    // Total misses
  cacheSize: number;      // Current cache size
}
```

**Memory Management**:
- LRU eviction with 10,000 entry limit (~1MB)
- Automatic cleanup of expired entries
- Per-conductor instance (not shared across Workers)

---

## Supporting Modules

### `utils/` - Utility Functions

**`ast-utils.ts`**: SQL parsing utilities
- `extractTableName()`: Extract table name from parsed SQL
- `getQueryType()`: Determine query type (SELECT/INSERT/UPDATE/DELETE/etc)
- `buildQuery()`: Construct parameterized query from template literal

**`schema-utils.ts`**: Schema metadata extraction
- `extractTableMetadata()`: Parse CREATE TABLE statement
  - Identifies primary key and shard key
  - Determines sharding strategy
  - Extracts column definitions

**`sharding.ts`**: Shard selection logic
- `getShard()`: Hash-based shard selection for a given key value
- Consistent hashing ensures even data distribution

**`topology-cache.ts`**: In-memory query plan caching
- Caches query plan data to eliminate Topology DO calls
- Supports different TTLs for different data types (30s-5min)
- LRU eviction with 10,000 entry limit
- Smart cache key generation from WHERE clauses
- Automatic invalidation on INSERT/UPDATE/DELETE operations

### `queue/types.ts` - Background Job Definitions

Defines async operations for index management:

**`IndexBuildJob`**: Build new virtual index
- Scans all shards for distinct values
- Populates index metadata in Topology
- Triggered by CREATE INDEX

**`IndexMaintenanceJob`**: Update existing indexes
- Keeps indexes in sync after data changes
- Triggered by UPDATE/DELETE operations
- Processes affected indexes only

---

## Data Flow Examples

### Example 1: Shard-Key Query (Single Shard)

```typescript
await conductor.sql`SELECT * FROM users WHERE id = ${123}`
```

**Flow**:
1. Conductor parses SQL → AST
2. Conductor → Topology: "Which shards for this query?"
3. Topology analyzes: `id` is shard key, hash(123) → shard 5
4. Topology → Conductor: `[shard_5]`
5. Conductor → Storage shard 5: `executeQuery()`
6. Storage executes on local SQLite, returns rows
7. Conductor → Client: Results (no merge needed)

**Result**: 1 shard queried ✅

---

### Example 2: Indexed Column Query (Optimized Fan-Out)

```typescript
await conductor.sql`SELECT * FROM users WHERE email = ${'alice@example.com'}`
```

**Flow**:
1. Conductor parses SQL → AST
2. Conductor → Topology: "Which shards for this query?"
3. Topology checks: `idx_email` virtual index exists
4. Topology looks up: `'alice@example.com'` → `[shard_0, shard_7]`
5. Topology → Conductor: `[shard_0, shard_7]`
6. Conductor queries both shards in parallel
7. Conductor merges results → Client

**Result**: 2 shards queried instead of 100 ✅

---

### Example 3: Full Table Scan (All Shards)

```typescript
await conductor.sql`SELECT * FROM users WHERE name = ${'Alice'}`
```

**Flow**:
1. Conductor parses SQL → AST
2. Conductor → Topology: "Which shards for this query?"
3. Topology analyzes: `name` is not indexed, not shard key
4. Topology → Conductor: `[all shards]`
5. Conductor queries all shards in parallel (batched in groups of 7)
6. Conductor merges results → Client

**Result**: All shards queried (expensive, but correct) ⚠️

---

### Example 4: CREATE INDEX (Async Build)

```typescript
await conductor.sql`CREATE INDEX idx_email ON users(email)`
```

**Flow**:
1. Conductor identifies CREATE INDEX statement
2. Conductor → All Storage shards: Create SQLite index (parallel)
3. Conductor → Topology: Create virtual index (status: 'building')
4. Conductor enqueues `IndexBuildJob` to queue
5. Conductor → Client: Success (non-blocking!)
6. **Background**: Queue consumer processes job:
   - Queries all shards: `SELECT DISTINCT email FROM users`
   - Builds mapping: `{ 'alice@...' → [shard_0], ... }`
   - Stores in Topology via `batchUpsertIndexEntries()`
   - Updates status → 'ready'

**Result**: Index created without blocking client ✅

---

### Example 5: INSERT with Index Maintenance

```typescript
await conductor.sql`
  INSERT INTO users (id, email, name)
  VALUES (${456}, ${'charlie@example.com'}, ${'Charlie'})
`
```

**Flow**:
1. Conductor parses SQL → AST
2. Conductor → Topology: "Which shards for this INSERT?"
3. Topology determines: hash(456) → shard 12
4. Topology checks: `idx_email` virtual index exists
5. Topology **eagerly updates** index: `'charlie@...'` → add shard 12
6. Topology → Conductor: `[shard_12]`
7. Conductor → Storage shard 12: `INSERT...`
8. Conductor → Client: Success

**Result**: Data inserted, index updated optimistically ✅

---

## Sharding Strategy

### Hash-Based Sharding

The engine uses **hash-based sharding** for data distribution:

```typescript
function getShard(keyValue: any, numShards: number): number {
  const hash = simpleHash(String(keyValue));
  return hash % numShards;
}
```

**Properties**:
- **Deterministic**: Same key always routes to same shard
- **Even distribution**: Hash function spreads data uniformly
- **Simple**: No range management, no rebalancing complexity
- **Predictable**: Easy to debug and reason about

**Shard Key Selection**:
- Defaults to the primary key
- Must be included in WHERE clause for single-shard routing
- Should have high cardinality for even distribution

---

## Performance Characteristics

### Query Performance

| Query Pattern | Shards Queried | Performance |
|---------------|----------------|-------------|
| `WHERE shard_key = ?` | 1 | ⚡ Excellent - Single shard |
| `WHERE indexed_col = ?` | Few (via virtual index) | ✅ Good - Reduced fan-out |
| `WHERE non_indexed_col = ?` | All | ⚠️ Poor - Full scan |
| `JOIN` (not yet supported) | N/A | ❌ Not implemented |

### Throughput Scaling

- **Linear scaling** with shard count (for shard-key queries)
- **Parallel execution** across shards
- **Batching** respects Cloudflare's subrequest limits
- **Topology cache**: Eliminates 66%+ of Topology DO calls for repeated queries
- **Bottleneck**: Topology is single Durable Object (mitigated by caching)

### Virtual Index Benefits

Example with 100 shards:

| Scenario | Without Index | With Index | Speedup |
|----------|---------------|------------|---------|
| `WHERE email = ?` | 100 shards | 2 shards | 50x fewer requests |
| `WHERE country = 'US'` | 100 shards | 30 shards | 3.3x fewer requests |
| Build time | N/A | ~5 seconds | One-time cost |

---

## Limitations & Trade-offs

### Current Limitations

1. **No cross-shard transactions**: Each shard is independent
2. **No JOINs across shards**: All JOIN data must be on same shard
3. **No foreign key constraints**: Enforcement would require cross-shard coordination
4. **Eventual consistency for indexes**: UPDATE/DELETE index maintenance is async
5. **Single Topology bottleneck**: All routing goes through one Durable Object

### Design Trade-offs

**Chosen**: Simple hash-based sharding
**Alternative**: Range-based sharding
**Reason**: Simpler implementation, no rebalancing, even distribution

**Chosen**: Async index maintenance (UPDATE/DELETE)
**Alternative**: Synchronous index updates
**Reason**: Don't block client queries, better write performance

**Chosen**: Virtual indexes (not distributed indexes)
**Alternative**: Scatter-gather all queries
**Reason**: Massive performance improvement for non-shard-key queries

---

## Comparison to D1

| Feature | D1 | Vitess for Durable Objects |
|---------|----|-----------------------------|
| **Architecture** | Single SQLite instance | Sharded across many SQLite instances |
| **Max Size** | ~10GB (estimated) | Virtually unlimited (add shards) |
| **Throughput** | Single-threaded | Parallel across shards |
| **Scaling** | Vertical only | Horizontal sharding |
| **Indexes** | Local SQLite only | SQLite + Virtual indexes |
| **Transactions** | Full ACID | Per-shard ACID only |
| **Consistency** | Strong | Strong per-shard, eventual for indexes |
| **Setup Complexity** | Minimal | Moderate (must initialize cluster) |

---

## Key Differences from Vitess

While inspired by Vitess, this engine is **not a direct port**:

| Aspect | Vitess | This Engine |
|--------|--------|-------------|
| **Backend** | MySQL | SQLite (Durable Objects) |
| **Topology Service** | etcd/Consul | Single Topology Durable Object |
| **VTGate** | Stateless proxy layer | Conductor (stateless) |
| **VTTablet** | MySQL manager | Not needed (DO handles lifecycle) |
| **Resharding** | Dynamic resharding | Static shard count |
| **Transactions** | 2PC distributed txns | Per-shard only |
| **Query Planning** | Full query planner | Basic routing logic |

**Philosophy**: Embrace Cloudflare's primitives (Durable Objects) rather than replicate Vitess's Kubernetes-based architecture.

---

## Future Enhancements

Potential improvements to consider:

1. **Smart resharding**: Dynamic shard splitting as data grows
2. **Cross-shard transactions**: 2PC or Sagas for multi-shard writes
3. **Range indexes**: Support for `WHERE col > ?` queries
4. **Aggregation pushdown**: Execute COUNT/SUM/AVG on shards before merging
5. **Read replicas**: Add read-only Storage replicas for higher read throughput

---

## Getting Started

### Initialize a Cluster

```typescript
// Create topology with 10 storage nodes
const topologyId = env.TOPOLOGY.idFromName('my-database');
const topology = env.TOPOLOGY.get(topologyId);
await topology.create(10);

// Create conductor
const conductor = createConductor('my-database', env);

// Create table
await conductor.sql`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT,
    name TEXT
  )
`;

// Create index for better query performance
await conductor.sql`CREATE INDEX idx_email ON users(email)`;
```

### Execute Queries

```typescript
// Insert (routes to single shard)
await conductor.sql`
  INSERT INTO users (id, email, name)
  VALUES (${1}, ${'alice@example.com'}, ${'Alice'})
`;

// Query by shard key (single shard)
const user = await conductor.sql`
  SELECT * FROM users WHERE id = ${1}
`;

// Query by indexed column (optimized fan-out)
const users = await conductor.sql`
  SELECT * FROM users WHERE email = ${'alice@example.com'}
`;

// Full table scan (all shards)
const allUsers = await conductor.sql`
  SELECT * FROM users
`;
```

---

## Summary

The **engine** provides a complete distributed SQL database system:

- **Storage**: Sharded SQLite databases (the data layer)
- **Topology**: Cluster metadata and routing logic (the control plane)
- **Conductor**: Query coordination and execution (the gateway)
- **Virtual Indexes**: Cross-shard query optimization (the performance layer)

Together, they deliver **horizontal scalability** for SQLite workloads on Cloudflare's infrastructure, unlocking higher throughput and capacity than D1's single-instance model while maintaining simplicity and developer ergonomics.
