# Virtual Indexes Specification

## Implementation Status

**Current Phase:** Phase 2 In Progress ⏳ - Index Creation Infrastructure Complete

**Phases:**
- ✅ Phase 1: Queue Infrastructure (COMPLETED)
- ⏳ Phase 2: Foundation (Index Creation) - Infrastructure Complete, Building Algorithm Pending
- ⏳ Phase 3: Query Optimization
- ⏳ Phase 4: Index Maintenance
- ⏳ Phase 5: Advanced Features

**What's Working:**
- ✅ Cloudflare Queue producer/consumer configured
- ✅ Job types defined (IndexBuildJob, IndexUpdateJob)
- ✅ Conductor has `enqueueIndexJob()` helper
- ✅ Queue consumer skeleton ready to process jobs
- ✅ Topology schema: virtual_indexes and virtual_index_entries tables
- ✅ Topology methods: createVirtualIndex, updateIndexStatus, batchUpsertIndexEntries, getIndexedShards, dropVirtualIndex
- ✅ Batch upsert using single-entry writes (simple, safe, and fast given DO write performance)
- ✅ CREATE INDEX parsing in Conductor
- ✅ handleCreateIndex method in Conductor (validates, creates index definition, enqueues job)
- ✅ IF NOT EXISTS support for CREATE INDEX
- ✅ UNIQUE index support
- ✅ Comprehensive tests for Topology virtual index methods (12 tests including batch operations)
- ✅ Comprehensive tests for CREATE INDEX in Conductor (6 tests)

**Next Steps:**
- Implement index building algorithm in queue consumer (processBuildIndexJob)
- Test end-to-end index creation flow with queue processing

---

## Overview

Virtual indexes are metadata-only indexes stored in the Topology that enable efficient query routing without requiring physical index structures on each shard. They map index key values to the specific shards that contain matching rows.

## Problem

Without indexes, queries like `SELECT * FROM users WHERE email = 'alice@example.com'` must:
1. Query all shards (e.g., 100 shards)
2. Each shard scans its entire table
3. Results are merged at the Conductor

This is inefficient even though the Conductor can route based on the primary key (shard key).

## Solution: Virtual Indexes

Store index metadata in Topology that maps: `(table, index_column, value) → [shard_ids]`

### Example

Table: `users` with 100 shards
Index: `email`

```typescript
virtual_indexes: {
  'users:email:alice@example.com': [3, 47],      // Alice's records on shards 3 and 47
  'users:email:bob@example.com': [12],           // Bob's record on shard 12
  'users:email:charlie@example.com': [3, 88, 91] // Charlie has records on 3 shards
}
```

Now `WHERE email = 'alice@example.com'` only queries shards 3 and 47 instead of all 100.

## Architecture

### 1. Index Definition (Topology)

```typescript
interface VirtualIndex {
  table_name: string;
  column_name: string;
  index_type: 'hash' | 'unique';  // unique = at most one value per key
  created_at: string;
}

interface VirtualIndexEntry {
  table_name: string;
  column_name: string;
  key_value: string;              // The indexed value (stringified)
  shard_ids: number[];            // Which shards contain this value
  updated_at: string;
}
```

### 2. Index Maintenance (Also Queue-Based)

Index maintenance for INSERT/UPDATE/DELETE should also be async to avoid blocking queries.

#### On INSERT:
```sql
INSERT INTO users (id, email, name) VALUES (123, 'alice@example.com', 'Alice')
```

**Immediate (Conductor):**
1. Determines target shard (e.g., shard 47) based on `id` hash
2. Executes INSERT on shard 47
3. Enqueues index update job

**Background (Queue Consumer):**
```typescript
interface IndexUpdateJob {
  type: 'index_update';
  operation: 'insert';
  table_name: string;
  shard_id: number;
  indexed_values: Record<string, any>; // { email: 'alice@example.com' }
}
```
- Updates virtual index: Add 47 to `users:email:alice@example.com`

#### On UPDATE:
```sql
UPDATE users SET email = 'newemail@example.com' WHERE id = 123
```

**Immediate (Conductor):**
1. Routes to shard 47 (based on id)
2. Fetches old indexed column values: `SELECT email FROM users WHERE id = 123`
3. Executes UPDATE on shard 47
4. Enqueues index update job with both old and new values

**Background (Queue Consumer):**
```typescript
interface IndexUpdateJob {
  type: 'index_update';
  operation: 'update';
  table_name: string;
  shard_id: number;
  old_values: Record<string, any>; // { email: 'alice@example.com' }
  new_values: Record<string, any>; // { email: 'newemail@example.com' }
}
```
- Remove 47 from `users:email:alice@example.com`
- Add 47 to `users:email:newemail@example.com`

#### On DELETE:
```sql
DELETE FROM users WHERE id = 123
```

**Immediate (Conductor):**
1. Routes to shard 47
2. Fetches indexed column values before delete
3. Executes DELETE on shard 47
4. Enqueues index update job

**Background (Queue Consumer):**
```typescript
interface IndexUpdateJob {
  type: 'index_update';
  operation: 'delete';
  table_name: string;
  shard_id: number;
  indexed_values: Record<string, any>; // { email: 'alice@example.com' }
}
```
- Remove 47 from `users:email:alice@example.com`

#### Why Queue-Based Maintenance?

**Benefits:**
- ✅ **Fast writes** - INSERT/UPDATE/DELETE return immediately
- ✅ **Decouples concerns** - Query execution separate from index maintenance
- ✅ **Handles failures gracefully** - Can retry index updates without failing queries
- ✅ **Batching opportunity** - Can batch multiple index updates together

**Trade-offs:**
- ⚠️ **Eventually consistent** - Brief lag between write and index update (typically < 100ms)
- ⚠️ **Acceptable** - Queries may hit extra shards during lag, still return correct results
- ✅ **Repair mechanism** - Background job can detect and fix inconsistencies

### 3. Index Usage (Query Routing)

```sql
SELECT * FROM users WHERE email = 'alice@example.com'
```

**Enhanced routing logic:**
1. Parse query → detect `WHERE email = 'alice@example.com'`
2. Check Topology for virtual index on `users.email`
3. Lookup `users:email:alice@example.com` → get [3, 47]
4. Query only shards 3 and 47 (instead of all 100)
5. Merge results

### 4. Index Creation (Async with Cloudflare Queue)

```sql
CREATE INDEX idx_email ON users(email)
```

**Queue-based architecture to avoid blocking:**

#### Immediate Response (< 10ms):
1. Conductor receives `CREATE INDEX` statement
2. Validates syntax and column exists
3. Creates index definition in Topology with `status: 'building'`
4. Enqueues index build job to Cloudflare Queue
5. Returns success immediately to client

#### Background Processing (Cloudflare Queue Consumer):
```typescript
interface IndexBuildJob {
  type: 'build_index';
  table_name: string;
  column_name: string;
  index_name: string;
  database_id: string;
}
```

**Build process:**
1. Queue consumer receives job
2. Query all shards in batches: `SELECT DISTINCT column_value FROM table`
3. For each distinct value:
   - Query shards to find which contain that value
   - Create virtual_index_entry in Topology
4. Update index status: `building` → `ready`
5. If failure: Update status to `failed`, include error message

#### Index Status:
```typescript
type IndexStatus =
  | 'building'   // Initial state, background job processing
  | 'ready'      // Built and ready to use
  | 'failed'     // Build failed, see error_message
  | 'rebuilding' // Rebuild in progress (triggered manually or by repair job)
```

**Query behavior during build:**
- Index with `status: 'building'` → ignored, query all shards (normal behavior)
- Index with `status: 'ready'` → used for query optimization
- Index with `status: 'failed'` → ignored, query all shards

## Implementation Phases

### Phase 1: Queue Infrastructure ✅ COMPLETED
- ✅ Set up Cloudflare Queue binding in wrangler.jsonc
  - Producer: `INDEX_QUEUE` binding on `vitess-index-jobs` queue
  - Consumer: Configured with batch_size=10, timeout=5s, retries=3
- ✅ Create queue consumer handler
  - Queue consumer configuration added to main `wrangler.jsonc`
  - `src/queue-consumer.ts` - Queue message handler (`queueHandler` function)
  - Exported from `src/index.ts` as `queue` handler
  - Single worker handles both HTTP requests and queue messages
- ✅ Define job types (IndexBuildJob, IndexUpdateJob)
  - `src/Queue/types.ts` - TypeScript interfaces for all job types
  - MessageBatch and Message types for Cloudflare Queue integration
- ✅ Implement job dispatcher in Conductor
  - Added `indexQueue?: Queue` to ConductorClient constructor
  - Added `enqueueIndexJob()` helper method
  - Integrated with `createConductor()` function
- ✅ Generated types with `npm run cf-typegen`
  - `worker-configuration.d.ts` includes INDEX_QUEUE binding
- [ ] Add queue monitoring/observability (future enhancement)

### Phase 2: Foundation (Index Creation)
- ✅ Topology schema for virtual_indexes table
- ✅ Topology schema for virtual_index_entries table
- ✅ Topology CRUD methods for virtual indexes
- ✅ Parse CREATE INDEX statement
- ✅ Immediate: Validate and create index definition (status: 'building')
- ✅ Enqueue IndexBuildJob
- ✅ Tests for Topology virtual index methods
- ✅ Tests for CREATE INDEX in Conductor
- [ ] Queue consumer: Build index from existing data (implementation pending)
- [ ] Queue consumer: Update status to 'ready' or 'failed' (implementation pending)
- [ ] Add DROP INDEX support (Topology method exists, needs Conductor integration)

### Phase 3: Query Optimization
- [ ] Extend determineShardTargets to check for indexes
- [ ] Lookup virtual_index_entries for simple WHERE clauses
- [ ] Use index to reduce shard list
- [ ] Handle index status (only use 'ready' indexes)
- [ ] Tests for index-based routing

### Phase 4: Index Maintenance
- [ ] On INSERT: Enqueue IndexUpdateJob with new values
- [ ] On UPDATE: Fetch old values, enqueue with old + new
- [ ] On DELETE: Fetch old values, enqueue with values to remove
- [ ] Queue consumer: Process index updates
- [ ] Handle batch updates for performance
- [ ] Tests for index maintenance

### Phase 5: Advanced Features
- [ ] Composite indexes: `CREATE INDEX ON users(country, city)`
- [ ] IN queries: `WHERE col IN (val1, val2)`
- [ ] Index validation/repair background job
- [ ] Index statistics (cardinality, usage metrics)
- [ ] Index rebuild command
- [ ] Range queries (requires different structure)

## Key Design Decisions

### 1. Why Virtual (Metadata-Only)?
- **No storage overhead on shards** - each shard doesn't maintain its own index
- **Centralized consistency** - single source of truth in Topology
- **Fast updates** - only Topology metadata changes, no shard writes
- **Works with existing data** - can build indexes without changing shard schema

### 2. Why String Keys?
- Simplifies implementation (no complex key encoding)
- Works with any data type (numbers, strings, dates)
- Easy to serialize/deserialize
- Consistent with how SQLite handles comparisons

### 3. Consistency Guarantees
- **Eventually consistent** - index may briefly be out of sync after failures
- **Acceptable for reads** - may query extra shards, still returns correct results
- **Repair via background job** - periodic index validation/rebuild

### 4. Storage Concerns
- Each unique value = one Topology entry
- High cardinality columns (like email) → many entries
- **Mitigation:** Limit to columns with reasonable cardinality
- **Future:** Shard the index itself if Topology becomes too large

## Example Queries

### Simple Equality (Index Used)
```sql
SELECT * FROM users WHERE email = 'alice@example.com'
-- Lookup: users:email:alice@example.com → [3, 47]
-- Query: Only shards 3, 47
```

### Compound Condition (Partial Index Use)
```sql
SELECT * FROM users WHERE email = 'alice@example.com' AND age > 25
-- Lookup: users:email:alice@example.com → [3, 47]
-- Query: Shards 3, 47 with full WHERE clause
-- Each shard filters by age > 25
```

### Shard Key + Index (Best Case)
```sql
SELECT * FROM users WHERE id = 123 AND email = 'alice@example.com'
-- Route by id → shard 47
-- Query: Only shard 47
-- No index lookup needed (shard key takes precedence)
```

### No Index Available
```sql
SELECT * FROM users WHERE name LIKE '%Alice%'
-- No index on name
-- Query: All shards (existing behavior)
```

## Cloudflare Queue Configuration

### wrangler.toml
```toml
[[queues.producers]]
queue = "vitess-index-jobs"
binding = "INDEX_QUEUE"

[[queues.consumers]]
queue = "vitess-index-jobs"
max_batch_size = 10
max_batch_timeout = 5
max_retries = 3
dead_letter_queue = "vitess-index-dlq"
```

### Queue Consumer Worker
Separate worker that processes index jobs:
- Receives batches of up to 10 jobs
- Processes in parallel where possible
- Retries failed jobs (max 3 times)
- Dead letter queue for persistent failures

### Job Types
```typescript
type IndexJob = IndexBuildJob | IndexUpdateJob;

interface IndexBuildJob {
  type: 'build_index';
  database_id: string;
  table_name: string;
  column_name: string;
  index_name: string;
  created_at: string;
}

interface IndexUpdateJob {
  type: 'index_update';
  database_id: string;
  table_name: string;
  operation: 'insert' | 'update' | 'delete';
  shard_id: number;
  indexed_values: Record<string, any>;
  old_values?: Record<string, any>; // For updates
}
```

## Open Questions

1. **Index cardinality limits?** Should we warn/error on high-cardinality columns?
   - *Recommendation:* Warn if > 100k unique values, error if > 1M

2. **Index size limits?** What's the max size for virtual_index_entries in Topology?
   - *Consideration:* Durable Object storage limits, may need to shard index itself

3. **Queue batch size?** How many index updates to batch together?
   - *Recommendation:* Start with 10, tune based on performance

4. **Composite indexes?** Support `(col1, col2)` in Phase 1 or defer?
   - *Recommendation:* Defer to Phase 5

5. **NULL handling?** How do we index NULL values?
   - *Recommendation:* Index as string "NULL", distinguish from actual "NULL" string

6. **Retry strategy?** What if queue consumer fails?
   - *Solution:* Cloudflare Queue handles retries (max 3), then DLQ

7. **Index consistency?** What if index update fails but query succeeds?
   - *Solution:* Eventually consistent, repair job validates periodically
