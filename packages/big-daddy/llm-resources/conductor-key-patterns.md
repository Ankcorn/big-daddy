# Conductor Key Patterns & Gotchas

## Virtual Shard Pattern
Every table has a hidden `_virtualShard` column:
- Added at CREATE TABLE time as composite PK: `(_virtualShard, original_pk)`
- Allows same PK value on different shards (critical for resharding)
- Filtered on every query, stripped from SELECT results

## Shard Routing
1. **Shard key queries** → single shard (hash-based)
2. **Indexed column queries** → subset of shards (virtual index lookup)
3. **Non-indexed queries** → ALL shards (expensive fan-out)

## Index Maintenance Patterns

### Synchronous (INSERT)
- Values extracted from INSERT statement
- Index entries added immediately during query

### Asynchronous (UPDATE/DELETE)
- SELECT captures old values before mutation
- Events queued for background processing
- Eventual consistency for index updates

## Cache Invalidation Strategy
- **Table cache**: Always invalidated on writes
- **Index cache**:
  - INSERT: Specific key invalidation
  - UPDATE/DELETE: Full index invalidation (conservative)

## Per-Shard Row Count Tracking

Row counts are tracked per-shard in `table_shards.row_count`:

| Operation | Action |
|-----------|--------|
| INSERT | Increment by VALUES.length per shard |
| DELETE | Decrement by rowsAffected per shard |
| UPDATE | No change (row count unchanged) |
| Resharding | Set exact counts after atomic switch |

**Why VALUES.length for INSERT?**
SQLite's `rowsWritten` counts B-tree operations (table + indexes), which inflates the count 2x for tables with composite primary keys. Using VALUES.length gives the logical row count.

**Topology Methods:**
- `getTableShardRowCounts(table)` → get all shard counts
- `bumpTableShardRowCount(table, shard, delta)` → single shard
- `batchBumpTableShardRowCounts(table, deltaMap)` → multiple shards
- `setTableShardRowCounts(table, countsMap)` → set exact (resharding)

## Effect Usage
Table operations (`create-drop.ts`) use Effect for:
- Typed errors (`TableAlreadyExistsError`, `TopologyFetchError`, etc.)
- Parallel execution with `Effect.all({ concurrency: 'unbounded' })`
- Error recovery and logging

CRUD operations (`write.ts`) use Effect for:
- Shard execution with `ShardQueryExecutionError`
- Batch processing with sequential/parallel composition

## Test Environment Handling
`indexes/create.ts` has special test logic:
```typescript
if (env) {
  // Immediately process queue in test environment
  const { queueHandler } = await import('../../../queue-consumer');
  await queueHandler(batch, env, correlationId);
}
```

## Common Pitfalls

### Parameter Remapping
Multi-row INSERTs require parameter index remapping per shard:
- Original: `[p0, p1, p2, p3, p4, p5]` (2 rows × 3 cols)
- Shard 0: `[p0, p1, p2]` with indices remapped to `[0, 1, 2]`
- Shard 1: `[p3, p4, p5]` with indices remapped to `[0, 1, 2]`

### Batch Execution Results
When executing statement batches:
- Single statement → `QueryResult[]` (one per shard)
- Multiple statements → `QueryResult[][]` (batch per shard)

Extract correctly:
```typescript
// UPDATE with indexes: [SELECT, UPDATE, SELECT]
const updateResults = resultsArray.map(batch => batch[1]);
```

### Aggregation Merging
`mergeAggregations()` handles cross-shard aggregates:
- COUNT/SUM: Sum all values
- MIN/MAX: Take min/max across shards
- AVG: Average of averages (approximation - ideally need SUM+COUNT)

## Subrequest Batching
Cloudflare limit: 7 subrequests per request
- Shards processed in batches of 7
- Batches run sequentially, shards within batch run in parallel
