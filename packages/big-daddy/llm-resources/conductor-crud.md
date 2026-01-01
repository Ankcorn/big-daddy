# Conductor CRUD Operations

## SELECT (`crud/select.ts`)
**Flow:**
1. Extract table name from AST
2. Get cached query plan → `shardsToQuery`
3. Execute on shards in parallel
4. Merge results with `mergeResultsSimple()`
5. Add cache stats

**Key:** Uses topology cache to skip Topology DO calls on cache hits.

---

## INSERT (`crud/insert.ts`)
**Flow:**
1. Get query plan data (all shards for table)
2. `groupInsertByShards()` - hash shard key to determine target shard per row
3. Log write if resharding in progress
4. Execute per-shard INSERTs with remapped parameters
5. Dispatch index maintenance events
6. Invalidate cache
7. **Bump per-shard row counts via `batchBumpTableShardRowCounts()`**

**Key Functions:**
- `groupInsertByShards()` - Groups rows by target shard, remaps placeholder indices

**Important:** 
- Multi-row INSERTs are split by shard key hash
- Row counts use VALUES.length (not SQLite's rowsWritten which includes index writes)

---

## UPDATE (`crud/update.ts`)
**Flow:**
1. Get query plan
2. Log write if resharding
3. If indexes exist: `[SELECT before, UPDATE, SELECT after]` batch
4. Execute on shards
5. Dispatch index maintenance with old/new row diffs
6. Invalidate cache

**Key:** Captures indexed column values before AND after for proper index sync.

---

## DELETE (`crud/delete.ts`)
**Flow:**
1. Get query plan
2. Log write if resharding
3. If indexes exist: `[SELECT capture, DELETE]` batch
4. Execute on shards
5. Dispatch index maintenance with old rows
6. Invalidate cache
7. **Decrement per-shard row counts via `batchBumpTableShardRowCounts()`**

---

## Shared Utilities (`utils/write.ts`)

### `executeOnShards()`
- Batches shards in groups of 7 (Cloudflare subrequest limit)
- Injects `_virtualShard` filter via `injectVirtualShard()`
- Uses Effect for error handling
- Returns `{ results, shardStats }`

### `getCachedQueryPlanData()`
- Checks TopologyCache first
- Falls back to Topology DO on miss
- Caches result with appropriate TTL

### `invalidateCacheForWrite()`
- Invalidates table cache
- Invalidates affected index caches

### `logWriteIfResharding()`
- Logs writes to queue during active resharding for replay

---

## Virtual Shard Injection (`utils/helpers.ts`)

### For INSERT:
```typescript
// Adds _virtualShard column and placeholder per row
columns: [...original, '_virtualShard']
values: [...original, { type: 'Placeholder', parameterIndex: N }]
```

### For SELECT/UPDATE/DELETE:
```typescript
// Adds WHERE clause filter
WHERE original_condition AND _virtualShard = ?
```

---

## Index Maintenance (`utils/index-maintenance.ts`)

### `prepareIndexMaintenanceQueries()`
Returns statement batches based on operation:
- INSERT: `[INSERT]`
- DELETE: `[SELECT, DELETE]`
- UPDATE: `[SELECT before, UPDATE, SELECT after]`

### `dispatchIndexSyncingFromQueryResults()`
Generates and queues index events:
- INSERT → `add` events for new values
- DELETE → `remove` events for old values
- UPDATE → `remove` old + `add` new (diff-based)
