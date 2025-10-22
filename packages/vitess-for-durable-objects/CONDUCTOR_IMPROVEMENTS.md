# Conductor Improvements

This document lists potential improvements to `Conductor.ts` for better scalability and maintainability.

## High Priority - Performance

### 1. Add Topology Caching
**Current:** Every query fetches full topology from Durable Object (lines 60-63, 176-178)
**Problem:** Network latency on every query, topology rarely changes
**Solution:** In-memory cache with TTL or invalidation mechanism
**Complexity:** Medium
**Dependencies:** None

### 2. Parallelize Shard Queries
**Current:** Sequential `for` loop executes queries one at a time (lines 140-158)
**Problem:** N shards = N×latency instead of ~1×latency
**Solution:** Use `Promise.all()` to execute shard queries in parallel
**Complexity:** Low
**Dependencies:** None

## High Priority - Code Quality

### 3. Extract Shard Selection Logic
**Current:** Duplicate WHERE clause parsing for SELECT/UPDATE/DELETE (lines 81-136)
**Problem:** 45 lines of nearly identical code, hard to maintain
**Solution:** Extract into `determineShardTargets()` method
**Complexity:** Low
**Dependencies:** None

### 4. Refactor Monolithic sql() Method
**Current:** 125-line method doing parsing, routing, execution, merging (lines 42-167)
**Problem:** Hard to extend for virtual indexes and rebalancing
**Solution:** Break into: `parse -> route -> execute -> merge` pipeline
**Complexity:** Medium
**Dependencies:** Should do after #3


## Medium Priority - Architecture

### 6. Create QueryRouter Class
**Current:** Routing logic embedded in ConductorClient
**Problem:** Will be complex with virtual indexes and rebalancing
**Solution:** Separate class: `QueryRouter.determineShards(statement, topology, indexes)`
**Complexity:** Medium
**Dependencies:** Should do after #3 and #4

### 7. Create ShardExecutor Class
**Current:** Execution logic inline in sql() method
**Problem:** Need to handle rebalancing (double-writes, read fallback)
**Solution:** Separate class: `ShardExecutor.executeParallel(targets, query, rebalancingState)`
**Complexity:** Medium
**Dependencies:** Works well with #2

### 8. Abstract Shard Location for Rebalancing
**Current:** Assumes each shard has one active node_id
**Problem:** During rebalancing, need to track source/target nodes
**Solution:** Add shard status tracking:
```typescript
interface ShardLocation {
  node_id: string;
  status: 'active' | 'rebalancing_source' | 'rebalancing_target';
  rebalancing_to?: string;
}
```
**Complexity:** High
**Dependencies:** Requires topology schema changes

## Medium Priority - Robustness

### 9. Improve Hash Function
**Current:** Simple string hash (lines 375-389)
**Problem:** Not consistent hashing, no versioning
**Solution:** Use proven hash (MurmurHash/XXHash), add version field
**Complexity:** Low
**Dependencies:** None

### 10. Better Error Handling
**Current:** Throws errors immediately on topology/shard lookup failures
**Problem:** No retry logic, partial failure handling
**Solution:** Add retry policies, graceful degradation
**Complexity:** Medium
**Dependencies:** None

### 11. Fix Type Casting
**Current:** `as unknown as StorageQueryResult` (line 153)
**Problem:** Suggests type mismatch being forced
**Solution:** Investigate and fix proper types
**Complexity:** Low
**Dependencies:** None

## Low Priority - Future Features

### 12. Add Virtual Index Support
**Current:** Only routes based on shard key
**Problem:** Secondary indexes will need different routing
**Solution:** Extend `determineShardTargets()` to consult topology.indexes
**Complexity:** High
**Dependencies:** Requires #3 or #6, topology schema changes

### 13. Support Batch Inserts
**Current:** Only handles single-row inserts (line 331)
**Problem:** Common use case not supported
**Solution:** Loop through rows, group by target shard, batch execute
**Complexity:** Medium
**Dependencies:** Works well with #2 (parallel execution)

### 14. Add Query Plan Caching
**Current:** Parse and analyze AST on every query
**Problem:** Repeated work for parameterized queries
**Solution:** Cache parsed plans keyed by query template
**Complexity:** Medium
**Dependencies:** None

### 15. Implement Result Streaming
**Current:** Load all results into memory before returning
**Problem:** Large result sets can exhaust memory
**Solution:** Stream results as they arrive from shards
**Complexity:** High
**Dependencies:** Major API change

## Low Priority - Developer Experience

### 16. Add Query Metrics/Tracing
**Current:** No visibility into query performance
**Problem:** Can't debug slow queries or shard imbalance
**Solution:** Add timing, shard counts, cache hit rates
**Complexity:** Low
**Dependencies:** Works well with #6 and #7 (cleaner injection points)

### 17. Add Query Plan Explain
**Current:** No way to see which shards will be queried
**Problem:** Hard to debug routing decisions
**Solution:** Add `conductor.explain(sql)` method
**Complexity:** Low
**Dependencies:** Works well with #6 (QueryRouter)

---

## Recommended Order

**Phase 1 - Quick Wins:**
1. #2 - Parallelize Shard Queries (big perf win, low effort)
2. #3 - Extract Shard Selection Logic (reduce duplication)
3. #9 - Improve Hash Function (future-proof)
4. #11 - Fix Type Casting (technical debt)

**Phase 2 - Architecture:**
5. #1 - Add Topology Caching (major perf improvement)
6. #4 - Refactor Monolithic sql() Method
7. #5 - Fix Parameter Tracking (correctness bug)

**Phase 3 - Scaling:**
8. #6 - Create QueryRouter Class
9. #7 - Create ShardExecutor Class
10. #8 - Abstract Shard Location (enables rebalancing)

**Phase 4 - Features:**
11. #12 - Virtual Index Support
12. #13 - Batch Inserts
13. #16 - Query Metrics

---

## Instructions

Review this list and reply with the numbers you want implemented (e.g., "do 1, 2, 3, and 5").
