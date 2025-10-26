# Big Daddy Benchmarks

Load testing suite for the Big Daddy distributed database system.

## Overview

This package provides a Cloudflare Worker that exposes HTTP endpoints for load testing Big Daddy via RPC service bindings. It includes Artillery.io configurations for various load testing scenarios.

## Architecture

```
┌─────────────────┐
│  Artillery.io   │
│  (HTTP Client)  │
└────────┬────────┘
         │ HTTP
         ▼
┌─────────────────┐
│   Benchmarks    │
│     Worker      │
└────────┬────────┘
         │ RPC Service Binding
         ▼
┌─────────────────┐
│   Big Daddy     │
│    Database     │
└─────────────────┘
```

## Setup

### 1. Install Dependencies

```bash
pnpm install
```

### 2. Start Big Daddy (in another terminal)

```bash
cd ../big-daddy
pnpm dev
```

### 3. Start Benchmarks Worker

```bash
pnpm dev
```

The worker will be available at `http://localhost:8787`

## Available Endpoints

### Setup & Management

- **GET `/`** - List all available endpoints
- **POST `/setup`** - Initialize database schema (creates tables and indexes)
- **POST `/reset`** - Drop and recreate all tables
- **GET `/stats`** - Get database statistics

### Benchmark Operations

- **POST `/insert`** - Insert a single user record
- **GET `/select-by-id?id=123`** - Select user by ID (shard key - single shard query)
- **GET `/select-by-email?email=user@example.com`** - Select user by email (indexed - optimized multi-shard)
- **GET `/select-all`** - Full table scan (all shards)
- **POST `/update`** - Update a random user
- **POST `/delete`** - Delete a random user
- **POST `/mixed-workload`** - Execute a random mix of operations

## Manual Testing

### 1. Initialize the Big Daddy cluster

```bash
# First, initialize the cluster topology (8 storage nodes)
curl -X POST http://localhost:8787/init

# Then, create the database schema
curl -X POST http://localhost:8787/setup
```

### 2. Insert some test data

```bash
# Insert 10 users
for i in {1..10}; do
  curl -X POST http://localhost:8787/insert
done
```

### 3. Test different query patterns

```bash
# Query by ID (shard key)
curl "http://localhost:8787/select-by-id?id=12345"

# Query by email (index)
curl "http://localhost:8787/select-by-email?email=user12345@example.com"

# Full scan
curl http://localhost:8787/select-all

# Get stats
curl http://localhost:8787/stats
```

## Load Testing with Artillery

### Prerequisites

Artillery should be installed via pnpm (already in package.json):

```bash
pnpm install
```

### Quick Smoke Test

Run a simple 30-second load test:

```bash
pnpm artillery run artillery-simple.yml
```

This will:
- Send 10 requests/second for 30 seconds
- Execute mixed read/write operations
- Display real-time metrics

### Full Load Test

Run comprehensive load tests with multiple phases:

```bash
pnpm artillery run artillery.yml
```

This includes:
1. **Warm-up** (30s @ 5 req/s) - Initialize connections
2. **Ramp-up** (60s @ 5→50 req/s) - Gradual load increase
3. **Sustained Load** (120s @ 50 req/s) - Steady state testing
4. **Peak Load** (60s @ 100 req/s) - Stress testing
5. **Cool-down** (30s @ 100→10 req/s) - Graceful wind-down

### Custom Load Tests

Create your own artillery configuration:

```yaml
config:
  target: "http://localhost:8787"
  phases:
    - duration: 60
      arrivalRate: 20
      name: "Custom test"

scenarios:
  - flow:
      - get:
          url: "/select-by-id?id={{ $randomNumber(1, 100000) }}"
```

## Workload Scenarios

### Read-Heavy (60% of traffic)

Simulates typical web application with mostly reads:
- 3x SELECT by ID per iteration
- 1x SELECT by email per iteration

### Write-Heavy (20% of traffic)

Tests write throughput:
- INSERT operations
- UPDATE operations

### Mixed Workload (15% of traffic)

Realistic combination:
- 50% SELECT by ID
- 20% SELECT by email
- 15% INSERT
- 10% UPDATE
- 5% DELETE

### Analytics (5% of traffic)

Heavy queries for analytics:
- Full table scans
- Aggregate queries

## Metrics & Analysis

Artillery provides comprehensive metrics:

### Request Metrics
- **RPS** (Requests per second)
- **Response time** (min, max, median, p95, p99)
- **Success rate** (2xx responses)
- **Error rate** (4xx, 5xx responses)

### Custom Metrics
- **Cache hit rate** - Tracked via response.cacheStats
- **Database duration** - Server-side query execution time
- **Correlation ID tracking** - End-to-end request tracing

### Example Output

```
Summary report @ 21:45:23(+0000)
──────────────────────────────────────────
Scenarios launched:  3000
Scenarios completed: 3000
Requests completed:  12000
Mean response/sec: 40.2
Response time (msec):
  min: 12
  max: 234
  median: 28
  p95: 89
  p99: 156
Scenario counts:
  Read-heavy workload: 1800 (60%)
  Write-heavy workload: 600 (20%)
  Mixed workload: 450 (15%)
  Analytics queries: 150 (5%)
Codes:
  200: 12000
```

## Correlation ID Tracking

All requests include correlation IDs for distributed tracing:

- **Header**: `x-correlation-id: artillery-{timestamp}-{random}`
- **Response**: Includes same correlation ID in JSON body
- **Big Daddy Logs**: Tagged with correlation ID for debugging

This enables:
- End-to-end request tracing across worker boundaries
- Performance debugging in Cloudflare dashboard
- Correlation between load test requests and database operations

## Performance Tips

### 1. Warm-up Phase
Always include a warm-up phase to:
- Initialize Durable Object instances
- Populate caches
- Establish connections

### 2. Realistic Data Distribution
Use random IDs within a realistic range:
- Too small: All data in few shards
- Too large: Mostly cache misses

### 3. Monitor Cache Performance
Track cache hit rates in responses:
```json
{
  "cacheStats": {
    "cacheHit": true,
    "totalHits": 1234,
    "totalMisses": 56
  }
}
```

### 4. Correlation ID Analysis
Filter Cloudflare logs by correlation ID to debug slow requests:
```
correlationId:"artillery-1234567890-abc123"
```

## Cloudflare Deployment

### Deploy Big Daddy

```bash
cd ../big-daddy
pnpm deploy
```

### Deploy Benchmarks

Update `wrangler.jsonc` service binding to point to deployed Big Daddy:

```jsonc
{
	"services": [
		{
			"binding": "BIG_DADDY",
			"service": "big-daddy",  // Your deployed worker name
			"environment": "production"  // Optional
		}
	]
}
```

Then deploy:

```bash
pnpm deploy
```

### Run Load Tests Against Production

Update artillery target:

```yaml
config:
  target: "https://big-daddy-benchmarks.your-subdomain.workers.dev"
```

**⚠️ Warning**: Be mindful of Cloudflare Workers limits and costs when load testing production!

## Troubleshooting

### Service Binding Not Working

Ensure both workers are running:
```bash
# Terminal 1
cd packages/big-daddy && pnpm dev

# Terminal 2
cd packages/benchmarks && pnpm dev
```

### High Error Rates

Check:
1. Database is initialized: `curl -X POST http://localhost:8787/setup`
2. Big Daddy worker is healthy
3. Artillery arrival rate isn't too aggressive

### Slow Response Times

Investigate:
1. Cache hit rates (should be >70% for read-heavy workloads)
2. Shard distribution (check if queries hit optimal shards)
3. Index usage (ensure indexes are created and ready)

## Next Steps

- Add more complex query patterns
- Test with multiple databases
- Implement realistic data distributions
- Add monitoring dashboards
- Test failure scenarios (shard failures, network issues)
