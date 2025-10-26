# Testing Guide

This project uses Vitest with the Cloudflare Workers pool for testing Durable Objects.

## Setup

The test environment is configured with:
- **Vitest**: Modern test framework
- **@cloudflare/vitest-pool-workers**: Cloudflare Workers integration for Vitest
- **cloudflare:test**: Runtime helpers for writing tests

## Running Tests

```bash
# Run tests in watch mode
npm test

# Run tests once
npm run test:run
```

## Test Structure

Tests are colocated with the source code:
- `src/Storage/Storage.test.ts` - Tests for Storage Durable Object
- `src/Topology/Topology.test.ts` - Tests for Topology Durable Object

## Writing Tests

### Basic Test Structure

```typescript
import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';

describe('My Durable Object', () => {
  it('should do something', async () => {
    // Get a Durable Object stub using idFromName
    const id = env.STORAGE.idFromName('unique-test-id');
    const stub = env.STORAGE.get(id);

    // Call methods on the stub
    const result = await stub.someMethod();

    // Assert expectations
    expect(result).toBe(expectedValue);
  });
});
```

### Important Concepts

#### Test Isolation
Each test should use a unique Durable Object ID to ensure isolation:
```typescript
const id = env.STORAGE.idFromName('test-storage-1');  // Unique ID
```

This ensures tests don't interfere with each other.

#### Accessing Durable Objects
The `env` object from `cloudflare:test` provides access to your Durable Object bindings:
- `env.STORAGE` - Storage Durable Object namespace
- `env.TOPOLOGY` - Topology Durable Object namespace

#### Async/Await
All Durable Object method calls are asynchronous and return promises:
```typescript
const result = await stub.executeQuery(query);
```

## Test Coverage

### Storage Durable Object Tests
- ✅ Simple query execution
- ✅ Table creation and data insertion
- ✅ Batch query execution
- ✅ Database size retrieval
- ✅ UPDATE operations
- ✅ DELETE operations

### Topology Durable Object Tests
- ✅ Empty topology initialization
- ✅ Adding storage nodes
- ✅ Updating storage node status
- ✅ Adding and querying tables
- ✅ Adding table shards
- ✅ Removing storage nodes
- ✅ Updating table configuration

## Configuration

### vitest.config.ts
```typescript
import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        wrangler: {
          configPath: './wrangler.jsonc',
        },
      },
    },
  },
});
```

This configuration:
- Uses the Workers pool for running tests
- Points to `wrangler.jsonc` for Durable Object configuration
- Automatically provides the test environment with access to bindings

## Best Practices

1. **Use descriptive test IDs**: Make test IDs unique and descriptive
   ```typescript
   const id = env.STORAGE.idFromName('test-batch-queries-complex');
   ```

2. **Test one thing at a time**: Each test should focus on a single behavior

3. **Setup in test**: Don't rely on external setup - each test should be self-contained

4. **Clean assertions**: Use appropriate matchers
   ```typescript
   expect(result).toHaveProperty('rows');
   expect(result.rows).toHaveLength(1);
   expect(result.rows[0]).toEqual({ id: 1, name: 'John' });
   ```

5. **Handle types properly**: The Durable Object stubs are properly typed, but results may need casting
   ```typescript
   const result = await stub.executeQuery(query);
   expect((result as any).rows).toHaveLength(1);
   ```

## Troubleshooting

### Tests not finding Durable Objects
- Verify `wrangler.jsonc` is correctly configured
- Ensure Durable Objects are properly exported from `src/index.ts`
- Check that migrations are set up correctly

### Type errors with `env`
- Make sure `cloudflare:test` is imported
- Verify TypeScript can find the worker configuration types

### Tests timing out
- Increase timeout in vitest.config.ts if needed
- Check if Durable Object methods are hanging

## Resources

- [Cloudflare Workers Vitest Integration](https://developers.cloudflare.com/workers/testing/vitest-integration/)
- [Vitest Documentation](https://vitest.dev/)
- [Durable Objects Documentation](https://developers.cloudflare.com/workers/runtime-apis/durable-objects/)
