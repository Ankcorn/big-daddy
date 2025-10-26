/**
 * Sharding utility functions
 */

/**
 * Hash a value to a shard ID using a simple string hash
 *
 * @param value - The value to hash (will be converted to string)
 * @param numShards - The number of shards to distribute across
 * @returns The shard ID (0-indexed)
 */
export function hashToShard(value: any, numShards: number): number {
	// Simple hash function for MVP
	// Convert value to string and use a basic hash
	const str = String(value);
	let hash = 0;

	for (let i = 0; i < str.length; i++) {
		const char = str.charCodeAt(i);
		hash = (hash << 5) - hash + char;
		hash = hash & hash; // Convert to 32-bit integer
	}

	// Make hash positive and modulo by number of shards
	return Math.abs(hash) % numShards;
}
