# Kore

A low-latency, high-performance caching database written in Rust.

## Features

- **Low Latency**: Built with Rust and optimized data structures for minimal latency
- **High Performance**: Sharded hashmap architecture (default 4096 shards) for high concurrency
- **RESP Protocol**: Compatible with Redis/Valkey protocol
- **Memory Management**: Configurable max memory with approximated LRU eviction (Redis-style)
- **Expiration**: Per-entry TTL support with background sweeping
- **Thread-Safe**: Lock-free operations where possible, with fine-grained locking
- **Statistics**: Built-in metrics for hits, misses, evictions, and more

## Architecture

Kore implements the same core concepts as pogocache but leverages Rust's safety guarantees:

- **Sharded Hashmap**: Default 4096 shards to minimize lock contention
- **Reference Counting**: Arc-based entry management for thread-safe sharing
- **Approximated LRU Eviction**: Redis-style eviction that samples N random entries (default 5) and evicts the least recently used one
- **Background Sweeping**: Automatic cleanup of expired entries
- **Atomic Operations**: Lock-free statistics and counters

## Building

```bash
cargo build --release
```

## Running

Basic usage:

```bash
./target/release/kore
```

With custom options:

```bash
./target/release/kore \
  --host 127.0.0.1 \
  --port 6379 \
  --shards 4096 \
  --maxmemory 1073741824 \
  --threads 4 \
  -v 2
```

## Command Line Options

- `-h, --host <HOST>`: Host address to bind to (default: 127.0.0.1)
- `-p, --port <PORT>`: Port to bind to (default: 6379)
- `--threads <THREADS>`: Number of worker threads (default: number of CPU cores)
- `--shards <SHARDS>`: Number of shards for the hashmap (default: 4096)
- `--maxmemory <BYTES>`: Maximum memory in bytes (default: 80% of system memory)
- `--maxentrysize <BYTES>`: Maximum entry size in bytes (default: 524288000 = 500MB)
- `--evict <BOOL>`: Enable eviction when memory is full (default: true)
- `--autosweep <BOOL>`: Enable automatic sweeping of expired entries (default: true)
- `--loadfactor <FLOAT>`: Load factor (0.55-0.95) (default: 0.75)
- `--maxconns <COUNT>`: Maximum number of connections (default: 1024)
- `--auth <PASSWORD>`: Authentication password (default: none)
- `-v, --verbosity <LEVEL>`: Verbosity level 0-3 (default: 1)

## Supported Commands

### Basic Operations
- `PING [message]` - Test connection
- `ECHO message` - Echo back message
- `AUTH password` - Authenticate

### Key-Value Operations
- `SET key value [NX|XX] [GET] [EX seconds] [PX milliseconds] [EXAT timestamp] [PXAT timestamp] [KEEPTTL]`
- `GET key`
- `DEL key [key ...]`
- `EXISTS key [key ...]`
- `MGET key [key ...]`
- `MSET key value [key value ...]`

### Numeric Operations
- `INCR key`
- `DECR key`
- `INCRBY key delta`
- `DECRBY key delta`

### Expiration
- `EXPIRE key seconds`
- `PEXPIRE key milliseconds`
- `TTL key` - Returns TTL in seconds
- `PTTL key` - Returns TTL in milliseconds

### Database Operations
- `DBSIZE` - Get number of keys
- `KEYS pattern` - Find keys matching pattern (supports * and ?)
- `FLUSHDB` / `FLUSHALL` - Clear all keys
- `INFO` - Get server statistics
- `SWEEP` - Manually trigger expired entry sweep

### Configuration Operations
- `CONFIG GET parameter` - Get runtime configuration value
- `CONFIG SET parameter value` - Set runtime configuration value

#### Supported CONFIG Parameters
- `maxentrysize` / `max-entry-size` - Maximum entry size in bytes (minimum: 1024, cannot exceed maxmemory)

### Sorted Set Operations (Ranking System)
Kore supports sorted sets, ideal for implementing leaderboards and ranking systems:

- `ZADD key score member [score member ...]` - Add or update members with scores
- `ZRANGE key start stop [WITHSCORES]` - Get members by rank (ascending order)
- `ZREVRANGE key start stop [WITHSCORES]` - Get members by rank (descending order)
- `ZCARD key` - Get the number of members
- `ZSCORE key member` - Get the score of a member
- `ZREM key member [member ...]` - Remove members
- `ZRANK key member` - Get the rank (0-based) of a member in ascending order
- `ZREVRANK key member` - Get the rank (0-based) of a member in descending order

**Implementation Details:**
- Uses BTreeMap for maintaining score order (O(log n) operations)
- HashMap for fast member lookup (O(1) operations)
- Supports Strategy pattern for different iteration strategies (forward/reverse)
- Thread-safe with RwLock for concurrent access

**Example - Leaderboard:**
```bash
# Add players with scores
ZADD leaderboard 1500 player1 1200 player2 1800 player3

# Get top 3 players (highest scores)
ZREVRANGE leaderboard 0 2 WITHSCORES
1) "player3"
2) "1800"
3) "player1"
4) "1500"
5) "player2"
6) "1200"

# Update a player's score
ZADD leaderboard 2000 player2

# Get player's rank (0 = first place)
ZREVRANK leaderboard player2
(integer) 0

# Get player's score
ZSCORE leaderboard player2
"2000"
```

## Example Usage

Using redis-cli or any Redis-compatible client:

```bash
redis-cli -p 6379

127.0.0.1:6379> SET mykey "Hello, Kore!"
OK

127.0.0.1:6379> GET mykey
"Hello, Kore!"

127.0.0.1:6379> SET counter 0
OK

127.0.0.1:6379> INCR counter
(integer) 1

127.0.0.1:6379> INCRBY counter 10
(integer) 11

127.0.0.1:6379> CONFIG GET maxentrysize
1) "maxentrysize"
2) "524288000"

127.0.0.1:6379> CONFIG SET maxentrysize 104857600
OK

127.0.0.1:6379> INFO
# Server
kore_version:0.1.0

# Stats
...

# Memory
used_memory:...
maxmemory:...
maxentrysize:104857600
...

127.0.0.1:6379> SET tempkey "expires soon" EX 60
OK

127.0.0.1:6379> TTL tempkey
(integer) 60

127.0.0.1:6379> INFO
# Server
kore_version:0.1.0

# Stats
total_commands_processed:7
cmd_get:1
cmd_set:3
...
```

## Performance Characteristics

- **Concurrency**: High concurrency through sharding (default 4096 shards)
- **Memory**: Configurable max memory with automatic eviction
- **Latency**: Sub-microsecond operation latency for cache hits
- **Throughput**: Scales linearly with number of cores

## Implementation Details

### Data Structures

- **Entry**: Reference-counted entry with key, value, creation time, optional expiration, flags, and CAS value
- **Sharded Hashmap**: Uses Rust's `HashMap` with `ahash` for fast hashing
- **Locking**: `parking_lot::RwLock` for per-shard locking (faster than std)

### Memory Management

- Tracks total memory usage across all entries
- Eviction triggered when memory usage exceeds `maxmemory`
- **Approximated LRU eviction algorithm**: Samples N random entries (default 5), evicts the least recently used one
  - Much better than simple random eviction
  - Similar to Redis's eviction strategy
  - Low overhead (no global LRU list needed)
  - Tracks last access time per entry using atomic operations
- Background task sweeps expired entries every second

### Protocol

- RESP (REdis Serialization Protocol) parser
- Supports all RESP value types: simple strings, errors, integers, bulk strings, arrays
- Zero-copy parsing where possible
