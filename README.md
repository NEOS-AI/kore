# Kore

A low-latency, high-performance **Redis/Valkey-compatible** cache and data server written in Rust (version **0.6.0**).

Kore speaks the RESP protocol so common Redis clients (`redis-cli`, redis-py, redis-rs, ioredis, …) work against it. Beyond a sharded in-memory keyspace it includes persistence, replication, cluster/Sentinel-lite, full-text/vector search, and Kore-specific locking (Redlock, fair queues, deadlock detection).

## Features

### Core engine
- **Sharded keyspace** — default 4096 shards; unified map for all Redis types (`KeyValue` / `KeySlot`)
- **RESP2 + RESP3** — `HELLO 2|3`; maps/bools/push where applicable
- **Memory & eviction** — `maxmemory` + Redis-style policies (`allkeys-lru/lfu`, `volatile-*`, `noeviction`); active expire sampling
- **Multi-DB** — `SELECT` / `SWAPDB` / `MOVE` / `COPY` (`--databases`, default 16)
- **Pipelines & Unix sockets** — reply coalescing; optional `--unixsocket`

### Data types & commands
- **Strings, hashes, lists, sets, sorted sets, streams** (incl. consumer groups / blocking ops)
- **Bitmaps / HyperLogLog**, **geospatial**, transactions (`MULTI`/`EXEC`/`WATCH`)
- **Lua** — `EVAL` / `EVALSHA` / `SCRIPT *` (Functions library still stub — see roadmap)
- **DUMP / RESTORE** — Redis RDB wire for string/list/set/hash/zset; geo as ZSET_2 geohash; stream type-15+KST1; legacy KDF1 still accepted

### Persistence & HA
- **RDB** (Kore `KORDB`, incl. search/HNSW graph) — `SAVE` / `BGSAVE` / timed `--save`
- **AOF** — append + `BGREWRITEAOF`; load prefers AOF when `--appendonly`
- **Replication** — `REPLICAOF`, `SYNC`/`PSYNC`, backlog, `WAIT`, min-replicas write gate
- **Failover** — coordinated `FAILOVER TO`; **Sentinel-lite** (MONITOR, ODOWN quorum, hello bus lite)
- **Cluster** — hash slots, MOVED/ASK, gossip, reshard/`MIGRATE`, NODE prepare/commit 2PC (RESP; not binary bus)

### Security & ops
- **ACL** users/categories + `ACL LOG`; boot `--aclfile`
- **TLS** — `--tls` / cert / key; dual `--tls-port`; mTLS (`--tls-auth-clients` + `--tls-ca`); replica link (`--tls-replication`)
- **Metrics** — Prometheus text on `--metrics-port`; `HEALTH` / richer `INFO`
- **Slowlog**, structured JSON logs (`--log-format json`)

### Kore differentiators
- **Redlock** multi-instance locks, **fair FIFO** queueing, **deadlock detection** (+ optional web UI)
- **FT search** — TEXT / NUMERIC / TAG / VECTOR; **HNSW** ANN with durable RDB/AOF graph

### Status & planning
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Detailed backlog: [TODO.md](TODO.md) → *Next work queue (post-GB)* (perf GC+, MIGRATE wire, TLS depth, …)
- High-level plan: [docs/roadmap.md](docs/roadmap.md)
- Locking / load rules for contributors: [docs/locking.md](docs/locking.md)

## Architecture

- **Sharded Hashmap**: Default 4096 shards to minimize lock contention
- **Unified keyspace**: strings and typed containers share one map; slot-level TTL
- **Approximated LRU/LFU eviction**: Redis-style sampling (default 5 candidates)
- **Background sweeping**: active expire sampling + optional full `SWEEP`
- **Atomic stats**: hits, misses, evictions, and more via lock-free counters

## Building

```bash
cargo build --release
```

CI (GitHub Actions) runs `cargo build --all-targets` and `cargo test --all-targets -- --test-threads=1` on every push/PR to `main`.

## Benchmarks

See [docs/benchmarks.md](docs/benchmarks.md) for a reproducible methodology comparing Kore to Redis/Valkey with `redis-benchmark` (persistence disabled). Pipelined SET improved again in Batch GC (~+19% vs FI on M3 Pro via standalone stream skip); residual gap vs Valkey absolute throughput remains.

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
- `-v, --verbosity <LEVEL>`: Verbosity level 0–3 (default: **1 = WARN**). Mapping: `0` → ERROR, `1` → WARN, `2` → INFO, `3` → DEBUG
- `--log-format <text|json>`: Log output format — human-readable `text` (default) or structured JSON lines (`json`) for log aggregators. **Boot-only** (not changeable via `CONFIG SET`). JSON includes `target` fields for aggregator indexing; text keeps targets off for quieter consoles. Optional `RUST_LOG` / `EnvFilter` overrides the verbosity floor when set (e.g. `RUST_LOG=kore=debug`)
- `--databases <N>`: Number of logical databases (default: 16)
- `--dir` / `--dbfilename` / `--appendonly` / `--appendfilename` / `--save`: persistence paths and policies
- `--replicaof <host> <port>`: start as replica; `--cluster-enabled` for cluster mode
- `--tls` / `--tls-cert` / `--tls-key`: enable TLS on the client port
- `--unixsocket <path>`: bind a Unix domain socket in addition to TCP
- `--metrics-port <port>`: Prometheus text endpoint (0 = off); binds `127.0.0.1`
- `--enable-redlock` / `--redlock-instances` / fair-queue and deadlock UI flags: see [docs/redlock.md](docs/redlock.md)

For the full flag surface run `kore --help`.

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

### Distributed Lock Operations
- `SETNX key value` - SET if Not eXists (returns 1 if set, 0 if key exists)
- `GETDEL key` - GET and DELete atomically
- `GETEX key [EX seconds] [PX milliseconds] [EXAT timestamp] [PXAT timestamp] [PERSIST]` - GET with expiration options

**NEW: Redlock Algorithm Support**
Kore now supports the Redlock algorithm for distributed locking across multiple instances:
- Quorum-based lock acquisition (N/2 + 1)
- Automatic retry with exponential backoff
- Clock drift compensation
- Fault-tolerant design
- **Automatic deadlock detection** (NEW!)
- **Fair lock queueing** (NEW!) - FIFO ordering to prevent starvation

See [Redlock Documentation](docs/redlock.md), [Deadlock Detection](docs/deadlock_detection.md) for detailed usage.

### Redlock Configuration
- `--enable-redlock` - Enable Redlock distributed locking
- `--redlock-instances <ADDRS>` - Comma-separated instance addresses
- `--redlock-retry-count <COUNT>` - Number of retry attempts (default: 3)
- `--redlock-retry-delay-ms <MS>` - Delay between retries (default: 200)

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

### Pub/Sub Operations (Message Broadcasting)

Kore supports Redis-style Pub/Sub for real-time message broadcasting between clients:

- `PUBLISH channel message` - Publish a message to a channel
- `SUBSCRIBE channel [channel ...]` - Subscribe to one or more channels
- `UNSUBSCRIBE [channel ...]` - Unsubscribe from channels
- `PSUBSCRIBE pattern [pattern ...]` - Subscribe to channels matching patterns
- `PUNSUBSCRIBE [pattern ...]` - Unsubscribe from patterns
- `PUBSUB CHANNELS [pattern]` - List active channels
- `PUBSUB NUMSUB [channel ...]` - Get subscriber count per channel
- `PUBSUB NUMPAT` - Get number of pattern subscriptions

**Pattern Matching:**
- `*` - Matches any characters
- `?` - Matches single character
- `[...]` - Character class matching
- `\x` - Escape character

**Implementation Details:**
- Thread-safe with RwLock for concurrent access
- Broadcast channels for efficient message distribution
- Automatic client cleanup on disconnect
- Pattern matching with glob-style syntax
- No message persistence (memory-based)

**Example - Real-time Notifications:**
```bash
# Client 1 - Subscribe to notifications
SUBSCRIBE notifications
1) "subscribe"
2) "notifications"
3) (integer) 1

# Client 2 - Publish a message
PUBLISH notifications "New user registered!"
(integer) 1  # Number of subscribers that received the message

# Client 1 receives:
1) "message"
2) "notifications"
3) "New user registered!"

# Pattern subscription
PSUBSCRIBE news.*

# Publish to matching channels
PUBLISH news.tech "AI breakthrough"
PUBLISH news.sports "Team wins championship"

# Client receives both messages:
1) "pmessage"
2) "news.*"
3) "news.tech"
4) "AI breakthrough"

# Get statistics
PUBSUB CHANNELS
1) "notifications"
2) "news.tech"
3) "news.sports"

PUBSUB NUMSUB notifications
1) "notifications"
2) (integer) 3

PUBSUB NUMPAT
(integer) 5
```

**Use Cases:**
- Real-time notifications and alerts
- Chat applications and messaging
- Event broadcasting and fan-out
- Log aggregation and monitoring
- Microservice communication

**Documentation:**
- [Pub/Sub Guide](docs/pubsub.md)

### Search Operations (Full-Text & Vector Search)

Kore supports Redis-like search capabilities including full-text search and vector similarity search:

**Index Management:**
- `FT.CREATE index [PREFIX count prefix ...] SCHEMA field_name field_type [options ...]` - Create a search index
- `FT.DROPINDEX index` - Drop a search index
- `FT._LIST` - List all search indices
- `FT.INFO index` - Get index information

**Search:**
- `FT.SEARCH index query [LIMIT offset count] [RETURN count field ...]` - Search for documents

**Supported Field Types:**
- **TEXT** - Full-text search with tokenization
  - Options: `WEIGHT weight` (default: 1.0), `SORTABLE`
- **NUMERIC** - Numeric range queries
  - Options: `SORTABLE`
- **TAG** - Exact matching with tags
  - Options: `SEPARATOR separator` (default: ","), `SORTABLE`
- **VECTOR** - Vector similarity search (KNN/ANN)
  - Algorithms: `FLAT` (brute-force), `HNSW` (approximate)
  - Distance metrics: `COSINE`, `L2`, `IP` (inner product)
  - Options: `DIM dimensions`, `DISTANCE_METRIC metric`

**Implementation Details:**
- Inverted index for full-text search
- Range indices for numeric queries
- Tag indices for exact matching
- Vector indices with multiple similarity metrics
- Flat (brute-force) and HNSW algorithms for vector search
- Thread-safe with RwLock for concurrent access

**Example - Full-Text Search:**
```bash
# Create an index for articles
FT.CREATE articles PREFIX 1 article: SCHEMA title TEXT WEIGHT 2.0 body TEXT

# Index a document (programmatically)
# cache.index_document("articles", "article:1", {
#   "title": "Introduction to Rust",
#   "body": "Rust is a systems programming language..."
# })

# Search for articles
FT.SEARCH articles "rust programming" LIMIT 0 10

# Get index info
FT.INFO articles

# List all indices
FT._LIST

# Drop an index
FT.DROPINDEX articles
```

**Example - Vector Search:**
```bash
# Create a vector index for embeddings
FT.CREATE embeddings SCHEMA
  text TEXT
  embedding VECTOR FLAT TYPE FLOAT32 DIM 128 DISTANCE_METRIC COSINE

# Index documents with vectors (programmatically)
# cache.index_document("embeddings", "doc1", {
#   "text": "document content",
#   "embedding": [0.1, 0.2, 0.3, ...]
# })

# Search for similar documents (programmatically via QueryFilter::Vector)
```

**Example - Numeric Range Search:**
```bash
# Create an index with numeric fields
FT.CREATE products SCHEMA name TEXT price NUMERIC SORTABLE

# Index products (programmatically)
# cache.index_document("products", "prod1", {
#   "name": "Laptop",
#   "price": 999.99
# })

# Search with numeric filters (programmatically via QueryFilter::NumericRange)
```

**Use Cases:**
- Full-text search for articles, documents, logs
- Vector similarity search for semantic search, recommendations
- Product search with filters and ranges
- Real-time search and indexing
- Multi-field search and ranking

### Distributed Lock Pattern

Kore supports both basic and advanced (Redlock) distributed locking patterns.

#### Basic Mode (Single Instance)

```bash
# Acquire a lock with automatic expiration (prevents deadlock)
SET mylock "unique-request-id" NX EX 10
OK

# Alternative: Use SETNX (returns 1 if acquired, 0 if failed)
SETNX mylock "unique-request-id"
(integer) 1

# Set expiration after acquiring lock
EXPIRE mylock 10
(integer) 1

# Check lock ownership before releasing
GET mylock
"unique-request-id"

# Release lock atomically
GETDEL mylock
"unique-request-id"

# Renew lock TTL while holding it
GETEX mylock EX 10
"unique-request-id"

# Remove expiration from a lock (make it permanent)
GETEX mylock PERSIST
"unique-request-id"
```

#### Redlock Mode (Multi-Instance)

```rust
use kore::{Cache, Redlock};
use bytes::Bytes;
use std::sync::Arc;

// Create multiple cache instances
let cache1 = Arc::new(Cache::new(CacheConfig::default()));
let cache2 = Arc::new(Cache::new(CacheConfig::default()));
let cache3 = Arc::new(Cache::new(CacheConfig::default()));

// Create Redlock (quorum = 2 for 3 instances)
let redlock = Redlock::new(vec![cache1, cache2, cache3])?
    .with_fair_queueing(100)  // Enable fair FIFO ordering
    .with_deadlock_detection(5000, false);  // Enable deadlock detection

// Acquire a distributed lock
let lock = redlock.lock("my-resource", Bytes::from("unique-id"), 10000)?;

// Or acquire with priority (0 = highest priority)
let lock = redlock.lock_with_priority("my-resource", Bytes::from("unique-id"), 10000, 0)?;

// Check queue position
if let Some(pos) = redlock.get_queue_position("my-resource", &client_id) {
    println!("Position in queue: {}", pos);
}

// Extend lock if needed
lock.extend(5000)?;

// Lock is automatically released when dropped
```

**Best Practices for Distributed Locks:**
- Always use a unique identifier (e.g., UUID) as the lock value
- Always set an expiration (TTL) to prevent deadlocks
- Verify ownership before releasing (check value matches)
- Use `GETDEL` for atomic lock release
- Use `GETEX` to renew locks during long operations
- **Use Redlock for multi-instance deployments** for stronger guarantees
- **Enable fair queueing** to prevent lock starvation
- **Enable deadlock detection** for automatic deadlock prevention

**Documentation:**
- [Distributed Locks Guide](docs/distributed_locks.md)
- [Redlock Implementation](docs/redlock.md)
- [Deadlock Detection](docs/deadlock_detection.md)
- [Locking guidelines](docs/locking.md) (contributor lock orders / load commit)

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
kore_version:0.6.0

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
kore_version:0.6.0

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
