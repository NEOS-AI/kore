# Distributed Locks in Kore

Kore provides distributed locking primitives compatible with Redis, enabling coordination of access to shared resources across multiple clients or processes.

## Supported Commands

### SETNX (SET if Not eXists)
```
SETNX key value
```
- Sets the key to hold the string value if the key does not exist
- Returns `1` if the key was set
- Returns `0` if the key was not set (already exists)
- **Use case**: Acquiring a lock

**Example:**
```bash
SETNX mylock "process-123"
(integer) 1  # Lock acquired

SETNX mylock "process-456"
(integer) 0  # Lock already held
```

### GETDEL (GET and DELete)
```
GETDEL key
```
- Gets the value of the key and deletes the key atomically
- Returns the value, or `nil` if the key does not exist
- **Use case**: Releasing a lock while verifying ownership

**Example:**
```bash
SET mylock "process-123"
OK

GETDEL mylock
"process-123"  # Returns value and deletes key

GETDEL mylock
(nil)  # Key no longer exists
```

### GETEX (GET with EXpire)
```
GETEX key [EX seconds] [PX milliseconds] [EXAT timestamp] [PXAT timestamp] [PERSIST]
```
- Gets the value of the key and optionally sets or updates its expiration
- Returns the value, or `nil` if the key does not exist
- **Use case**: Renewing lock TTL or checking lock ownership

**Options:**
- `EX seconds`: Set expiration in seconds
- `PX milliseconds`: Set expiration in milliseconds
- `EXAT timestamp`: Set expiration as Unix timestamp in seconds
- `PXAT timestamp`: Set expiration as Unix timestamp in milliseconds
- `PERSIST`: Remove the expiration

**Example:**
```bash
SET mylock "process-123" EX 10
OK

# Renew lock for another 10 seconds
GETEX mylock EX 10
"process-123"

# Remove expiration
GETEX mylock PERSIST
"process-123"
```

## Lock Patterns

### Basic Lock Acquisition with Timeout

```bash
# Acquire lock with 10 second timeout (prevents deadlock)
SET resource_lock "unique-id-12345" NX EX 10
OK

# Or use SETNX + EXPIRE
SETNX resource_lock "unique-id-12345"
(integer) 1
EXPIRE resource_lock 10
(integer) 1
```

### Lock Release with Ownership Verification

```bash
# Check if we still own the lock
GET resource_lock
"unique-id-12345"

# Release only if we own it
# (In practice, use a Lua script for atomicity)
DEL resource_lock
(integer) 1

# Or use GETDEL for atomic get-and-delete
GETDEL resource_lock
"unique-id-12345"
```

### Lock Renewal

```bash
# Extend lock while doing long operation
GETEX resource_lock EX 10
"unique-id-12345"

# Continue work...

# Extend again if needed
GETEX resource_lock EX 10
"unique-id-12345"
```

### Complete Lock Pattern Example

```bash
# 1. Try to acquire lock
SET myresource "request-id-abc123" NX EX 30
OK

# 2. Do critical section work
# ... perform operations ...

# 3. Renew lock if operation takes longer
GETEX myresource EX 30
"request-id-abc123"

# 4. Release lock when done (verify ownership first)
GET myresource
"request-id-abc123"

DEL myresource
(integer) 1
```

## Best Practices

### 1. Always Use Unique Lock Values
Use a unique identifier (UUID, process ID + timestamp, etc.) as the lock value to verify ownership:

```bash
SET lock:user:123 "uuid-v4-abcd-1234-efgh-5678" NX EX 10
```

### 2. Always Set Expiration (TTL)
Never acquire a lock without a timeout to prevent deadlocks:

```bash
# Good: Lock with timeout
SET mylock "client-1" NX EX 10

# Bad: Lock without timeout (can cause deadlock)
SETNX mylock "client-1"
```

### 3. Verify Ownership Before Release
Always check that you still own the lock before releasing it:

```bash
# Get current lock holder
GET mylock
"my-unique-id"

# Only delete if it matches
if value == "my-unique-id":
    DEL mylock
```

### 4. Use Atomic Operations
Use `GETDEL` for atomic get-and-delete operations:

```bash
# Atomic: get value and delete
GETDEL mylock
```

### 5. Handle Lock Acquisition Failures
Implement retry logic with exponential backoff:

```bash
attempts = 0
while attempts < max_attempts:
    result = SET mylock "my-id" NX EX 10
    if result == "OK":
        break
    sleep(backoff_time)
    attempts += 1
    backoff_time *= 2
```

## Limitations

### Current Implementation
- **Redlock Algorithm Support**: ✅ Now implemented for distributed multi-instance deployments
- Single-instance mode still available for simple use cases
- Lock ownership verification handled automatically by Redlock

### Recommended for:
- **Multi-instance distributed systems** (using Redlock)
- Single Kore instance deployments (basic mode)
- Low to moderate concurrency scenarios
- High-stakes critical sections requiring guaranteed mutual exclusion (with Redlock)

### Redlock Mode vs Basic Mode

#### Basic Mode (Single Instance)
- Uses SETNX, GETDEL, GETEX commands directly
- Suitable for single-instance deployments
- O(1) lock operations
- No quorum requirement

#### Redlock Mode (Multi-Instance)
- Implements the Redlock algorithm across multiple instances
- Requires majority quorum (N/2 + 1) for lock acquisition
- Handles clock drift and network delays
- Automatic retry with exponential backoff
- Safer for distributed environments

## Performance Characteristics

- **Lock Acquisition**: O(N) where N is number of instances (Redlock) or O(1) (basic)
- **Lock Release**: O(N) where N is number of instances (Redlock) or O(1) (basic)
- **Lock Renewal**: O(N) where N is number of instances (Redlock) or O(1) (basic)
- **Concurrency**: Thread-safe with sharded locking (minimal contention)

## Redlock Usage

### Programmatic API

```rust
use kore::{Cache, Redlock};
use bytes::Bytes;
use std::sync::Arc;

// Create multiple cache instances
let cache1 = Arc::new(Cache::new(CacheConfig::default()));
let cache2 = Arc::new(Cache::new(CacheConfig::default()));
let cache3 = Arc::new(Cache::new(CacheConfig::default()));

// Create Redlock with 3 instances (quorum = 2)
let redlock = Redlock::new(vec![cache1, cache2, cache3])?;

// Acquire a distributed lock
let lock_value = Bytes::from("unique-client-id");
let lock = redlock.lock("my-resource", lock_value, 10000)?;

// Perform critical section work
// ... your code here ...

// Extend lock if needed
lock.extend(5000)?;

// Lock is automatically released when dropped
// or manually release:
redlock.unlock(&lock)?;
```

### Configuration

Enable Redlock via command-line arguments:

```bash
# Start first instance
kore --port 6379 --enable-redlock

# Start second instance
kore --port 6380 --enable-redlock

# Start third instance
kore --port 6381 --enable-redlock

# Configure Redlock instances
kore --enable-redlock \
     --redlock-instances "localhost:6379,localhost:6380,localhost:6381" \
     --redlock-retry-count 3 \
     --redlock-retry-delay-ms 200
```

### Redlock Configuration Options

- `--enable-redlock`: Enable Redlock distributed locking
- `--redlock-instances`: Comma-separated list of instance addresses
- `--redlock-retry-count`: Number of retry attempts (default: 3)
- `--redlock-retry-delay-ms`: Delay between retries in milliseconds (default: 200)
