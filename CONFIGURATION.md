# Configuration

Complete reference for all configurable parameters.

Configuration is supplied through environment variables. No configuration file
is used. Two parameters may additionally be modified at runtime through the
`CONFIG SET` command.

Defaults are defined in `DefaultConfig()` in `internal/engine/engine.go`. An
unset or invalid variable leaves the corresponding default in effect; invalid
values are not treated as zero values.

## 1. Memory and eviction

| Variable | Default | Accepted values | Runtime equivalent |
|---|---|---|---|
| `MAX_MEMORY_MB` | `32` | Positive integer | `CONFIG SET maxmemory` |
| `MAXMEMORY_POLICY` | `allkeys-lru` | See section 1.2 | `CONFIG SET maxmemory-policy` |

### 1.1 Memory limit

`MAX_MEMORY_MB` defines the threshold at which eviction is triggered. The bound
is approximate: eviction reduces usage below the limit before a write is
applied, and the write is then added, so usage may exceed the limit by
approximately the size of one value.

A value of `0` disables the limit. The process will then grow until constrained
by the operating system, which is appropriate only on a dedicated host.

Accounting is estimated from key length, value length, and a fixed per-entry
overhead. Precise heap measurement would require runtime introspection on every
write, at a cost disproportionate to the accuracy required by an eviction
trigger.

### 1.2 Eviction policies

| Policy | Eligible keys | Selection criterion |
|---|---|---|
| `noeviction` | None | Writes are refused with `OOM` |
| `allkeys-lru` | All | Greatest idle time within the sample |
| `allkeys-lfu` | All | Lowest access frequency, after time decay |
| `allkeys-random` | All | First key of a random sample |
| `volatile-lru` | Keys with a TTL | Greatest idle time |
| `volatile-ttl` | Keys with a TTL | Nearest expiry deadline |
| `volatile-random` | Keys with a TTL | First key of a random sample |

The volatile variants interpret the presence of a TTL as an indication that a
key is disposable. Keys without a TTL are never selected. When no key carries a
TTL, these policies report `OOM` rather than evicting data that was not marked
as temporary.

`noeviction` is appropriate when the server holds authoritative data. All other
policies assume that key loss is acceptable.

## 2. Store

| Variable | Default | Accepted values |
|---|---|---|
| `STORE` | `sharded` | `sharded`, `global`, `syncmap` |
| `SHARD_COUNT` | `256` | A power of two |

Three store implementations are provided so that identical workloads may be
benchmarked against each without code modification. `sharded` is the intended
production configuration; `global` and `syncmap` exist as comparison baselines.
Measured results are recorded in [BENCHMARKS.md](BENCHMARKS.md).

`SHARD_COUNT` must be a power of two, as the shard index is computed by
bitmask. Values that are not powers of two are rejected and the default is
retained. Measured returns diminish beyond 64 shards; the default of 256 sits
past that point at negligible additional cost.

## 3. Persistence

| Variable | Default | Accepted values |
|---|---|---|
| `AOF_ENABLED` | `false` | The literal string `true` enables it |
| `AOF_PATH` | `redisgo.aof` | Any writable path |
| `AOF_SYNC` | `everysec` | `always`, `everysec`, `no` |
| `SNAPSHOT_PATH` | `redisgo.rdb` | Any writable path |

`AOF_ENABLED` recognizes only the exact string `true`. Other values, including
`1` and `yes`, leave persistence disabled.

### 3.1 Synchronization policies

Writing to a file transfers data to the operating system's page cache. Until
`fsync` completes, that data is lost if the machine fails, notwithstanding that
the write returned successfully.

| Policy | Data lost on machine failure | Cost per write |
|---|---|---|
| `always` | None | One disk round trip |
| `everysec` | Up to one second of writes | One background fsync per second |
| `no` | Whatever the operating system had buffered | None |

`everysec` is the default and matches the Redis default. `always` is warranted
only where the cache holds authoritative data.

### 3.2 Snapshots

Snapshots are written on clean shutdown and in response to `SAVE` or `BGSAVE`.
No automatic interval is configured.

Both persistence paths must be writable; parent directories are created if
absent.

On hosts with an ephemeral filesystem, including free-tier platform instances,
neither file survives a restart and persistence has no practical effect.

## 4. Network

| Variable | Default | Notes |
|---|---|---|
| `TCP_ADDR` | `:6379` | Cache listener address |
| `PORT` | Unset | Supplied by the hosting platform; presence selects cloud mode |

### 4.1 Operating modes

The presence of `PORT` selects the operating mode:

| `PORT` | Mode | Listeners |
|---|---|---|
| Unset | Local | RESP over TCP on `TCP_ADDR`, playground on `:8080` |
| Set | Cloud | Playground on `$PORT` only |

Cloud mode accommodates platforms that expose exactly one port per service.
`PORT` must not be set manually on such a platform; the platform assigns it,
and overriding the assignment prevents the router from reaching the service.

`TCP_ADDR` accepts a full listen address. `:6380` selects an alternative port;
`127.0.0.1:6379` restricts the listener to loopback. The variable has no effect
in cloud mode.

## 5. Runtime configuration

Two parameters may be modified without restarting the server:

```
CONFIG GET maxmemory
CONFIG GET maxmemory-policy
CONFIG GET *

CONFIG SET maxmemory 64mb
CONFIG SET maxmemory-policy allkeys-lfu
```

`CONFIG SET maxmemory` accepts a byte count or a suffixed value using `kb`,
`mb`, or `gb`.

Both parameters are stored as atomic values and read on every write operation,
which permits modification while the server is serving traffic.

`CONFIG SET` is rejected within browser playground sessions, as it would alter
behaviour for all connected clients. `CONFIG GET` remains available.

## 6. Playground session limits

Session limits are not environment-configurable. They are defined in
`DefaultSessionLimits()` in `internal/engine/session.go`.

| Limit | Default | Purpose |
|---|---|---|
| Keys per session | 500 | Prevents a single client from filling the instance |
| Maximum value size | 64 KB | Prevents large single allocations |
| Commands per second | 50 | Throttles sustained load |
| Burst allowance | 100 | Permits pasted command blocks without throttling |
| Idle session lifetime | 15 minutes | Reclaims memory from abandoned connections |

Rate limiting uses a token bucket rather than a leaky bucket, so that short
bursts are permitted while sustained excess is constrained.

The following commands are rejected within a session, as each affects the
server as a whole rather than the caller's keyspace: `SAVE`, `BGSAVE`,
`BGREWRITEAOF`, `LASTSAVE`, and `CONFIG SET`.

## 7. Example configurations

Development, with persistence enabled:

```
AOF_ENABLED=true go run ./cmd/server
```

Demonstrating eviction:

```
MAX_MEMORY_MB=1 MAXMEMORY_POLICY=allkeys-lru go run ./cmd/server
```

Comparing store implementations:

```
STORE=global go run ./cmd/server
STORE=syncmap go run ./cmd/server
SHARD_COUNT=16 go run ./cmd/server
```

Maximum durability, at reduced throughput:

```
AOF_ENABLED=true AOF_SYNC=always MAXMEMORY_POLICY=noeviction go run ./cmd/server
```

Two instances with separate persistence files:

```
TCP_ADDR=:6379 AOF_PATH=a.aof SNAPSHOT_PATH=a.rdb go run ./cmd/server
TCP_ADDR=:6380 AOF_PATH=b.aof SNAPSHOT_PATH=b.rdb go run ./cmd/server
```

Note that the second instance will fail to bind the HTTP listener, as the
playground address is not environment-configurable. Running two complete
instances on one host requires adding a variable for that address.

## 8. Inspecting the active configuration

The `INFO` command reports the running configuration and operational counters:
version, uptime, store implementation, memory in use, configured limit, active
policy, command and connection counts, expired and evicted key totals, and the
current key count.

The `/api/stats` endpoint returns equivalent data in JSON, with the addition of
the active browser session count.
