# Command reference

Specification of all implemented commands. Behaviour conforms to Redis unless
a deviation is stated explicitly.

Arity follows the Redis convention: a positive value specifies an exact
argument count including the command name; a negative value specifies a
minimum.

## 1. Connection

| Command | Arity | Reply |
|---|---|---|
| `PING [message]` | -1 | `PONG`, or the supplied message |
| `ECHO message` | 2 | The supplied message |
| `QUIT` | — | `OK`, followed by connection closure |

`QUIT` is handled by the TCP layer rather than the engine, as it is the only
command whose effect is on the connection rather than the keyspace.

`COMMAND` is registered as a stub returning an empty array. `redis-cli` issues
`COMMAND DOCS` upon connection and blocks until a reply is received; without
this handler the client appears unresponsive.

## 2. Strings

| Command | Arity | Reply |
|---|---|---|
| `SET key value [EX s] [PX ms] [NX\|XX]` | -3 | `OK`, or nil if a condition was not met |
| `GET key` | 2 | The value, or nil |
| `SETNX key value` | 3 | 1 if set, 0 if the key existed |
| `SETEX key seconds value` | 4 | `OK` |
| `APPEND key value` | 3 | Resulting length |
| `STRLEN key` | 2 | Length, or 0 if absent |
| `INCR key` | 2 | Resulting value |
| `DECR key` | 2 | Resulting value |
| `INCRBY key delta` | 3 | Resulting value |
| `DECRBY key delta` | 3 | Resulting value |
| `MGET key [key ...]` | -2 | Array of values, nil per absent key |
| `MSET key value [key value ...]` | -3 | `OK` |

### 2.1 SET options

| Option | Effect |
|---|---|
| `EX seconds` | Sets an expiry in seconds; must be positive |
| `PX milliseconds` | Sets an expiry in milliseconds; must be positive |
| `NX` | Applies the write only if the key is absent |
| `XX` | Applies the write only if the key exists |

Specifying both `NX` and `XX` is a syntax error. A condition that is not met
produces a nil reply rather than an error.

The existence check and the write are performed within a single locked
callback. `SET NX` is commonly used as a distributed locking primitive, so the
two operations must be atomic; performing the check before acquiring the write
lock would permit two clients to each conclude that they had acquired the lock.

### 2.2 Counters

Counters are stored as strings and parsed on each operation, consistent with
Redis. A value written by `SET` may therefore be incremented. A value that does
not parse as a 64-bit integer produces `ERR value is not an integer or out of
range`.

`INCR`, `DECR`, `INCRBY`, and `DECRBY` perform read-modify-write within a
single write lock, so concurrent increments on a shared key cannot lose
updates.

### 2.3 MGET type handling

`MGET` returns nil at the position of a key holding a non-string type, rather
than returning an error for the entire command. This matches Redis.

## 3. Keys

| Command | Arity | Reply |
|---|---|---|
| `DEL key [key ...]` | -2 | Count of keys deleted |
| `EXISTS key [key ...]` | -2 | Count of keys present |
| `TYPE key` | 2 | `string`, `list`, `hash`, or `none` |
| `KEYS pattern` | 2 | Array of key names |
| `DBSIZE` | 1 | Current key count |
| `FLUSHALL` | -1 | `OK` |

`KEYS` accepts only the pattern `*`. Other patterns produce an error. Full glob
matching is properly paired with `SCAN`, which is not implemented; `KEYS`
blocks the server for the duration of a full keyspace traversal and is not
suitable for large keyspaces in any implementation.

Within browser playground sessions, `KEYS`, `DBSIZE`, and `FLUSHALL` operate on
the caller's namespace rather than the global keyspace.

## 4. Expiry

| Command | Arity | Reply |
|---|---|---|
| `EXPIRE key seconds` | 3 | 1 if applied, 0 if the key is absent |
| `PEXPIRE key milliseconds` | 3 | 1 if applied, 0 if absent |
| `PEXPIREAT key ms-timestamp` | 3 | 1 if applied, 0 if absent |
| `TTL key` | 2 | Remaining seconds, or a sentinel |
| `PTTL key` | 2 | Remaining milliseconds, or a sentinel |
| `PERSIST key` | 2 | 1 if an expiry was removed, 0 otherwise |

### 4.1 TTL sentinel values

The following return values are part of the protocol specification and do not
indicate errors:

| Value | Meaning |
|---|---|
| `-2` | The key does not exist |
| `-1` | The key exists and has no associated expiry |
| `n` | `n` units remain |

Remaining time is truncated rather than rounded; a key with 9.7 seconds
remaining reports `9`.

### 4.2 Non-positive expiry values

An expiry of zero or less deletes the key immediately, consistent with Redis,
rather than producing an error.

### 4.3 PEXPIREAT

`PEXPIREAT` sets an absolute deadline expressed in milliseconds since the Unix
epoch. It exists principally to support AOF compaction: emitting `EXPIRE` with
a relative duration would reset the countdown at every rewrite and restart,
allowing keys to outlive their intended lifetimes indefinitely.

## 5. Lists

| Command | Arity | Reply |
|---|---|---|
| `LPUSH key value [value ...]` | -3 | Resulting length |
| `RPUSH key value [value ...]` | -3 | Resulting length |
| `LPOP key` | 2 | The element, or nil |
| `RPOP key` | 2 | The element, or nil |
| `LLEN key` | 2 | Length, or 0 if absent |
| `LRANGE key start stop` | 4 | Array of elements |

`LRANGE` accepts negative indices counting from the end of the list, so
`LRANGE key 0 -1` returns all elements. Indices outside the list bounds are
clamped rather than rejected.

A list key is deleted when its final element is removed, consistent with Redis.
An empty list and an absent key are therefore indistinguishable.

## 6. Hashes

| Command | Arity | Reply |
|---|---|---|
| `HSET key field value [field value ...]` | -4 | Count of fields newly created |
| `HGET key field` | 3 | The value, or nil |
| `HDEL key field [field ...]` | -3 | Count of fields deleted |
| `HLEN key` | 2 | Field count, or 0 if absent |
| `HEXISTS key field` | 3 | 1 or 0 |
| `HGETALL key` | 2 | Flat array alternating field and value |

`HSET` counts only fields that did not previously exist. Overwriting an
existing field returns 0 although the write is applied.

`HGETALL` returns fields in map iteration order, which Go randomizes. Redis
provides no ordering guarantee for this command, so client code must not depend
on field order.

A hash key is deleted when its final field is removed.

## 7. Server

| Command | Arity | Reply |
|---|---|---|
| `INFO [section]` | -1 | Multi-line text block |
| `CONFIG GET parameter` | -3 | Flat array of parameter name and value |
| `CONFIG SET parameter value` | -3 | `OK` |
| `MEMORY USAGE key` | -2 | Estimated size in bytes, or nil |
| `MEMORY DOCTOR` | -2 | Diagnostic summary |
| `OBJECT FREQ key` | 3 | Decayed access frequency counter |
| `OBJECT IDLETIME key` | 3 | Seconds since last access |
| `OBJECT ENCODING key` | 3 | The value type |

`INFO` emits Server, Memory, Stats, and Keyspace sections in the plain-text
format used by Redis.

`CONFIG` supports the `maxmemory` and `maxmemory-policy` parameters.
`maxmemory` accepts a byte count or a value suffixed with `kb`, `mb`, or `gb`.
See [CONFIGURATION.md](CONFIGURATION.md).

`MEMORY USAGE` returns an estimate derived from key length, value length, and a
fixed per-entry overhead. Precise heap measurement would require runtime
introspection on every write, at a cost disproportionate to the accuracy
required by an eviction trigger.

`OBJECT FREQ` and `OBJECT IDLETIME` expose eviction metadata and are the only
external means of inspecting it. `FREQ` returns the counter after time-based
decay has been applied, so a key that was frequently accessed in the past
reports a lower value than it did at that time.

## 8. Persistence

| Command | Arity | Reply |
|---|---|---|
| `SAVE` | 1 | `OK` on completion |
| `BGSAVE` | 1 | `Background saving started` |
| `BGREWRITEAOF` | 1 | `Background append only file rewriting started` |
| `LASTSAVE` | 1 | Unix timestamp of the last successful save |

`SAVE` blocks. It acquires each shard's read lock in sequence, so writes to a
shard under traversal are delayed. This is acceptable for an explicit
administrative operation.

`BGSAVE` executes on a separate goroutine but does **not** produce a consistent
point-in-time image. Redis forks, allowing a child process to serialize a
frozen copy of memory while the parent continues serving. Go does not support
fork for this purpose, so the implementation traverses the live keyspace; keys
written during traversal may or may not appear in the result. This is
acceptable for a cache and would not be for a database.

`BGREWRITEAOF` replaces the log with the minimal command sequence that
reproduces the current keyspace. It is also triggered automatically when the
log reaches twice its post-compaction size.

Both background operations decline to start while an operation of the same type
is in progress.

All four commands are rejected within browser playground sessions.

## 9. Error responses

| Error | Condition |
|---|---|
| `ERR unknown command 'X'` | The command is not present in the registry |
| `ERR wrong number of arguments for 'x' command` | Arity validation failed |
| `WRONGTYPE Operation against a key holding the wrong kind of value` | The key holds a type the command does not accept |
| `ERR value is not an integer or out of range` | A numeric argument or stored value could not be parsed |
| `ERR syntax error` | Malformed options, such as `SET` with both `NX` and `XX` |
| `ERR invalid expire time in 'x' command` | An expiry of zero or less where a positive value is required |
| `OOM command not allowed when used memory > 'maxmemory'` | Memory limit reached under the `noeviction` policy |

Client libraries match on the `WRONGTYPE` prefix specifically, making the exact
text of that response part of the protocol contract rather than a
human-readable message.

Errors are a first-class RESP type and do not indicate transport failure. A
command returning an error has completed a normal request-response cycle. Such
commands are excluded from the append-only log, preventing replay from
reconstructing a state the server never held.

## 10. Unimplemented commands

Sets, sorted sets, `SCAN`, publish and subscribe, transactions with `MULTI` and
`WATCH`, and replication are not implemented.

The latter three require per-connection state rather than keyspace state, which
represents a distinct class of problem from the functionality documented here.
These were excluded from scope rather than left incomplete; the implementation
is complete as a single-node cache.
