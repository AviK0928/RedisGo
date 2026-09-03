# Design notes

Why this is built the way it is, including the places it deliberately differs
from real Redis and the mistakes that shaped it.

## One engine, two front doors

The engine knows nothing about sockets. It takes a `[]string` of arguments and
returns a `resp.Value`. The TCP listener and the WebSocket bridge both call the
same dispatcher.

This exists because a Render free web service exposes exactly one port, and it
must be the one named in `$PORT`. Locally the server runs raw RESP on 6379 and
the playground on 8080; in the cloud it runs the playground only. The mode
switch is three lines in `main.go` and everything below it is unaware.

The side effect turned out to matter more than the reason. Because both paths
share a dispatcher, the browser terminal and `redis-cli` exercise identical
code. The demo is not a mock of the server; it is the server.

## The store interface takes callbacks

`View` and `Update` run a caller-supplied function while holding the lock,
rather than returning an `*Entry` for the caller to use afterwards.

The first version did the obvious thing and returned the entry. Handlers then
mutated it after the lock had been released:

```go
entry, ok := store.Get(key)      // read lock taken and released
entry.Str = newValue             // mutation, unprotected
```

That loses updates. Two clients running `INCR` on the same key can both read 5
and both write 6. The race detector never flagged it, because the concurrency
test gave each goroutine its own key and nothing ever contended.

The fix was structural rather than a patch: make it impossible to hold a
reference to an entry outside the lock. `Update` receives the current entry and
returns the one to store, all inside the write lock, so read-modify-write is
atomic by construction. `TestConcurrentIncrementIsAtomic` now runs 10,000
increments across 50 goroutines and asserts the total, which catches lost
updates that `-race` alone would not.

The general lesson: a race detector finds unsynchronized access, not incorrect
synchronization. A test that never contends proves nothing about contention.

## Sharding, and where it stops working

The default store is 256 independently locked maps, with the shard chosen by
FNV-1a over the key. Two clients touching unrelated keys almost never contend;
with one lock they always do.

A shard is itself a `GlobalStore`, which is what makes the benchmark comparison
honest. The two implementations differ only in how many locks exist, not in
data structure or code path.

Shard count is a power of two so the index is a bitmask rather than a modulo.
FNV-1a rather than a cryptographic hash because speed matters more than
distribution quality here, and it spreads prefix-sharing keys (`user:1`,
`user:2`) rather than clustering them.

The limit is real: `BenchmarkHotKey` shows all three implementations converging
when every goroutine hits one key, because one key means one shard and 255
locks sit idle. Sharding distributes contention across keys, not within one.

`Len()` walks every shard and takes each read lock in turn, so it is not a
snapshot of a single instant. Acceptable for a stats counter, and would not be
for anything requiring consistency.

## Expiry needs both mechanisms

**Lazy**: every read checks the deadline and deletes the key if it has passed.
Cheap, and the cost falls on the operation that would have returned stale data
anyway.

Lazy alone leaks. A key with a TTL that nobody ever reads again occupies memory
forever, because nothing ever looks at it. A cache full of one-off session keys
would grow without bound.

**Active**: a background cycle on a 100ms tick samples 20 keys that carry TTLs,
deletes the expired ones, and repeats immediately if more than 25% of the
sample was dead. Go randomizes map iteration order, so the random sample is
free.

It samples rather than scans because a full sweep of a large keyspace would
block every other operation for its duration. The tradeoff is that expiry is
probabilistic: a key is removed soon after it dies, not at the instant. Redis
made the same choice for the same reason, and the cycle is bounded to 16 rounds
so one tick cannot run away.

## Approximated LRU, not true LRU

True LRU requires a linked list threaded through every entry, plus a list
update on every read. Two pointers per key of memory, and a write-lock's worth
of work on what should be a read.

Instead, eviction samples five random keys and evicts the worst one by the
active policy. That gets close to true LRU's hit rate for none of the memory,
and it is what Redis does.

Recency tracking still costs something. `touch` runs on every read and writes
an access timestamp plus a probabilistic LFU counter bump. The counter is
eight bits with logarithmic growth, so a key accessed ten times and one
accessed ten million times remain distinguishable; a plain counter would
saturate immediately. The counter decays with idle time, without which a key
that was hot last week would outrank one that is hot now, forever.

Those fields are atomics because they are written during reads, which hold only
a read lock. Making every `GET` take a write lock to record that it happened
would defeat the purpose.

## maxmemory is a target, not a ceiling

Eviction runs *before* a write and frees memory until usage is under the limit.
The write then lands on top. So usage can exceed `maxmemory` by roughly the
size of one value.

The test initially asserted a hard ceiling and failed by 287 bytes on a 1 MB
limit. The right fix was to correct the assertion, not the code: Redis has the
same property, which is why its documentation describes `maxmemory` as
approximate and advises leaving headroom.

Evicting to a low-water mark, say 95% of the limit, would tighten it. That
costs 5% of usable memory permanently and still cannot guarantee a ceiling for
a large value, so it was not worth it.

Entry sizes are approximate too: key length, value length, and a fixed
per-entry overhead. Measuring real Go heap usage per object would need
reflection or runtime introspection on every write, which would cost more than
the accuracy is worth for an eviction trigger.

## Two persistence mechanisms, because they fail differently

**The AOF** records how the data got here: every write command, in RESP form,
appended to a file. That means the log is readable with `cat`, replayable by
feeding it to a socket, and needs no separate serializer.

Three fsync policies, because durability is a spectrum and the distinction is
easy to miss. Writing to a file only hands bytes to the page cache; until
`fsync`, a machine crash loses them even though the process believed the write
succeeded. `always` acknowledges nothing until it is on disk. `everysec` loses
at most a second. `no` leaves it to the operating system.

**Snapshots** record only what the data is now: a length-prefixed binary dump.
Far smaller, and far faster to load, since recovering a million keys means
reading a million records rather than replaying every command that ever touched
them. The cost is everything written since the last dump.

A custom binary format rather than `encoding/gob`, so the layout is explicit and
a corrupt file fails loudly instead of decoding into something plausible. The
file starts with a magic number, and the `Kind` constants are duplicated in the
snapshot package rather than imported: renumbering the engine's constants must
not silently change what an existing file means.

Snapshots are written to a temp file and atomically renamed, with an `fsync`
before the rename. Without that sync the rename could land while the contents
are still only in the page cache, leaving a correctly named but empty snapshot
after a crash.

### They load in a specific order

The snapshot loads first, then the AOF replays on top. The snapshot is the
older, coarser record; the log holds whatever happened after it. Loading them
the other way round would let stale snapshot values overwrite newer logged ones.

### A truncated AOF is recoverable; a truncated snapshot is not

If the process died mid-write, the tail of the log is a partial command. Replay
stops there and the server starts with everything before it. Refusing to start
because of a half-written command would turn a survivable crash into data loss.

A snapshot gets the opposite treatment. Half a snapshot is half a database, and
loading it silently would look like data loss with nothing to explain it. So
truncation is a hard error.

### Rewrite emits absolute expiry times

Log compaction walks the live keyspace and emits the shortest command sequence
that reproduces it. A key incremented a million times occupies a million
records; afterwards it occupies one `SET`. This is not cosmetic: replay time is
proportional to log length, so an uncompacted log makes startup slower every
day the server runs.

Expiry is emitted as `PEXPIREAT` with an absolute timestamp rather than
`EXPIRE` with a duration. A duration would restart the clock on every rewrite
and every restart, so a key with a one-hour TTL would live forever across
frequent restarts. This was a genuine bug in the first version and would have
stayed invisible until someone noticed keys that never expired.

### BGSAVE is not a consistent point in time

Real Redis forks, so the child writes a frozen copy of memory while the parent
keeps serving. Go cannot fork usefully, so `BGSAVE` here walks the live
keyspace instead. Keys written during the walk may or may not appear in the
result.

For a cache that is an acceptable trade. For a database it would not be, and
this is the clearest place where the implementation is bounded by its host
language rather than by effort.

## Recovery goes through eviction

`EnableAOF` replays by calling `Execute`, which means replayed writes are
subject to the memory limit like any other. If the log holds more data than
`maxmemory` allows, recovery evicts as it loads and the server comes back with
less than was saved. Real Redis behaves the same way. Worth knowing before
sizing a limit below a working set.

## Pipelining needed both sides

Pipelining mostly falls out of correct buffered reads, but the first benchmark
showed only 1.8x, which was suspiciously low.

The cause was `resp.Writer.Write` flushing after every value. The "pipelined"
client was issuing one syscall per command and batching only its reads, so half
the point was missing. Splitting encode from flush (`WriteNoFlush` plus an
explicit `Flush`) and having the server hold replies while `Reader.Buffered()`
reports more commands pending took it to 12.5x at depth 100.

The server-side condition matters: replies are held only while more commands
are already parsed and waiting. An unflushed reply with nothing following it
would hang the client, so the optimization applies exactly when a batch
genuinely arrived together.

## The playground is namespaced, not sandboxed

Every visitor's keys are silently prefixed with a session id. Keys are rewritten
on the way in and stripped on the way out, so from the browser it looks like a
private server. `KEYS`, `DBSIZE` and `FLUSHALL` are answered from the session's
own view rather than passed through, so one visitor never sees the server's
true size or wipes anyone else's data.

Making that work generically required key specifications on every command:
where the key arguments are, in the style of `COMMAND INFO`. A dispatcher
cannot otherwise know that `GET`'s key is at position 1 while `MSET`'s are at
1, 3, 5. Getting `KeyStep` wrong on `MSET` would namespace values as though
they were keys.

Session ids come from `crypto/rand`, because a guessable id would let one
visitor reconnect as another and read their keys.

This is isolation between cooperating visitors, not a security boundary. A
determined attacker sharing a process with other sessions is not a threat model
this handles, and it does not need to be: the data is whatever someone typed
into a demo thirty seconds ago.

### Limits, because an open cache server is a free memory allocator

Per session: 500 keys, 64 KB values, 50 commands per second with a burst of
100. Server-wide commands (`SAVE`, `BGSAVE`, `BGREWRITEAOF`, `CONFIG SET`) are
refused; `CONFIG GET` is allowed because reading configuration is harmless.

The rate limiter is a token bucket rather than a leaky bucket, so someone
pasting a block of commands is not throttled for it, while sustained abuse
still is.

Abandoned sessions are reaped after 15 minutes. A visitor who closes the tab is
gone but their keys are not, and without a reaper the demo would slowly fill
with the leavings of everyone who ever visited. The reaper closes sessions
outside the manager lock, since closing walks the keyspace and holding the lock
through it would block every new connection meanwhile.

## The bug worth remembering

`ParsePolicy("")` returned `NoEviction, true`. Treating the empty string as a
valid parse meant that `ConfigFromEnv`, layering an unset environment variable
over the defaults, silently overwrote `allkeys-lru` with `noeviction`.

The symptom was a configuration value that looked deliberate but was not, and
it only surfaced because the demo refused writes instead of evicting. Nothing
errored; the server simply had a policy nobody chose.

"Unset" and "explicitly set to the zero value" have to be distinguishable. Any
`ParseX` that accepts the empty string will clobber defaults in exactly this
pattern.

## Known rough edges

**TCP shutdown is abrupt.** HTTP connections drain through `srv.Shutdown`, but
the TCP listener stops on context cancellation without waiting for in-flight
commands. Persistence still happens after the drain, so nothing is lost from
the log, but a connected client sees a close rather than a clean finish.

**`Session.countKeys` walks the keyspace.** It runs on every write to check the
key limit, and on every reply to update the browser's counter. That is O(n) per
command, acceptable only because the demo's keyspace is small and capped. A
running per-session count would be the fix.

**Allocation in the hot path.** `GET` allocates three times for what is
conceptually a map lookup: constructing the reply value, and `strings.ToLower`
on the command name for dispatch. A case-folded lookup that does not allocate
and a reply type that borrows rather than copies would both help. Measured but
not addressed.
