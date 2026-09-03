# Benchmarks

## How these were produced

Store benchmarks ran on **GitHub Actions runners: Intel Xeon Platinum 8573C,
4 cores, Go 1.22, Linux**, with `-benchtime=3s -count=5 -cpu=4`, summarized
with `benchstat`.

They did not run on a laptop, and that was a deliberate decision. The first
attempt used an i5-8250U, a 15W ultrabook part that boosts to 3.4 GHz and then
throttles as it heats. Two back-to-back runs of identical code disagreed by up
to 3x, and the ordering of results changed between them: whichever case
happened to run while the chip was cool looked fastest. Any conclusion drawn
from that data would have been noise. CI runners have stable clocks and anyone
can re-run the workflow, which makes the numbers reproducible rather than
anecdotal.

Reproduce with: Actions → `ci` → Run workflow. The `bench` job uploads both the
raw output and the benchstat summary.

The pipelining benchmark is the exception and ran locally over loopback, noted
where it appears.

## Store implementations

Three implementations behind one interface, so the same workload runs against
all of them without touching code. `keyspace` is 10,000 keys; a small keyspace
is what creates contention, which is the thing being measured.

Nanoseconds per operation, lower is better.

### 90% reads, 10% writes

| goroutines | sharded (256) | global mutex | sync.Map |
|---:|---:|---:|---:|
| 1 | 284.2 | 615.6 | 345.0 |
| 8 | 276.7 | 672.8 | 330.7 |
| 64 | 268.4 | 741.8 | 319.4 |
| 512 | **271.7** | 778.9 | 308.1 |

### 50% reads, 50% writes

| goroutines | sharded (256) | global mutex | sync.Map |
|---:|---:|---:|---:|
| 1 | 340.2 | 677.0 | 784.7 |
| 8 | 311.7 | 704.6 | 752.3 |
| 64 | 311.9 | 748.6 | 741.3 |
| 512 | **310.6** | 842.0 | 864.0 |

### What the shape says

**Sharding is worth 2.9x at high concurrency**, but the ratio is the less
interesting half of the result. The important part is the trend: the sharded
store is flat from 1 to 512 goroutines (284ns → 272ns, a 4% *improvement* as
scheduling overhead amortizes), while the single-mutex store degrades by 27%
over the same range. One implementation scales and the other does not.

That is exactly what the design predicts. With one lock, two clients contend
regardless of whether their keys have anything to do with each other. With 256
locks, keys hashed to different shards never meet.

**`sync.Map` splits the two workloads sharply.** It is competitive on the
read-heavy mix, where its lock-free read path does what it was built for. On
the write-heavy mix it is the slowest option at low concurrency, 2.3x behind
sharded. `sync.Map` cannot express an atomic read-modify-write, which a cache
needs for `INCR` and `SET NX`, so writes here still take a mutex. The lock-free
reads are the only saving, and a 50/50 workload halves that benefit while
adding `sync.Map`'s own overhead on top.

## Where sharding stops helping

Every goroutine hammering one key. 64 goroutines, `INCR` on a single key.

| implementation | ns/op |
|---|---:|
| sharded | 581.7 |
| global mutex | 668.8 |
| sync.Map | 595.7 |

All three converge, and the sharded store loses most of its advantage. One key
hashes to one shard, so 255 of the 256 locks sit idle while everyone queues for
the same one. Sharding distributes contention across *keys*; it does nothing
for contention on a *key*.

Worth stating plainly because it bounds the claim. A workload with a genuine
hot key needs a different technique, and the honest answer to "how much does
sharding help" is "depends entirely on your key distribution."

## How many shards

90/10 workload, 64 goroutines.

| shards | ns/op | vs. 1 shard |
|---:|---:|---:|
| 1 | 772.2 | — |
| 4 | 423.4 | 1.82x |
| 16 | 321.0 | 2.41x |
| 64 | 284.8 | 2.71x |
| 256 | 274.8 | 2.81x |
| 1024 | 273.0 | 2.83x |

The knee is around 64. Going from 1 to 16 shards captures 2.4x of the eventual
2.8x; the last 4x increase in shard count buys 0.7%.

256 is the configured default: past the knee, cheap enough in memory that the
margin costs nothing, and a power of two so the shard index is a bitmask rather
than a modulo.

## Per-command cost

Single goroutine, no contention, so this isolates dispatch and allocation from
locking.

| command | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| PING | 73.9 | 8 | 1 |
| GET | 215.7 | 104 | 3 |
| SET | 287.3 | 216 | 4 |
| INCR | 329.3 | 128 | 5 |
| HGETALL | 340.1 | 232 | 4 |
| LRANGE | 364.1 | 320 | 5 |

`PING` is the floor: dispatch, arity check, and one allocation for the reply.
Everything above that is the cost of touching the keyspace.

Allocation is the obvious target for future work. `GET` allocates three times
for what is conceptually a map lookup, mostly in constructing the reply value
and in `strings.ToLower` on the command name. A dispatcher keyed on a
case-folded lookup without allocating, and a reply type that borrows rather than
copies, would move these numbers.

## Pipelining

**This one ran locally** (i5-8250U, Windows, loopback), so treat it as
indicative rather than precise. The absolute numbers are noisy; the ratios held
across runs.

`PING` over TCP, varying how many commands are in flight before reading replies.

| depth | ns/op | vs. sequential |
|---:|---:|---:|
| 1 (sequential) | 21,435 | — |
| 10 | 4,495 | 4.8x |
| 100 | 1,719 | **12.5x** |

The cost of a command over a socket is dominated by syscalls, not by the work
the server does: a `GET` costs 216ns in-process and 21µs over a sequential
connection.

Both sides have to batch for this to work. The first attempt showed only 1.8x
because `resp.Writer.Flush` was called after every value, so the "pipelined"
client was still issuing one syscall per command and batching only its reads.
Splitting encode from flush, and having the server hold replies while
`Reader.Buffered()` reports more commands pending, produced the numbers above.

Note that most of the benefit arrives by depth 10. Deeper batches buy
progressively less while holding more unflushed data in memory.

Over a real network the sequential case would be far worse, since propagation
delay adds to every round trip and pipelining amortizes that too. Loopback
measures only the syscall saving, which makes 12.5x a floor rather than a
ceiling.

## Eviction under pressure

Not a microbenchmark; a behavioural check that the memory limit holds.

Wrote 5,000 keys of roughly 570 bytes each, about 2.8 MB, into a server
configured with a 1 MB limit and `allkeys-lru`:

```
used_memory:1048916
maxmemory:1048576
evicted_keys:3163
db0:keys=1837
```

1,837 keys survived, 3,163 were evicted, and memory settled 340 bytes over the
limit — one entry's worth.

That overshoot is by design and matches Redis. Eviction runs *before* a write
and frees memory until usage is under the limit; the write then lands on top.
So `maxmemory` is a target the server converges on rather than an invariant it
maintains. Redis's own documentation calls it approximate for the same reason.

The same workload under `noeviction` returned `OOM command not allowed when
used memory > 'maxmemory'` once full, with the key count frozen and reads still
served. Same limit, same data, opposite behaviour, which is the entire point of
having the policy.

## Fuzzing

The RESP decoder faces untrusted bytes on every connection, so it is fuzzed
rather than only unit tested:

```
go test -fuzz=FuzzRead ./internal/resp/
```

A 60-second run executed **3.8 million inputs** and found 74 distinct coverage
paths with no crashes. Errors are expected and fine; panics are not, since a
panic in the decoder is a remote denial of service.

## Caveats

**The store numbers predate the eviction work.** They were taken when the store
was a keyspace with expiry. Approximated LRU later added a `touch` on every
read, which records an access timestamp and probabilistically bumps an LFU
counter, including a `rand.Float64` call. Reads are therefore slower now than
the `GET` figure above suggests. Re-running the sweep and publishing both sets
would show what recency tracking actually costs, and that is the next thing
worth measuring.

**A microbenchmark is not a server.** These measure the store and the command
dispatcher in-process. They exclude the socket, the protocol decode, and the
scheduler behaviour of thousands of real connections. The pipelining numbers
are the only ones here that cross a socket at all.

**Nothing here is compared against real Redis.** Redis is single-threaded C
with a decade of optimization; this is a portfolio project. The comparisons
that are meaningful are the ones above: this implementation against alternative
versions of itself, holding everything else fixed.
