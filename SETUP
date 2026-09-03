# Setup

Instructions for building, running, and testing redisgo from source.

## 1. Prerequisites

| Requirement | Version | Purpose |
|---|---|---|
| Go | 1.22 or later | Required for generics, `log/slog`, and `math/rand/v2` |
| Git | Any | Repository access |

No additional tooling is required. The project has no Docker, Node, or database
dependencies, and no build step beyond `go build`.

Verify the installed versions:

```
go version
git --version
```

Go distributions are available at https://go.dev/dl/. On Debian-derived
systems, the official tarball is preferred over `apt install golang`, which
frequently packages an outdated release.

### 1.1 Optional tooling

| Tool | Purpose | Installation |
|---|---|---|
| `redis-cli` | Verifying protocol compatibility with an unmodified client | `apt-get install redis-tools`, or `brew install redis` |
| `benchstat` | Statistical summaries of benchmark output | `go install golang.org/x/perf/cmd/benchstat@latest` |

Neither is required. The repository includes a client at `cmd/cli` that serves
the same purpose as `redis-cli` during development.

## 2. Building

```
git clone https://github.com/AviK0928/RedisGo.git
cd RedisGo
go build ./...
```

The initial build resolves two dependencies and typically completes in under a
minute. Subsequent builds complete in seconds.

If module resolution times out, configure the proxy explicitly and retry:

```
go env -w GOPROXY=https://proxy.golang.org,direct
```

## 3. Running the server

```
go run ./cmd/server
```

Startup emits two log lines confirming both listeners:

```
17:03:53 redisgo 0.7.0 starting in local mode, http on :8080
17:03:53 tcp listening on :6379
```

| Address | Service |
|---|---|
| `:6379` | Cache server, RESP over TCP |
| `:8080` | Browser playground and metrics endpoint |

The playground is available at http://localhost:8080.

Configuration is supplied through environment variables. See
[CONFIGURATION.md](CONFIGURATION.md) for the complete reference.

## 4. Connecting a client

### 4.1 Bundled client

```
go run ./cmd/cli
```

```
127.0.0.1:6379> SET greeting hello
OK
127.0.0.1:6379> GET greeting
"hello"
127.0.0.1:6379> QUIT
```

An alternative address may be supplied with `-addr host:port`.

### 4.2 Standard redis-cli

Connecting with an unmodified Redis client verifies wire compatibility rather
than assuming it:

```
redis-cli -p 6379
```

All commands listed in [COMMANDS.md](COMMANDS.md) are supported.

## 5. Testing

| Command | Purpose | Approximate duration |
|---|---|---|
| `go test ./...` | Unit and integration tests | 3 seconds |
| `go test -race -count=1 ./...` | Full suite under the race detector | 30 seconds |
| `go vet ./...` | Static analysis | 2 seconds |
| `gofmt -l .` | Formatting check; produces no output when clean | Immediate |

The second command is the one executed by continuous integration. The
`-count=1` flag disables result caching and should be used after any code
change.

### 5.1 Benchmarks

```
go test -bench=. -benchmem ./internal/engine/
go test -bench=BenchmarkPipeline ./internal/server/
```

Two constraints apply:

1. Benchmarks must not be run with `-race`. The detector's overhead invalidates
   the measurements.
2. Results obtained on consumer laptops are unreliable, particularly on
   thermally constrained processors. [BENCHMARKS.md](BENCHMARKS.md) documents
   the observed variance and the methodology adopted in response. Published
   figures are produced by the `bench` job in continuous integration.

### 5.2 Fuzzing

The RESP decoder processes untrusted input on every connection and is fuzz
tested:

```
go test -fuzz=FuzzRead -fuzztime=60s ./internal/resp/
```

Parse errors on malformed input are the expected outcome. A panic constitutes a
failure, as it would represent a remotely triggerable denial of service.

## 6. Exercising the implementation

The following procedures demonstrate the behaviours that distinguish this
implementation from a hash map behind a socket. Each uses deliberately small
limits so that the relevant condition is reached quickly.

### 6.1 Eviction under memory pressure

Start the server with a 1 MB limit:

```
MAX_MEMORY_MB=1 go run ./cmd/server
```

Write approximately 2.8 MB into it:

```
PAYLOAD=$(printf 'a%.0s' {1..500})
for i in $(seq 1 5000); do echo "SET key$i $PAYLOAD"; done | go run ./cmd/cli > /dev/null
```

Inspect the result with `INFO`. Memory usage remains at approximately the
configured limit, `evicted_keys` reports several thousand, and roughly one
third of the written keys remain resident.

To observe the alternative policy, change it at runtime and repeat:

```
127.0.0.1:6379> CONFIG SET maxmemory-policy noeviction
```

Under `noeviction` the same workload terminates in `OOM command not allowed
when used memory > 'maxmemory'`, the key count remains fixed, and reads
continue to be served.

### 6.2 Active expiry

```
127.0.0.1:6379> SET token abc EX 5
127.0.0.1:6379> DBSIZE
```

After six seconds, `DBSIZE` returns 0 without any intervening read. Removal is
performed by the background expiry cycle rather than by a check on access.

### 6.3 Persistence and recovery

Start the server with the append-only log enabled:

```
AOF_ENABLED=true go run ./cmd/server
```

Write several keys, then inspect the log. It is stored in RESP format and is
therefore readable directly:

```
cat redisgo.aof
```

Terminate the server and restart it with the same configuration. Startup logs
the number of commands replayed, and the keyspace is restored.

### 6.4 Log compaction

Execute `INCR counter` thirty times and record the log size:

```
wc -c redisgo.aof
```

Trigger compaction and record it again:

```
127.0.0.1:6379> BGREWRITEAOF
```

The thirty `INCR` records are replaced by a single `SET` expressing the same
final state.

### 6.5 Session isolation

Open http://localhost:8080 in two browser windows. Execute `SET x 1` in the
first and `GET x` in the second; the second returns `(nil)`. Each connection
receives a namespaced keyspace.

## 7. Repository layout

```
cmd/server         Server binary and the local/cloud mode switch
cmd/cli            Redis client, removing the redis-tools dependency for development
internal/resp      Wire protocol: reader, writer, fuzz-tested decoder
internal/engine    Cache core: store implementations, commands, eviction, sessions
internal/aof       Append-only log and replay
internal/snapshot  Binary point-in-time dump format
internal/server    TCP front door
web                HTTP front door and embedded playground
```

Suggested entry points by area of interest:

| Subject | File |
|---|---|
| Protocol parsing and serialization | `internal/resp/resp.go` |
| Command dispatch | `internal/engine/engine.go`, method `Execute` |
| Command implementations | `internal/engine/commands.go` |
| Concurrency and sharding | `internal/engine/store_sharded.go` |
| Eviction policies | `internal/engine/evict.go` |
| Persistence and recovery | `internal/engine/persist.go` |

[ARCHITECTURE.md](ARCHITECTURE.md) documents the component structure and call
flows. [DESIGN.md](DESIGN.md) records the rationale for each significant
decision.

## 8. Generated files

With persistence enabled, the server writes the following to its working
directory:

| File | Contents |
|---|---|
| `redisgo.aof` | Append-only command log |
| `redisgo.rdb` | Keyspace snapshot |
| `redisgo.aof.rewrite` | Temporary, present during compaction |
| `redisgo.rdb.tmp` | Temporary, present during snapshot creation |

All are excluded by `.gitignore`. Removing them resets the server to an empty
keyspace on the next start.

## 9. Deployment

The server compiles to a single binary with the frontend embedded via
`go:embed`. No container image, frontend build, or external service is
required.

When the `PORT` environment variable is present, the server starts the HTTP
listener only, on the specified port. This accommodates platforms that expose
exactly one port per service.

### 9.1 Reference deployment

The published demo runs on a Render free instance, configured as follows:

| Setting | Value |
|---|---|
| Language | Go |
| Build command | `go build -o server ./cmd/server` |
| Start command | `./server` |
| Health check path | `/healthz` |
| Environment | `MAX_MEMORY_MB=32` |

`PORT` is supplied by the platform and must not be set manually.

Free instances provide an ephemeral filesystem. Neither the append-only log nor
the snapshot survives a redeploy or an idle shutdown, so persistence is not
functional in that environment. A host with durable storage is required for
persistence to be meaningful.

## 10. Troubleshooting

| Symptom | Cause and resolution |
|---|---|
| `bind: address already in use` | Another process holds port 6379 or 8080. Identify it with `lsof -i :6379` (Unix) or `netstat -ano \| findstr :6379` (Windows), or start the server on a different port with `TCP_ADDR=:6380`. |
| `redis-cli` connects but displays no prompt | The client issues `COMMAND DOCS` on connect and blocks until it receives a reply. Confirm the `COMMAND` handler is registered. Single-shot invocations such as `redis-cli -p 6379 ping` bypass the handshake. |
| Browser terminal fails to connect | Inspect the browser console. Pages served over HTTPS require `wss://`, which the frontend derives from `location.protocol`. Proxies that do not forward WebSocket upgrade requests will prevent the connection. |
| Tests pass normally but fail under `-race` | The failure is genuine. The detector reports unsynchronized access that ordinary execution does not surface. |
| `no required module provides package` after adding an import | Run `go get <module>`. Note that `go mod tidy` removes dependencies that are not currently imported; running it before writing the code that uses a dependency will discard that dependency. |
