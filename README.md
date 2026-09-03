# redisgo

An in-memory cache server that speaks the Redis wire protocol, written in Go
with the standard library.

![ci](https://github.com/AviK0928/RedisGo/actions/workflows/ci.yml/badge.svg)

**Live demo:** _coming with the first deploy_

> Status: phase 0 of 9. The build, test, and deploy pipeline works end to end.
> The cache itself is being built on top of it, one subsystem at a time.
> Progress is tracked below.

## Why

Writing a cache from the socket upward is the most direct way to work through
protocol design, concurrent data structures, key expiry, eviction under memory
pressure, and durability, all in one system. The target is a server that real
Redis clients can talk to without knowing the difference.

No third-party dependencies so far. Everything is the Go standard library.

## Running it

Requires Go 1.22 or later.

```
go run ./cmd/server
```

Two listeners start:

| Address | What |
|---|---|
| `:6379` | the cache, over TCP |
| `:8080` | a stats page in the browser |

Drive it with the bundled client, in a second terminal:

```
go run ./cmd/cli
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> ECHO hello
hello
```

Once phase 1 lands, `redis-cli -p 6379` connects here too.

## Tests

```
go test ./...          # fast
go test -race ./...    # what CI runs
go vet ./...
gofmt -l .             # should print nothing
```

## Architecture

```
cmd/server       the binary, and the local/cloud mode switch
cmd/cli          a small client, so redis-tools is not needed to develop
internal/engine  the cache core: config, counters, command execution
internal/server  the TCP front door
web              the HTTP front door and the embedded stats page
```

The engine knows nothing about transports. The TCP listener and the HTTP
handler both call the same `Execute`, which is what will let a browser terminal
and a real Redis client exercise identical code.

## Progress

- [x] Scaffold, CI, and continuous deployment
- [ ] RESP protocol, so redis-cli can connect
- [ ] Data structures and key expiry
- [ ] Sharded concurrent store, benchmarked against a single-mutex baseline
- [ ] Memory limits and eviction (LRU, LFU, TTL)
- [ ] Append-only file and snapshots
- [ ] Pub/sub, transactions, pipelining
- [ ] Leader-follower replication
- [ ] Terminal in the browser

## Deployment

One binary, deployed to a Render free web service using Render's native Go
runtime. No Dockerfile and no separate frontend build: the static files are
compiled into the binary with `go:embed`.

When `PORT` is set the server assumes it is in the cloud and starts only the
HTTP listener, because a free web service exposes exactly one port. The free
instance sleeps after 15 minutes without traffic, so the first request after an
idle period takes about a minute to answer.

## License

MIT
