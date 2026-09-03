# redisgo

An in-memory cache server that speaks the Redis wire protocol, written in Go
with the standard library.

![ci](https://github.com/AviK0928/RedisGo/actions/workflows/ci.yml/badge.svg)

**Live demo:** _coming with the first deploy_

> Status: phase 0 of 9. The build, test, and deploy pipeline works end to end.
> The cache itself is being built on top of it, one subsystem at a time.
> Progress below.

## Why

Writing a cache from the socket upward is the most direct way to work through
protocol design, concurrent data structures, key expiry, eviction under memory
pressure, and durability, all in one system. The target is a server that real
Redis clients can talk to without knowing the difference.

No third-party dependencies so far. Everything is the Go standard library.

## Running it

Requires Go 1.22 or later.

```bash
go run ./cmd/server
```

Two listeners start:

| Address | What |
|---|---|
| `:6379` | the cache, over TCP |
| `:8080` | a stats page in the browser |

Drive it with the bundled client, in a second terminal:

```bash
go run ./cmd/cli
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> ECHO hello
hello
```

Once phase 1 lands, `redis-cli -p 6379` connects here too.

## Tests

```bash
go test ./...          # fast
go test -race ./...    # what CI runs
go vet ./...
gofmt -l .             # should print nothing
```

## Architecture
