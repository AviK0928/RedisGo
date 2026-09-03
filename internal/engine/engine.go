// Package engine is the cache core. It knows nothing about sockets, so the TCP
// front door and the HTTP front door can share one implementation.
package engine

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AviK0928/RedisGo/internal/resp"
)

const Version = "0.4.0"

const (
	expiryInterval   = 100 * time.Millisecond
	expirySampleSize = 20
	expiryThreshold  = 0.25
)

// StoreKind selects a keyspace implementation. Configurable so benchmarks can
// run the same workload against all three without touching code.
type StoreKind string

const (
	StoreSharded StoreKind = "sharded"
	StoreGlobal  StoreKind = "global"
	StoreSyncMap StoreKind = "syncmap"
)

type Config struct {
	MaxMemoryMB       int
	MaxKeysPerSession int
	TCPAddr           string
	HTTPAddr          string
	Store             StoreKind
	ShardCount        int
}

func DefaultConfig() Config {
	return Config{
		MaxMemoryMB:       32,
		MaxKeysPerSession: 500,
		TCPAddr:           ":6379",
		HTTPAddr:          ":8080",
		Store:             StoreSharded,
		ShardCount:        256,
	}
}

func ConfigFromEnv() Config {
	c := DefaultConfig()
	if v, ok := envInt("MAX_MEMORY_MB"); ok {
		c.MaxMemoryMB = v
	}
	if v, ok := envInt("MAX_KEYS_PER_SESSION"); ok {
		c.MaxKeysPerSession = v
	}
	if v := os.Getenv("TCP_ADDR"); v != "" {
		c.TCPAddr = v
	}
	switch StoreKind(strings.ToLower(os.Getenv("STORE"))) {
	case StoreGlobal:
		c.Store = StoreGlobal
	case StoreSyncMap:
		c.Store = StoreSyncMap
	case StoreSharded:
		c.Store = StoreSharded
	}
	if v, ok := envInt("SHARD_COUNT"); ok && v&(v-1) == 0 {
		c.ShardCount = v
	}
	return c
}

func envInt(key string) (int, bool) {
	raw := os.Getenv(key)
	if raw == "" {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func newStore(cfg Config, clock Clock) Store {
	switch cfg.Store {
	case StoreGlobal:
		return NewGlobalStore(clock)
	case StoreSyncMap:
		return NewSyncMapStore(clock)
	default:
		return NewShardedStore(clock, cfg.ShardCount)
	}
}

type Stats struct {
	Version           string `json:"version"`
	UptimeSeconds     int64  `json:"uptime_seconds"`
	Keys              int    `json:"keys"`
	CommandsProcessed uint64 `json:"commands_processed"`
	Connections       int    `json:"connections"`
	ExpiredKeys       uint64 `json:"expired_keys"`
	MaxMemoryMB       int    `json:"max_memory_mb"`
	StoreKind         string `json:"store_kind"`
}

type Handler func(e *Engine, args []string) resp.Value

// Command carries the metadata the dispatcher needs. Arity follows Redis's
// convention: positive means exactly that many arguments including the command
// name, negative means at least that many.
type Command struct {
	Name    string
	Arity   int
	Flags   []string
	Handler Handler
}

var registry = map[string]Command{}

func register(c Command) {
	registry[strings.ToLower(c.Name)] = c
}

func init() {
	// connection
	register(Command{Name: "PING", Arity: -1, Handler: cmdPing})
	register(Command{Name: "ECHO", Arity: 2, Handler: cmdEcho})
	register(Command{Name: "COMMAND", Arity: -1, Handler: cmdCommand})

	// strings
	register(Command{Name: "SET", Arity: -3, Flags: []string{"write"}, Handler: cmdSet})
	register(Command{Name: "GET", Arity: 2, Flags: []string{"readonly"}, Handler: cmdGet})
	register(Command{Name: "SETNX", Arity: 3, Flags: []string{"write"}, Handler: cmdSetNX})
	register(Command{Name: "SETEX", Arity: 4, Flags: []string{"write"}, Handler: cmdSetEX})
	register(Command{Name: "APPEND", Arity: 3, Flags: []string{"write"}, Handler: cmdAppend})
	register(Command{Name: "STRLEN", Arity: 2, Flags: []string{"readonly"}, Handler: cmdStrlen})
	register(Command{Name: "INCR", Arity: 2, Flags: []string{"write"}, Handler: cmdIncr})
	register(Command{Name: "DECR", Arity: 2, Flags: []string{"write"}, Handler: cmdDecr})
	register(Command{Name: "INCRBY", Arity: 3, Flags: []string{"write"}, Handler: cmdIncrBy})
	register(Command{Name: "DECRBY", Arity: 3, Flags: []string{"write"}, Handler: cmdDecrBy})
	register(Command{Name: "MGET", Arity: -2, Flags: []string{"readonly"}, Handler: cmdMGet})
	register(Command{Name: "MSET", Arity: -3, Flags: []string{"write"}, Handler: cmdMSet})

	// keyspace
	register(Command{Name: "DEL", Arity: -2, Flags: []string{"write"}, Handler: cmdDel})
	register(Command{Name: "EXISTS", Arity: -2, Flags: []string{"readonly"}, Handler: cmdExists})
	register(Command{Name: "TYPE", Arity: 2, Flags: []string{"readonly"}, Handler: cmdType})
	register(Command{Name: "KEYS", Arity: 2, Flags: []string{"readonly"}, Handler: cmdKeys})
	register(Command{Name: "DBSIZE", Arity: 1, Flags: []string{"readonly"}, Handler: cmdDBSize})
	register(Command{Name: "FLUSHALL", Arity: -1, Flags: []string{"write"}, Handler: cmdFlushAll})

	// expiry
	register(Command{Name: "EXPIRE", Arity: 3, Flags: []string{"write"}, Handler: cmdExpire})
	register(Command{Name: "PEXPIRE", Arity: 3, Flags: []string{"write"}, Handler: cmdPExpire})
	register(Command{Name: "TTL", Arity: 2, Flags: []string{"readonly"}, Handler: cmdTTL})
	register(Command{Name: "PTTL", Arity: 2, Flags: []string{"readonly"}, Handler: cmdPTTL})
	register(Command{Name: "PERSIST", Arity: 2, Flags: []string{"write"}, Handler: cmdPersist})

	// lists
	register(Command{Name: "LPUSH", Arity: -3, Flags: []string{"write"}, Handler: cmdLPush})
	register(Command{Name: "RPUSH", Arity: -3, Flags: []string{"write"}, Handler: cmdRPush})
	register(Command{Name: "LPOP", Arity: 2, Flags: []string{"write"}, Handler: cmdLPop})
	register(Command{Name: "RPOP", Arity: 2, Flags: []string{"write"}, Handler: cmdRPop})
	register(Command{Name: "LLEN", Arity: 2, Flags: []string{"readonly"}, Handler: cmdLLen})
	register(Command{Name: "LRANGE", Arity: 4, Flags: []string{"readonly"}, Handler: cmdLRange})

	// hashes
	register(Command{Name: "HSET", Arity: -4, Flags: []string{"write"}, Handler: cmdHSet})
	register(Command{Name: "HGET", Arity: 3, Flags: []string{"readonly"}, Handler: cmdHGet})
	register(Command{Name: "HDEL", Arity: -3, Flags: []string{"write"}, Handler: cmdHDel})
	register(Command{Name: "HLEN", Arity: 2, Flags: []string{"readonly"}, Handler: cmdHLen})
	register(Command{Name: "HEXISTS", Arity: 3, Flags: []string{"readonly"}, Handler: cmdHExists})
	register(Command{Name: "HGETALL", Arity: 2, Flags: []string{"readonly"}, Handler: cmdHGetAll})
}

type Engine struct {
	cfg       Config
	clock     Clock
	store     Store
	startedAt time.Time

	mu       sync.Mutex
	commands uint64
	conns    int
	expired  uint64
}

func New(cfg Config) *Engine {
	return NewWithClock(cfg, realClock{})
}

// NewWithClock builds an engine on an injected clock, for tests.
func NewWithClock(cfg Config, clock Clock) *Engine {
	return &Engine{
		cfg:       cfg,
		clock:     clock,
		store:     newStore(cfg, clock),
		startedAt: clock.Now(),
	}
}

// StartExpiryLoop runs active expiration until the channel closes.
func (e *Engine) StartExpiryLoop(done <-chan struct{}) {
	ticker := time.NewTicker(expiryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			e.ExpireNow()
		}
	}
}

// ExpireNow runs one expiry cycle immediately. Tests use it to avoid waiting
// for the ticker.
func (e *Engine) ExpireNow() int {
	removed := e.store.ExpireCycle(expirySampleSize, expiryThreshold)
	if removed > 0 {
		e.mu.Lock()
		e.expired += uint64(removed)
		e.mu.Unlock()
	}
	return removed
}

func (e *Engine) Config() Config { return e.cfg }

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	commands, conns, expired := e.commands, e.conns, e.expired
	e.mu.Unlock()

	return Stats{
		Version:           Version,
		UptimeSeconds:     int64(e.clock.Now().Sub(e.startedAt).Seconds()),
		Keys:              e.store.Len(),
		CommandsProcessed: commands,
		Connections:       conns,
		ExpiredKeys:       expired,
		MaxMemoryMB:       e.cfg.MaxMemoryMB,
		StoreKind:         string(e.cfg.Store),
	}
}

func (e *Engine) AddConn(delta int) {
	e.mu.Lock()
	e.conns += delta
	e.mu.Unlock()
}

func (e *Engine) Execute(args []string) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR empty command")
	}

	name := strings.ToLower(args[0])
	cmd, found := registry[name]
	if !found {
		return resp.Errf("ERR unknown command '%s'", args[0])
	}
	if !arityOK(cmd.Arity, len(args)) {
		return resp.Errf("ERR wrong number of arguments for '%s' command", name)
	}

	e.mu.Lock()
	e.commands++
	e.mu.Unlock()

	return cmd.Handler(e, args)
}

func arityOK(arity, got int) bool {
	if arity >= 0 {
		return got == arity
	}
	return got >= -arity
}

// wrongType is the error real Redis returns when a command meets a key of the
// wrong kind. Clients check for this prefix, so the wording matters.
func wrongType() resp.Value {
	return resp.Err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

// viewTyped reads a key of an expected kind under the read lock. absent is the
// reply for a missing key, which differs per command: nil for GET, zero for
// STRLEN, an empty array for LRANGE.
func (e *Engine) viewTyped(key string, want Kind, absent resp.Value, fn func(*Entry) resp.Value) resp.Value {
	reply := absent
	found := e.store.View(key, func(entry *Entry) {
		if entry.Kind != want {
			reply = wrongType()
			return
		}
		reply = fn(entry)
	})
	if !found {
		return absent
	}
	return reply
}

// updateTyped runs a read-modify-write on a key of an expected kind, entirely
// under the write lock. fn returns the entry to store, or nil to delete it.
func (e *Engine) updateTyped(key string, want Kind, fn func(current *Entry) (*Entry, resp.Value)) resp.Value {
	var reply resp.Value
	e.store.Update(key, func(current *Entry) *Entry {
		if current != nil && current.Kind != want {
			reply = wrongType()
			return current
		}
		next, r := fn(current)
		reply = r
		return next
	})
	return reply
}
