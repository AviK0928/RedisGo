// Package engine is the cache core. It knows nothing about sockets, so the TCP
// front door and the HTTP front door can share one implementation.
package engine

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AviK0928/RedisGo/internal/aof"
	"github.com/AviK0928/RedisGo/internal/resp"
)

const Version = "0.6.0"

const (
	expiryInterval   = 100 * time.Millisecond
	expirySampleSize = 20
	expiryThreshold  = 0.25

	// evictSampleSize is how many keys the policy looks at per eviction.
	// Redis defaults to 5; larger samples approximate true LRU more closely
	// at proportionally more cost per eviction.
	evictSampleSize = 5

	// evictMaxRounds bounds one eviction pass, so a single write cannot stall
	// while the server frees an unbounded amount of memory.
	evictMaxRounds = 200
)

// StoreKind selects a keyspace implementation, so benchmarks can run the same
// workload against all three without code changes.
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
	Policy            Policy
	AOFEnabled        bool
	AOFPath           string
	AOFSync           aof.SyncPolicy
	SnapshotPath      string
}

func DefaultConfig() Config {
	return Config{
		MaxMemoryMB:       32,
		MaxKeysPerSession: 500,
		TCPAddr:           ":6379",
		HTTPAddr:          ":8080",
		Store:             StoreSharded,
		ShardCount:        256,
		Policy:            AllKeysLRU,
		AOFEnabled:        false,
		AOFPath:           "redisgo.aof",
		AOFSync:           aof.SyncEverySec,
		SnapshotPath:      "redisgo.rdb",
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
	if p, ok := ParsePolicy(os.Getenv("MAXMEMORY_POLICY")); ok {
		c.Policy = p
	}
	if os.Getenv("AOF_ENABLED") == "true" {
		c.AOFEnabled = true
	}
	if v := os.Getenv("AOF_PATH"); v != "" {
		c.AOFPath = v
	}
	if p, ok := aof.ParseSyncPolicy(os.Getenv("AOF_SYNC")); ok {
		c.AOFSync = p
	}
	if v := os.Getenv("SNAPSHOT_PATH"); v != "" {
		c.SnapshotPath = v
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
	Version           string  `json:"version"`
	UptimeSeconds     int64   `json:"uptime_seconds"`
	Keys              int     `json:"keys"`
	CommandsProcessed uint64  `json:"commands_processed"`
	Connections       int     `json:"connections"`
	ExpiredKeys       uint64  `json:"expired_keys"`
	EvictedKeys       uint64  `json:"evicted_keys"`
	MemoryUsedBytes   int64   `json:"memory_used_bytes"`
	MaxMemoryBytes    int64   `json:"max_memory_bytes"`
	MemoryPercent     float64 `json:"memory_percent"`
	Policy            string  `json:"policy"`
	StoreKind         string  `json:"store_kind"`
	AOFEnabled        bool    `json:"aof_enabled"`
	AOFSizeBytes      int64   `json:"aof_size_bytes"`
	LastSaveUnix      int64   `json:"last_save_unix"`
}

type Handler func(e *Engine, args []string) resp.Value

// Command carries the metadata the dispatcher needs. Arity follows Redis's
// convention: positive means exactly that many arguments including the command
// name, negative means at least that many. The write flag is what tells the
// engine which commands must check memory first and reach the AOF afterwards.
type Command struct {
	Name    string
	Arity   int
	Flags   []string
	Handler Handler
}

func (c Command) isWrite() bool {
	for _, flag := range c.Flags {
		if flag == "write" {
			return true
		}
	}
	return false
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

	// admin
	register(Command{Name: "CONFIG", Arity: -3, Handler: cmdConfig})
	register(Command{Name: "INFO", Arity: -1, Handler: cmdInfo})
	register(Command{Name: "MEMORY", Arity: -2, Handler: cmdMemory})
	register(Command{Name: "OBJECT", Arity: 3, Handler: cmdObject})

	// persistence
	register(Command{Name: "SAVE", Arity: 1, Handler: cmdSave})
	register(Command{Name: "BGSAVE", Arity: 1, Handler: cmdBGSave})
	register(Command{Name: "BGREWRITEAOF", Arity: 1, Handler: cmdBGRewriteAOF})
	register(Command{Name: "LASTSAVE", Arity: 1, Handler: cmdLastSave})
	register(Command{Name: "PEXPIREAT", Arity: 3, Flags: []string{"write"}, Handler: cmdPExpireAt})
}

type Engine struct {
	cfg       Config
	clock     Clock
	store     Store
	startedAt time.Time

	// aofLog is nil when persistence is off. Every write command is appended
	// after it succeeds, so a rejected command never reaches the log.
	aofLog *aof.Log

	// loading suppresses logging during replay: the commands being replayed
	// came from the log, and re-appending them would double the file on every
	// restart.
	loading atomic.Bool

	// Reconfigurable at runtime through CONFIG SET, and read on every write,
	// so they are atomics rather than mutex-guarded fields.
	maxMemory atomic.Int64
	policy    atomic.Uint32

	mu              sync.Mutex
	commands        uint64
	conns           int
	expired         uint64
	evicted         uint64
	rewriteMu       sync.Mutex
	saving          atomic.Bool
	rewriting       atomic.Bool
	rewriteBaseline atomic.Int64
	lastSave        time.Time
}

func New(cfg Config) *Engine {
	return NewWithClock(cfg, realClock{})
}

// NewWithClock builds an engine on an injected clock, for tests.
func NewWithClock(cfg Config, clock Clock) *Engine {
	e := &Engine{
		cfg:       cfg,
		clock:     clock,
		store:     newStore(cfg, clock),
		startedAt: clock.Now(),
	}
	e.maxMemory.Store(int64(cfg.MaxMemoryMB) * 1024 * 1024)
	e.policy.Store(uint32(cfg.Policy))
	return e
}

func (e *Engine) EnableAOF() error {
	// The snapshot loads first because it is the older, coarser record: it
	// holds everything up to the last dump, and the AOF then replays whatever
	// happened after. Loading them the other way round would let stale
	// snapshot values overwrite newer logged ones.
	if _, err := e.LoadSnapshot(); err != nil {
		return err
	}

	if !e.cfg.AOFEnabled {
		return nil
	}

	start := e.clock.Now()

	e.loading.Store(true)
	applied, err := aof.Replay(e.cfg.AOFPath, func(args []string) {
		e.Execute(args)
	})
	e.loading.Store(false)

	if err != nil {
		return err
	}
	if applied > 0 {
		log.Printf("aof: replayed %d commands in %v, %d keys restored",
			applied, e.clock.Now().Sub(start), e.store.Len())
	}

	logFile, err := aof.Open(e.cfg.AOFPath, e.cfg.AOFSync)
	if err != nil {
		return err
	}
	e.aofLog = logFile
	return nil
}

// CloseAOF flushes and closes the log. Safe to call when AOF is off.
func (e *Engine) CloseAOF() error {
	if e.aofLog == nil {
		return nil
	}
	return e.aofLog.Close()
}

func (e *Engine) Policy() Policy     { return Policy(e.policy.Load()) }
func (e *Engine) SetPolicy(p Policy) { e.policy.Store(uint32(p)) }
func (e *Engine) MaxMemory() int64   { return e.maxMemory.Load() }
func (e *Engine) SetMaxMemory(n int64) {
	if n < 0 {
		n = 0
	}
	e.maxMemory.Store(n)
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

// ExpireNow runs one expiry cycle immediately, so tests need not wait.
func (e *Engine) ExpireNow() int {
	removed := e.store.ExpireCycle(expirySampleSize, expiryThreshold)
	if removed > 0 {
		e.mu.Lock()
		e.expired += uint64(removed)
		e.mu.Unlock()
	}
	return removed
}

// evictIfNeeded frees memory until usage is back under the limit. It reports
// whether the write may proceed; false means the policy is noeviction and the
// server is full.
func (e *Engine) evictIfNeeded() bool {
	limit := e.maxMemory.Load()
	if limit <= 0 || e.store.MemoryUsed() <= limit {
		return true
	}

	policy := e.Policy()
	if policy == NoEviction {
		return false
	}

	freed := 0
	for round := 0; round < evictMaxRounds && e.store.MemoryUsed() > limit; round++ {
		candidates := e.store.Sample(evictSampleSize, policy.volatileOnly())

		victim, ok := pickVictim(candidates, policy)
		if !ok {
			// Nothing eligible. Under a volatile policy this means no key has
			// a TTL, and Redis treats that as out of memory rather than
			// evicting data that was never marked as disposable.
			break
		}
		if e.store.Delete(victim) {
			freed++
		}
	}

	if freed > 0 {
		e.mu.Lock()
		e.evicted += uint64(freed)
		e.mu.Unlock()
	}

	return e.store.MemoryUsed() <= limit
}

func (e *Engine) Config() Config { return e.cfg }

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	commands, conns, expired, evicted := e.commands, e.conns, e.expired, e.evicted
	lastSave := e.lastSave
	e.mu.Unlock()

	used := e.store.MemoryUsed()
	limit := e.maxMemory.Load()

	percent := 0.0
	if limit > 0 {
		percent = float64(used) / float64(limit) * 100
	}

	var aofSize int64
	if e.aofLog != nil {
		aofSize = e.aofLog.Size()
	}

	return Stats{
		Version:           Version,
		UptimeSeconds:     int64(e.clock.Now().Sub(e.startedAt).Seconds()),
		Keys:              e.store.Len(),
		CommandsProcessed: commands,
		Connections:       conns,
		ExpiredKeys:       expired,
		EvictedKeys:       evicted,
		MemoryUsedBytes:   used,
		MaxMemoryBytes:    limit,
		MemoryPercent:     percent,
		Policy:            e.Policy().String(),
		StoreKind:         string(e.cfg.Store),
		AOFEnabled:        e.aofLog != nil,
		AOFSizeBytes:      aofSize,
		LastSaveUnix:      lastSave.Unix(),
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

	// Memory is checked before each write. Eviction brings usage below the
	// limit and the write then lands on top, so maxmemory is a target the
	// server converges on rather than a hard ceiling: usage can exceed it by
	// roughly the size of one value.
	if cmd.isWrite() && !e.evictIfNeeded() {
		return resp.Err("OOM command not allowed when used memory > 'maxmemory'")
	}

	e.mu.Lock()
	e.commands++
	e.mu.Unlock()

	reply := cmd.Handler(e, args)

	// The log is written after the fact, and only on success. A command that
	// hit a WRONGTYPE or was refused for memory must not be in the log, or
	// replay would rebuild a state the server never actually had.
	//
	// The rewrite lock is held across the append so a command cannot land on
	// the old file in the window where a rewrite is closing it and renaming
	// the replacement into place.
	if cmd.isWrite() && e.aofLog != nil && !e.loading.Load() && reply.Type != resp.Error {
		e.rewriteMu.Lock()
		if err := e.aofLog.Append(args); err != nil {
			log.Printf("aof: append failed: %v", err)
		}
		e.rewriteMu.Unlock()
	}

	return reply
}

func arityOK(arity, got int) bool {
	if arity >= 0 {
		return got == arity
	}
	return got >= -arity
}

// wrongType is the error real Redis returns when a command meets a key of the
// wrong kind. Clients match on this prefix, so the wording matters.
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
