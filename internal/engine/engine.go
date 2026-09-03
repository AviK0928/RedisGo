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

const Version = "0.2.0"

type Config struct {
	MaxMemoryMB       int
	MaxKeysPerSession int
	TCPAddr           string
	HTTPAddr          string
}

func DefaultConfig() Config {
	return Config{
		MaxMemoryMB:       32,
		MaxKeysPerSession: 500,
		TCPAddr:           ":6379",
		HTTPAddr:          ":8080",
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

type Stats struct {
	Version           string `json:"version"`
	UptimeSeconds     int64  `json:"uptime_seconds"`
	Keys              int    `json:"keys"`
	CommandsProcessed uint64 `json:"commands_processed"`
	Connections       int    `json:"connections"`
	MaxMemoryMB       int    `json:"max_memory_mb"`
}

// Handler runs one command. Every handler returns a RESP value, including for
// errors, because an error is a first-class reply type in the protocol.
type Handler func(e *Engine, args []string) resp.Value

// Command carries the metadata the dispatcher needs. Arity follows Redis's
// convention: positive means exactly that many arguments including the command
// name, negative means at least that many. Flags will matter in later phases,
// when eviction and replication need to know which commands write.
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
	register(Command{Name: "PING", Arity: -1, Handler: cmdPing})
	register(Command{Name: "ECHO", Arity: 2, Handler: cmdEcho})
	register(Command{Name: "SET", Arity: -3, Flags: []string{"write"}, Handler: cmdSet})
	register(Command{Name: "GET", Arity: 2, Flags: []string{"readonly"}, Handler: cmdGet})
	register(Command{Name: "DEL", Arity: -2, Flags: []string{"write"}, Handler: cmdDel})
	register(Command{Name: "EXISTS", Arity: -2, Flags: []string{"readonly"}, Handler: cmdExists})
	register(Command{Name: "DBSIZE", Arity: 1, Flags: []string{"readonly"}, Handler: cmdDBSize})
	register(Command{Name: "FLUSHALL", Arity: -1, Flags: []string{"write"}, Handler: cmdFlushAll})
	register(Command{Name: "COMMAND", Arity: -1, Handler: cmdCommand})
}

type Engine struct {
	cfg       Config
	startedAt time.Time

	mu       sync.Mutex
	commands uint64
	conns    int

	dataMu sync.RWMutex
	data   map[string]string
}

func New(cfg Config) *Engine {
	return &Engine{
		cfg:       cfg,
		startedAt: time.Now(),
		data:      make(map[string]string),
	}
}

func (e *Engine) Config() Config {
	return e.cfg
}

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	commands, conns := e.commands, e.conns
	e.mu.Unlock()

	return Stats{
		Version:           Version,
		UptimeSeconds:     int64(time.Since(e.startedAt).Seconds()),
		Keys:              e.keyCount(),
		CommandsProcessed: commands,
		Connections:       conns,
		MaxMemoryMB:       e.cfg.MaxMemoryMB,
	}
}

func (e *Engine) keyCount() int {
	e.dataMu.RLock()
	defer e.dataMu.RUnlock()
	return len(e.data)
}

// AddConn adjusts the live connection count. Pass -1 on disconnect.
func (e *Engine) AddConn(delta int) {
	e.mu.Lock()
	e.conns += delta
	e.mu.Unlock()
}

// Execute looks up one command, checks its arity, and runs it.
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

func cmdPing(e *Engine, args []string) resp.Value {
	if len(args) > 2 {
		return resp.Err("ERR wrong number of arguments for 'ping' command")
	}
	if len(args) == 2 {
		return resp.BulkString(args[1])
	}
	return resp.Simple("PONG")
}

func cmdEcho(e *Engine, args []string) resp.Value {
	return resp.BulkString(args[1])
}

func cmdSet(e *Engine, args []string) resp.Value {
	// Options like EX and NX arrive in phase 2, along with expiry.
	if len(args) != 3 {
		return resp.Err("ERR syntax error")
	}

	e.dataMu.Lock()
	e.data[args[1]] = args[2]
	e.dataMu.Unlock()

	return resp.Simple("OK")
}

func cmdGet(e *Engine, args []string) resp.Value {
	e.dataMu.RLock()
	value, found := e.data[args[1]]
	e.dataMu.RUnlock()

	if !found {
		return resp.NullBulk()
	}
	return resp.BulkString(value)
}

func cmdDel(e *Engine, args []string) resp.Value {
	deleted := 0

	e.dataMu.Lock()
	for _, key := range args[1:] {
		if _, found := e.data[key]; found {
			delete(e.data, key)
			deleted++
		}
	}
	e.dataMu.Unlock()

	return resp.Int(int64(deleted))
}

func cmdExists(e *Engine, args []string) resp.Value {
	count := 0

	e.dataMu.RLock()
	for _, key := range args[1:] {
		if _, found := e.data[key]; found {
			count++
		}
	}
	e.dataMu.RUnlock()

	return resp.Int(int64(count))
}

func cmdDBSize(e *Engine, args []string) resp.Value {
	return resp.Int(int64(e.keyCount()))
}

func cmdFlushAll(e *Engine, args []string) resp.Value {
	e.dataMu.Lock()
	e.data = make(map[string]string)
	e.dataMu.Unlock()

	return resp.Simple("OK")
}

// cmdCommand is a stub. redis-cli sends COMMAND DOCS on connect and waits for
// a reply before showing a prompt, so without this the client appears to hang.
func cmdCommand(e *Engine, args []string) resp.Value {
	return resp.Arr()
}
