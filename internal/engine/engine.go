// Package engine is the cache core. It knows nothing about sockets, so the TCP
// front door and the HTTP front door can share one implementation.
package engine

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Version is reported by the stats endpoint and, later, by INFO.
const Version = "0.1.0"

// Config holds every tunable.
type Config struct {
	MaxMemoryMB       int
	MaxKeysPerSession int
	TCPAddr           string
	HTTPAddr          string
}

// DefaultConfig is what you get when nothing is set.
func DefaultConfig() Config {
	return Config{
		MaxMemoryMB:       32,
		MaxKeysPerSession: 500,
		TCPAddr:           ":6379",
		HTTPAddr:          ":8080",
	}
}

// ConfigFromEnv layers environment variables on top of the defaults.
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

// Stats is the payload behind /api/stats.
type Stats struct {
	Version           string `json:"version"`
	UptimeSeconds     int64  `json:"uptime_seconds"`
	Keys              int    `json:"keys"`
	CommandsProcessed uint64 `json:"commands_processed"`
	Connections       int    `json:"connections"`
	MaxMemoryMB       int    `json:"max_memory_mb"`
}

// Reply is a command result. Phase 1 replaces this with a full RESP value.
type Reply struct {
	Text  string
	IsErr bool
}

func okReply(text string) Reply {
	return Reply{Text: text}
}

func errReply(text string) Reply {
	return Reply{Text: text, IsErr: true}
}

// Engine owns the keyspace and the server-wide counters.
type Engine struct {
	cfg       Config
	startedAt time.Time

	mu       sync.Mutex
	commands uint64
	conns    int
}

// New builds an engine. It starts no listeners; that is the caller's job.
func New(cfg Config) *Engine {
	return &Engine{cfg: cfg, startedAt: time.Now()}
}

// Config returns the configuration this engine was built with.
func (e *Engine) Config() Config {
	return e.cfg
}

// Stats snapshots the current counters.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return Stats{
		Version:           Version,
		UptimeSeconds:     int64(time.Since(e.startedAt).Seconds()),
		Keys:              0, // the real keyspace arrives in phase 2
		CommandsProcessed: e.commands,
		Connections:       e.conns,
		MaxMemoryMB:       e.cfg.MaxMemoryMB,
	}
}

// AddConn adjusts the live connection count. Pass -1 on disconnect.
func (e *Engine) AddConn(delta int) {
	e.mu.Lock()
	e.conns += delta
	e.mu.Unlock()
}

// Execute runs one command.
//
// PHASE 0 PLACEHOLDER. In phase 1 this becomes a dispatch table keyed on the
// command name and returns a resp.Value. Keep the shape (args in, reply out)
// so the TCP and HTTP callers do not have to change.
func (e *Engine) Execute(args []string) Reply {
	if len(args) == 0 {
		return errReply("ERR empty command")
	}

	e.mu.Lock()
	e.commands++
	e.mu.Unlock()

	switch strings.ToUpper(args[0]) {
	case "PING":
		if len(args) > 1 {
			return okReply(strings.Join(args[1:], " "))
		}
		return okReply("PONG")
	case "ECHO":
		if len(args) != 2 {
			return errReply("ERR wrong number of arguments for 'echo' command")
		}
		return okReply(args[1])
	case "VERSION":
		return okReply("redisgo " + Version)
	default:
		return errReply(fmt.Sprintf("ERR unknown command '%s' (implemented in a later phase)", args[0]))
	}
}
