package engine

import (
	"strings"
	"sync"
	"time"

	"github.com/AviK0928/RedisGo/internal/resp"
)

// SessionLimits caps what one visitor to the public playground can do.
//
// A cache server exposed to the internet with no limits is a free memory
// allocator for anyone who finds it. These are deliberately small: the demo
// runs on a 512MB instance shared by everyone who opens the page.
type SessionLimits struct {
	MaxKeys           int
	MaxValueSize      int
	CommandsPerSecond int
	Burst             int
}

func DefaultSessionLimits() SessionLimits {
	return SessionLimits{
		MaxKeys:           500,
		MaxValueSize:      64 * 1024,
		CommandsPerSecond: 50,
		Burst:             100,
	}
}

// Session is one visitor's view of the engine.
//
// Every key is silently prefixed with a session identifier, so two people
// using the playground at once cannot see or destroy each other's data. The
// prefix is added on the way in and stripped on the way out, so from the
// browser it looks like a private server.
type Session struct {
	engine *Engine
	id     string
	prefix string
	limits SessionLimits

	mu       sync.Mutex
	tokens   float64
	lastFill time.Time
	keys     int
}

// NewSession creates a namespaced view. The id should be unpredictable, since
// guessing another session's id would expose its keys.
func (e *Engine) NewSession(id string, limits SessionLimits) *Session {
	return &Session{
		engine:   e,
		id:       id,
		prefix:   "sess:" + id + ":",
		limits:   limits,
		tokens:   float64(limits.Burst),
		lastFill: e.clock.Now(),
	}
}

func (s *Session) ID() string { return s.id }

// blocked commands would let one visitor affect the whole server rather than
// their own keyspace.
var sessionBlocked = map[string]string{
	"save":         "SAVE is disabled in the playground",
	"bgsave":       "BGSAVE is disabled in the playground",
	"bgrewriteaof": "BGREWRITEAOF is disabled in the playground",
	"lastsave":     "LASTSAVE is disabled in the playground",
}

// Execute runs one command in the session's namespace.
func (s *Session) Execute(args []string) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR empty command")
	}

	name := strings.ToLower(args[0])

	if reason, blocked := sessionBlocked[name]; blocked {
		return resp.Errf("ERR %s", reason)
	}

	// CONFIG SET would change the whole server's behaviour, so only the read
	// half is allowed.
	if name == "config" && len(args) > 1 && strings.EqualFold(args[1], "SET") {
		return resp.Err("ERR CONFIG SET is disabled in the playground")
	}

	if !s.allow() {
		return resp.Err("ERR rate limit exceeded, slow down")
	}

	cmd, found := Lookup(name)
	if !found {
		return resp.Errf("ERR unknown command '%s'", args[0])
	}

	if err := s.checkValueSizes(args); err != "" {
		return resp.Err(err)
	}

	// Whole-keyspace commands are answered from the session's own keys rather
	// than passed through, so one visitor never sees the server's true size.
	switch name {
	case "keys":
		return s.keysCommand()
	case "dbsize":
		return resp.Int(int64(s.countKeys()))
	case "flushall":
		return s.flushCommand()
	}

	if cmd.isWrite() && !s.underKeyLimit(cmd, args) {
		return resp.Errf("ERR session key limit of %d reached, use DEL or FLUSHALL",
			s.limits.MaxKeys)
	}

	return s.engine.Execute(s.namespace(cmd, args))
}

// namespace rewrites key arguments to include the session prefix.
func (s *Session) namespace(cmd Command, args []string) []string {
	positions := cmd.keyPositions(len(args))
	if len(positions) == 0 {
		return args
	}

	// Copy, so the caller's slice is untouched. The browser echoes the
	// original command back to the terminal and it must not show the prefix.
	out := make([]string, len(args))
	copy(out, args)
	for _, i := range positions {
		out[i] = s.prefix + args[i]
	}
	return out
}

func (s *Session) sessionKeys() []string {
	var keys []string
	for _, key := range s.engine.store.Keys() {
		if strings.HasPrefix(key, s.prefix) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (s *Session) countKeys() int {
	return len(s.sessionKeys())
}

func (s *Session) keysCommand() resp.Value {
	keys := s.sessionKeys()
	values := make([]resp.Value, 0, len(keys))
	for _, key := range keys {
		values = append(values, resp.BulkString(strings.TrimPrefix(key, s.prefix)))
	}
	return resp.Arr(values...)
}

func (s *Session) flushCommand() resp.Value {
	for _, key := range s.sessionKeys() {
		s.engine.store.Delete(key)
	}
	return resp.Simple("OK")
}

func (s *Session) underKeyLimit(cmd Command, args []string) bool {
	// Only bother counting when the command could add a key that is not
	// already there. Counting on every write would be O(keyspace) per command.
	if s.countKeys() < s.limits.MaxKeys {
		return true
	}

	// At the limit, writes to keys that already exist are still fine.
	for _, i := range cmd.keyPositions(len(args)) {
		if !s.engine.store.View(s.prefix+args[i], func(*Entry) {}) {
			return false
		}
	}
	return true
}

func (s *Session) checkValueSizes(args []string) string {
	for _, arg := range args {
		if len(arg) > s.limits.MaxValueSize {
			return "ERR value exceeds the playground size limit"
		}
	}
	return ""
}

// allow is a token bucket: tokens refill at a steady rate up to a burst
// ceiling, and each command spends one. A leaky-bucket limiter would smooth
// bursts away, but someone pasting a block of commands into the terminal
// should not be throttled for it, so a burst allowance is the right shape.
func (s *Session) allow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.engine.clock.Now()
	elapsed := now.Sub(s.lastFill).Seconds()
	s.lastFill = now

	s.tokens += elapsed * float64(s.limits.CommandsPerSecond)
	if s.tokens > float64(s.limits.Burst) {
		s.tokens = float64(s.limits.Burst)
	}

	if s.tokens < 1 {
		return false
	}
	s.tokens--
	return true
}

// Close removes everything the session created.
func (s *Session) Close() {
	for _, key := range s.sessionKeys() {
		s.engine.store.Delete(key)
	}
}

// SessionStats is what the dashboard shows: the visitor's own numbers
// alongside the server-wide ones.
type SessionStats struct {
	Stats
	SessionKeys    int `json:"session_keys"`
	SessionMaxKeys int `json:"session_max_keys"`
}

func (s *Session) Stats() SessionStats {
	return SessionStats{
		Stats:          s.engine.Stats(),
		SessionKeys:    s.countKeys(),
		SessionMaxKeys: s.limits.MaxKeys,
	}
}
