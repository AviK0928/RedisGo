package engine

import (
	"strconv"
	"strings"
	"testing"

	"github.com/AviK0928/RedisGo/internal/resp"
)

func newTestSession(t *testing.T, limits SessionLimits) (*Engine, *Session) {
	t.Helper()
	e := NewWithClock(DefaultConfig(), NewFakeClock())
	return e, e.NewSession("test", limits)
}

// The point of namespacing: two visitors cannot see or destroy each other's
// data even though they share one engine.
func TestSessionsAreIsolated(t *testing.T) {
	e := NewWithClock(DefaultConfig(), NewFakeClock())
	alice := e.NewSession("alice", DefaultSessionLimits())
	bob := e.NewSession("bob", DefaultSessionLimits())

	alice.Execute([]string{"SET", "shared", "from-alice"})
	bob.Execute([]string{"SET", "shared", "from-bob"})

	if got := alice.Execute([]string{"GET", "shared"}); got.Str != "from-alice" {
		t.Errorf("alice sees %q, want from-alice", got.Str)
	}
	if got := bob.Execute([]string{"GET", "shared"}); got.Str != "from-bob" {
		t.Errorf("bob sees %q, want from-bob", got.Str)
	}

	// One visitor flushing must not touch the other.
	bob.Execute([]string{"FLUSHALL"})

	if got := alice.Execute([]string{"GET", "shared"}); got.Str != "from-alice" {
		t.Errorf("alice's key vanished when bob flushed: %+v", got)
	}
	if got := bob.Execute([]string{"GET", "shared"}); !got.IsNull {
		t.Errorf("bob's key survived his own flush: %+v", got)
	}
}

// KEYS and DBSIZE must report the session's view, not the server's.
func TestSessionKeyspaceCommands(t *testing.T) {
	e := NewWithClock(DefaultConfig(), NewFakeClock())
	alice := e.NewSession("alice", DefaultSessionLimits())
	bob := e.NewSession("bob", DefaultSessionLimits())

	alice.Execute([]string{"SET", "a1", "x"})
	alice.Execute([]string{"SET", "a2", "x"})
	bob.Execute([]string{"SET", "b1", "x"})

	if got := alice.Execute([]string{"DBSIZE"}); got.Num != 2 {
		t.Errorf("alice's DBSIZE = %d, want 2", got.Num)
	}

	keys := alice.Execute([]string{"KEYS", "*"})
	if len(keys.Array) != 2 {
		t.Fatalf("alice's KEYS returned %d entries, want 2", len(keys.Array))
	}
	for _, k := range keys.Array {
		if strings.Contains(k.Str, "sess:") {
			t.Errorf("KEYS leaked the internal prefix: %q", k.Str)
		}
	}

	// The engine underneath holds everyone's keys.
	if got := e.store.Len(); got != 3 {
		t.Errorf("engine holds %d keys, want 3", got)
	}
}

// MSET's keys are at 1, 3, 5..., which is what KeyStep exists for. If the
// spec were wrong, values would be namespaced as though they were keys.
func TestMultiKeyCommandsNamespaceCorrectly(t *testing.T) {
	_, s := newTestSession(t, DefaultSessionLimits())

	s.Execute([]string{"MSET", "k1", "v1", "k2", "v2"})

	if got := s.Execute([]string{"GET", "k1"}); got.Str != "v1" {
		t.Errorf("GET k1 = %+v, want v1", got)
	}
	if got := s.Execute([]string{"GET", "k2"}); got.Str != "v2" {
		t.Errorf("GET k2 = %+v, want v2", got)
	}

	reply := s.Execute([]string{"MGET", "k1", "k2", "missing"})
	if len(reply.Array) != 3 || reply.Array[0].Str != "v1" || !reply.Array[2].IsNull {
		t.Errorf("MGET = %+v", reply)
	}
}

func TestSessionKeyLimit(t *testing.T) {
	limits := DefaultSessionLimits()
	limits.MaxKeys = 5
	_, s := newTestSession(t, limits)

	for i := 0; i < 5; i++ {
		if got := s.Execute([]string{"SET", "k" + strconv.Itoa(i), "v"}); got.Type == resp.Error {
			t.Fatalf("write %d rejected early: %+v", i, got)
		}
	}

	if got := s.Execute([]string{"SET", "one-too-many", "v"}); got.Type != resp.Error {
		t.Errorf("write past the limit succeeded: %+v", got)
	}

	// Overwriting an existing key stays legal at the limit.
	if got := s.Execute([]string{"SET", "k0", "updated"}); got.Type == resp.Error {
		t.Errorf("overwrite at the limit rejected: %+v", got)
	}
}

func TestSessionValueSizeLimit(t *testing.T) {
	limits := DefaultSessionLimits()
	limits.MaxValueSize = 100
	_, s := newTestSession(t, limits)

	if got := s.Execute([]string{"SET", "k", strings.Repeat("x", 50)}); got.Type == resp.Error {
		t.Errorf("small value rejected: %+v", got)
	}
	if got := s.Execute([]string{"SET", "k", strings.Repeat("x", 200)}); got.Type != resp.Error {
		t.Errorf("oversized value accepted: %+v", got)
	}
}

func TestSessionRateLimit(t *testing.T) {
	limits := DefaultSessionLimits()
	limits.CommandsPerSecond = 10
	limits.Burst = 5
	_, s := newTestSession(t, limits)

	// The clock is frozen, so no tokens refill during the loop.
	throttled := false
	for i := 0; i < 20; i++ {
		if got := s.Execute([]string{"PING"}); got.Type == resp.Error {
			throttled = true
			break
		}
	}
	if !throttled {
		t.Error("rate limit never fired")
	}
}

func TestSessionBlocksServerWideCommands(t *testing.T) {
	_, s := newTestSession(t, DefaultSessionLimits())

	for _, args := range [][]string{
		{"SAVE"},
		{"BGSAVE"},
		{"BGREWRITEAOF"},
		{"CONFIG", "SET", "maxmemory", "1gb"},
	} {
		if got := s.Execute(args); got.Type != resp.Error {
			t.Errorf("%v was allowed in a session: %+v", args, got)
		}
	}

	// Reading configuration is harmless and stays available.
	if got := s.Execute([]string{"CONFIG", "GET", "maxmemory-policy"}); got.Type == resp.Error {
		t.Errorf("CONFIG GET blocked: %+v", got)
	}
}

func TestSessionCloseCleansUp(t *testing.T) {
	e := NewWithClock(DefaultConfig(), NewFakeClock())
	s := e.NewSession("temp", DefaultSessionLimits())

	s.Execute([]string{"SET", "a", "1"})
	s.Execute([]string{"SET", "b", "2"})
	if e.store.Len() != 2 {
		t.Fatalf("setup wrote %d keys, want 2", e.store.Len())
	}

	s.Close()

	if e.store.Len() != 0 {
		t.Errorf("%d keys survived session close", e.store.Len())
	}
}
