package engine

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AviK0928/RedisGo/internal/resp"
)

func tinyConfig(policy Policy) Config {
	c := DefaultConfig()
	c.MaxMemoryMB = 1
	c.Policy = policy
	return c
}

// The central claim of this phase: writing far more than the limit keeps
// memory bounded instead of growing without end.
func TestEvictionBoundsMemory(t *testing.T) {
	e := NewWithClock(tinyConfig(AllKeysLRU), NewFakeClock())
	limit := e.MaxMemory()

	payload := strings.Repeat("x", 1024)
	for i := 0; i < 20000; i++ {
		e.Execute([]string{"SET", "key" + strconv.Itoa(i), payload})
	}

	used := e.Stats().MemoryUsedBytes

	// Eviction frees memory to below the limit and the write then lands on
	// top, so usage can exceed maxmemory by roughly one value. Real Redis
	// behaves the same way, which is why its documentation describes
	// maxmemory as approximate and advises leaving headroom. What matters
	// here is that usage stays bounded rather than growing without end.
	const overshootAllowance = 64 * 1024

	if used > limit+overshootAllowance {
		t.Errorf("memory used %d exceeds limit %d by more than the %d byte allowance",
			used, limit, overshootAllowance)
	}
	if e.Stats().EvictedKeys == 0 {
		t.Error("nothing was evicted, so the limit was never enforced")
	}
	if e.Stats().Keys == 0 {
		t.Error("everything was evicted; the policy is too aggressive")
	}
}

// noeviction must refuse writes rather than discard data.
func TestNoEvictionReturnsOOM(t *testing.T) {
	e := NewWithClock(tinyConfig(NoEviction), NewFakeClock())

	payload := strings.Repeat("x", 1024)
	var lastReply resp.Value
	for i := 0; i < 20000; i++ {
		lastReply = e.Execute([]string{"SET", "key" + strconv.Itoa(i), payload})
		if lastReply.Type == resp.Error {
			break
		}
	}

	if lastReply.Type != resp.Error || !strings.HasPrefix(lastReply.Str, "OOM") {
		t.Fatalf("expected an OOM error once full, got %+v", lastReply)
	}

	// Reads must still work when the server is full.
	if got := e.Execute([]string{"DBSIZE"}); got.Type == resp.Error {
		t.Errorf("DBSIZE failed while full: %+v", got)
	}
	if e.Stats().EvictedKeys != 0 {
		t.Error("noeviction evicted keys")
	}
}

// LRU should keep the key that is being read and drop the ones that are not.
func TestLRUKeepsHotKey(t *testing.T) {
	clock := NewFakeClock()
	e := NewWithClock(tinyConfig(AllKeysLRU), clock)

	payload := strings.Repeat("x", 512)
	e.Execute([]string{"SET", "hot", payload})

	for i := 0; i < 5000; i++ {
		e.Execute([]string{"SET", "cold" + strconv.Itoa(i), payload})

		// Keep reading the hot key so its idle time stays at zero while the
		// cold keys age.
		clock.Advance(time.Second)
		e.Execute([]string{"GET", "hot"})
	}

	if got := e.Execute([]string{"GET", "hot"}); got.IsNull {
		t.Error("the continuously read key was evicted")
	}
}

// A volatile policy must not touch keys that were never marked disposable.
func TestVolatilePolicySparesPersistentKeys(t *testing.T) {
	e := NewWithClock(tinyConfig(VolatileTTL), NewFakeClock())

	payload := strings.Repeat("x", 512)
	e.Execute([]string{"SET", "permanent", payload})

	for i := 0; i < 5000; i++ {
		e.Execute([]string{"SET", "temp" + strconv.Itoa(i), payload, "EX", "3600"})
	}

	if got := e.Execute([]string{"GET", "permanent"}); got.IsNull {
		t.Error("a key without a TTL was evicted under a volatile policy")
	}
}

func TestConfigGetSet(t *testing.T) {
	e := NewWithClock(DefaultConfig(), NewFakeClock())

	if got := e.Execute([]string{"CONFIG", "SET", "maxmemory", "64mb"}); got.Str != "OK" {
		t.Fatalf("CONFIG SET maxmemory = %+v", got)
	}
	if e.MaxMemory() != 64*1024*1024 {
		t.Errorf("maxmemory = %d, want %d", e.MaxMemory(), 64*1024*1024)
	}

	if got := e.Execute([]string{"CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"}); got.Str != "OK" {
		t.Fatalf("CONFIG SET policy = %+v", got)
	}
	if e.Policy() != AllKeysLFU {
		t.Errorf("policy = %v, want allkeys-lfu", e.Policy())
	}

	if got := e.Execute([]string{"CONFIG", "SET", "maxmemory-policy", "nonsense"}); got.Type != resp.Error {
		t.Errorf("invalid policy accepted: %+v", got)
	}

	reply := e.Execute([]string{"CONFIG", "GET", "maxmemory-policy"})
	if len(reply.Array) != 2 || reply.Array[1].Str != "allkeys-lfu" {
		t.Errorf("CONFIG GET = %+v", reply)
	}
}

func TestMemoryUsageAndInfo(t *testing.T) {
	e := NewWithClock(DefaultConfig(), NewFakeClock())
	e.Execute([]string{"SET", "k", "some value"})

	if got := e.Execute([]string{"MEMORY", "USAGE", "k"}); got.Num <= 0 {
		t.Errorf("MEMORY USAGE = %d, want a positive size", got.Num)
	}
	if got := e.Execute([]string{"MEMORY", "USAGE", "missing"}); !got.IsNull {
		t.Errorf("MEMORY USAGE on a missing key = %+v, want nil", got)
	}

	info := e.Execute([]string{"INFO"})
	for _, section := range []string{"# Server", "# Memory", "# Stats", "# Keyspace", "maxmemory_policy"} {
		if !strings.Contains(info.Str, section) {
			t.Errorf("INFO is missing %q", section)
		}
	}
}
