package engine

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AviK0928/RedisGo/internal/resp"
)

func TestSetWithExpiry(t *testing.T) {
	clock := NewFakeClock()
	e := NewWithClock(DefaultConfig(), clock)

	e.Execute([]string{"SET", "k", "v", "EX", "10"})

	if got := e.Execute([]string{"TTL", "k"}); got.Num != 10 {
		t.Errorf("TTL = %d, want 10", got.Num)
	}

	clock.Advance(5 * time.Second)
	if got := e.Execute([]string{"GET", "k"}); got.Str != "v" {
		t.Errorf("GET before expiry = %+v, want v", got)
	}

	clock.Advance(6 * time.Second)
	if got := e.Execute([]string{"GET", "k"}); !got.IsNull {
		t.Errorf("GET after expiry = %+v, want nil", got)
	}
}

func TestTTLSentinels(t *testing.T) {
	e := NewWithClock(DefaultConfig(), NewFakeClock())

	if got := e.Execute([]string{"TTL", "missing"}); got.Num != -2 {
		t.Errorf("TTL of missing key = %d, want -2", got.Num)
	}

	e.Execute([]string{"SET", "k", "v"})
	if got := e.Execute([]string{"TTL", "k"}); got.Num != -1 {
		t.Errorf("TTL of key without expiry = %d, want -1", got.Num)
	}
}

func TestPersist(t *testing.T) {
	clock := NewFakeClock()
	e := NewWithClock(DefaultConfig(), clock)

	e.Execute([]string{"SET", "k", "v", "EX", "10"})
	if got := e.Execute([]string{"PERSIST", "k"}); got.Num != 1 {
		t.Errorf("PERSIST = %d, want 1", got.Num)
	}

	clock.Advance(time.Hour)
	if got := e.Execute([]string{"GET", "k"}); got.Str != "v" {
		t.Errorf("GET after PERSIST and a long wait = %+v, want v", got)
	}
}

// Active expiration must reclaim keys nobody reads. Lazy expiry alone would
// leave these in the map forever.
func TestActiveExpiry(t *testing.T) {
	clock := NewFakeClock()
	e := NewWithClock(DefaultConfig(), clock)

	for i := 0; i < 50; i++ {
		e.Execute([]string{"SET", "key" + strconv.Itoa(i), "v", "EX", "10"})
	}
	if got := e.Stats().Keys; got != 50 {
		t.Fatalf("keys before expiry = %d, want 50", got)
	}

	clock.Advance(11 * time.Second)

	// Nothing has read these keys, so only the active cycle can remove them.
	for i := 0; i < 20 && e.Stats().Keys > 0; i++ {
		e.ExpireNow()
	}

	if got := e.Stats().Keys; got != 0 {
		t.Errorf("keys after active expiry = %d, want 0", got)
	}
	if got := e.Stats().ExpiredKeys; got != 50 {
		t.Errorf("expired counter = %d, want 50", got)
	}
}

func TestSetNXAndXX(t *testing.T) {
	e := NewWithClock(DefaultConfig(), NewFakeClock())

	if got := e.Execute([]string{"SET", "k", "first", "NX"}); got.Str != "OK" {
		t.Errorf("SET NX on a missing key = %+v, want OK", got)
	}
	if got := e.Execute([]string{"SET", "k", "second", "NX"}); !got.IsNull {
		t.Errorf("SET NX on an existing key = %+v, want nil", got)
	}
	if got := e.Execute([]string{"GET", "k"}); got.Str != "first" {
		t.Errorf("value = %q, want first", got.Str)
	}
	if got := e.Execute([]string{"SET", "missing", "v", "XX"}); !got.IsNull {
		t.Errorf("SET XX on a missing key = %+v, want nil", got)
	}
}

func TestWrongType(t *testing.T) {
	e := NewWithClock(DefaultConfig(), NewFakeClock())

	e.Execute([]string{"LPUSH", "mylist", "a"})

	for _, args := range [][]string{
		{"GET", "mylist"},
		{"INCR", "mylist"},
		{"HGET", "mylist", "field"},
		{"APPEND", "mylist", "x"},
	} {
		got := e.Execute(args)
		if got.Type != resp.Error || !strings.HasPrefix(got.Str, "WRONGTYPE") {
			t.Errorf("%v = %+v, want a WRONGTYPE error", args, got)
		}
	}
}
