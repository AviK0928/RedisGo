package engine

import (
	"fmt"
	"sync"
	"testing"

	"github.com/AviK0928/RedisGo/internal/resp"
)

func TestStringCommands(t *testing.T) {
	e := New(DefaultConfig())

	if got := e.Execute([]string{"PING"}); got.Str != "PONG" {
		t.Errorf("PING = %q, want PONG", got.Str)
	}
	if got := e.Execute([]string{"ping"}); got.Str != "PONG" {
		t.Errorf("lowercase ping = %q, want PONG", got.Str)
	}
	if got := e.Execute([]string{"SET", "k", "v"}); got.Str != "OK" {
		t.Errorf("SET = %q, want OK", got.Str)
	}
	if got := e.Execute([]string{"GET", "k"}); got.Str != "v" {
		t.Errorf("GET = %q, want v", got.Str)
	}
	if got := e.Execute([]string{"GET", "missing"}); !got.IsNull {
		t.Errorf("GET missing = %+v, want a null bulk string", got)
	}
	if got := e.Execute([]string{"EXISTS", "k", "missing"}); got.Num != 1 {
		t.Errorf("EXISTS = %d, want 1", got.Num)
	}
	if got := e.Execute([]string{"DBSIZE"}); got.Num != 1 {
		t.Errorf("DBSIZE = %d, want 1", got.Num)
	}
	if got := e.Execute([]string{"DEL", "k", "missing"}); got.Num != 1 {
		t.Errorf("DEL = %d, want 1", got.Num)
	}
	if got := e.Execute([]string{"GET", "k"}); !got.IsNull {
		t.Errorf("GET after DEL = %+v, want a null bulk string", got)
	}
}

func TestErrorCases(t *testing.T) {
	e := New(DefaultConfig())

	cases := [][]string{
		{},
		{"NOSUCHCOMMAND"},
		{"GET"},
		{"GET", "a", "b"},
		{"SET", "onlykey"},
		{"ECHO"},
	}

	for _, args := range cases {
		if got := e.Execute(args); got.Type != resp.Error {
			t.Errorf("Execute(%v) = %+v, want an error reply", args, got)
		}
	}
}

// Guards the counters and the keyspace against data races.
// Run with: go test -race ./...
func TestConcurrentAccess(t *testing.T) {
	e := New(DefaultConfig())

	const goroutines, each = 50, 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			for j := 0; j < each; j++ {
				e.Execute([]string{"SET", key, "value"})
				e.Execute([]string{"GET", key})
				e.AddConn(1)
				e.Stats()
				e.AddConn(-1)
			}
		}(i)
	}
	wg.Wait()

	if got := e.Stats().Keys; got != goroutines {
		t.Errorf("keys = %d, want %d", got, goroutines)
	}
	if got := e.Stats().CommandsProcessed; got != 2*goroutines*each {
		t.Errorf("commands = %d, want %d", got, 2*goroutines*each)
	}
}
