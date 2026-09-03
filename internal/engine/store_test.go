package engine

import (
	"fmt"
	"sync"
	"testing"
)

// implementations lets every test below run against all three stores, so a
// benchmark comparison cannot quietly be comparing one correct implementation
// against two broken ones.
func implementations() map[string]func() Config {
	return map[string]func() Config{
		"sharded": func() Config {
			c := DefaultConfig()
			c.Store = StoreSharded
			return c
		},
		"global": func() Config {
			c := DefaultConfig()
			c.Store = StoreGlobal
			return c
		},
		"syncmap": func() Config {
			c := DefaultConfig()
			c.Store = StoreSyncMap
			return c
		},
	}
}

func TestStoreImplementationsAgree(t *testing.T) {
	for name, config := range implementations() {
		t.Run(name, func(t *testing.T) {
			e := NewWithClock(config(), NewFakeClock())

			if got := e.Execute([]string{"SET", "k", "v"}); got.Str != "OK" {
				t.Errorf("SET = %+v", got)
			}
			if got := e.Execute([]string{"GET", "k"}); got.Str != "v" {
				t.Errorf("GET = %+v", got)
			}
			if got := e.Execute([]string{"DBSIZE"}); got.Num != 1 {
				t.Errorf("DBSIZE = %d, want 1", got.Num)
			}
			if got := e.Execute([]string{"INCRBY", "n", "5"}); got.Num != 5 {
				t.Errorf("INCRBY = %d, want 5", got.Num)
			}
			if got := e.Execute([]string{"DEL", "k"}); got.Num != 1 {
				t.Errorf("DEL = %d, want 1", got.Num)
			}
			if got := e.Execute([]string{"GET", "k"}); !got.IsNull {
				t.Errorf("GET after DEL = %+v, want nil", got)
			}

			e.Execute([]string{"FLUSHALL"})
			if got := e.Execute([]string{"DBSIZE"}); got.Num != 0 {
				t.Errorf("DBSIZE after FLUSHALL = %d, want 0", got.Num)
			}
		})
	}
}

// The race phase 2 had: many goroutines incrementing one shared counter. If
// read-modify-write is not atomic, the total comes out short. The race
// detector alone would not catch a lost update, so this asserts the value.
func TestConcurrentIncrementIsAtomic(t *testing.T) {
	for name, config := range implementations() {
		t.Run(name, func(t *testing.T) {
			e := NewWithClock(config(), NewFakeClock())

			const goroutines, each = 50, 200
			var wg sync.WaitGroup
			wg.Add(goroutines)

			for i := 0; i < goroutines; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < each; j++ {
						e.Execute([]string{"INCR", "shared"})
					}
				}()
			}
			wg.Wait()

			want := int64(goroutines * each)
			got := e.Execute([]string{"GET", "shared"})
			if got.Str != fmt.Sprint(want) {
				t.Errorf("counter = %s, want %d (lost updates)", got.Str, want)
			}
		})
	}
}

// Concurrent pushes to one list must not lose elements either.
func TestConcurrentListPush(t *testing.T) {
	for name, config := range implementations() {
		t.Run(name, func(t *testing.T) {
			e := NewWithClock(config(), NewFakeClock())

			const goroutines, each = 20, 100
			var wg sync.WaitGroup
			wg.Add(goroutines)

			for i := 0; i < goroutines; i++ {
				go func(id int) {
					defer wg.Done()
					for j := 0; j < each; j++ {
						e.Execute([]string{"RPUSH", "list", fmt.Sprintf("%d-%d", id, j)})
					}
				}(i)
			}
			wg.Wait()

			if got := e.Execute([]string{"LLEN", "list"}); got.Num != goroutines*each {
				t.Errorf("list length = %d, want %d", got.Num, goroutines*each)
			}
		})
	}
}

func TestShardDistribution(t *testing.T) {
	const shards = 256
	counts := make([]int, shards)

	for i := 0; i < 100000; i++ {
		counts[fnv1a(fmt.Sprintf("user:%d", i))&(shards-1)]++
	}

	// A perfectly uniform hash would put ~390 keys in each shard. Assert only
	// that nothing is grossly lopsided; this catches a broken hash, not a
	// slightly imperfect one.
	for i, count := range counts {
		if count < 200 || count > 700 {
			t.Errorf("shard %d holds %d keys, expected roughly 390", i, count)
		}
	}
}
