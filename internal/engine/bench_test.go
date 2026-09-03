package engine

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

// Workload describes a read/write mix. Caches are overwhelmingly read-heavy in
// practice, so 90/10 is the realistic case and 50/50 is the stress case.
type workload struct {
	name      string
	readRatio float64
}

var workloads = []workload{
	{"read90", 0.90},
	{"read50", 0.50},
}

var storeKinds = []StoreKind{StoreSharded, StoreGlobal, StoreSyncMap}

// keyspaceSize decides how much contention there is. A small keyspace means
// many goroutines fighting over the same keys, which is the case that
// separates the implementations.
const keyspaceSize = 10000

func benchConfig(kind StoreKind) Config {
	c := DefaultConfig()
	c.Store = kind
	return c
}

// BenchmarkStore is the headline comparison: every store, every workload, at
// several concurrency levels.
//
// Run with:
//
//	go test -bench=BenchmarkStore -benchmem ./internal/engine/
func BenchmarkStore(b *testing.B) {
	for _, kind := range storeKinds {
		for _, w := range workloads {
			for _, concurrency := range []int{1, 8, 64, 512} {
				name := fmt.Sprintf("%s/%s/conc=%d", kind, w.name, concurrency)

				b.Run(name, func(b *testing.B) {
					e := New(benchConfig(kind))

					// Warm the keyspace, so the benchmark measures steady
					// state rather than map growth.
					for i := 0; i < keyspaceSize; i++ {
						e.Execute([]string{"SET", "key" + strconv.Itoa(i), "value"})
					}

					b.SetParallelism(concurrency)
					b.ResetTimer()

					b.RunParallel(func(pb *testing.PB) {
						// Each goroutine gets its own RNG. A shared one would
						// itself become the contention point and hide what we
						// are trying to measure.
						rng := rand.New(rand.NewSource(rand.Int63()))

						for pb.Next() {
							key := "key" + strconv.Itoa(rng.Intn(keyspaceSize))
							if rng.Float64() < w.readRatio {
								e.Execute([]string{"GET", key})
							} else {
								e.Execute([]string{"SET", key, "value"})
							}
						}
					})
				})
			}
		}
	}
}

// BenchmarkHotKey is the worst case: every goroutine hits one key. Sharding
// cannot help here, because one key means one shard. Worth measuring so the
// writeup can say honestly where the technique stops working.
func BenchmarkHotKey(b *testing.B) {
	for _, kind := range storeKinds {
		b.Run(string(kind), func(b *testing.B) {
			e := New(benchConfig(kind))
			e.Execute([]string{"SET", "hot", "0"})

			b.SetParallelism(64)
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					e.Execute([]string{"INCR", "hot"})
				}
			})
		})
	}
}

// BenchmarkShardCount finds where adding shards stops paying. Past a point the
// extra locks cost more in memory and cache pressure than they save in
// contention.
func BenchmarkShardCount(b *testing.B) {
	for _, shards := range []int{1, 4, 16, 64, 256, 1024} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			c := DefaultConfig()
			c.Store = StoreSharded
			c.ShardCount = shards
			e := New(c)

			for i := 0; i < keyspaceSize; i++ {
				e.Execute([]string{"SET", "key" + strconv.Itoa(i), "value"})
			}

			b.SetParallelism(64)
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(rand.Int63()))
				for pb.Next() {
					key := "key" + strconv.Itoa(rng.Intn(keyspaceSize))
					if rng.Float64() < 0.9 {
						e.Execute([]string{"GET", key})
					} else {
						e.Execute([]string{"SET", key, "value"})
					}
				}
			})
		})
	}
}

// BenchmarkCommands measures per-command cost with no contention, to separate
// dispatch and allocation overhead from locking overhead.
func BenchmarkCommands(b *testing.B) {
	e := New(DefaultConfig())
	e.Execute([]string{"SET", "k", "value"})
	e.Execute([]string{"RPUSH", "list", "a", "b", "c"})
	e.Execute([]string{"HSET", "hash", "f", "v"})

	commands := map[string][]string{
		"GET":     {"GET", "k"},
		"SET":     {"SET", "k", "value"},
		"INCR":    {"INCR", "counter"},
		"LRANGE":  {"LRANGE", "list", "0", "-1"},
		"HGETALL": {"HGETALL", "hash"},
		"PING":    {"PING"},
	}

	for name, args := range commands {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				e.Execute(args)
			}
		})
	}
}
