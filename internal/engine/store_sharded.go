package engine

import "math/rand/v2"

// ShardedStore splits the keyspace across N independently locked maps, so two
// clients touching unrelated keys almost never contend. A shard is itself a
// GlobalStore, which is what makes the benchmark comparison honest: the two
// implementations differ only in how many locks exist.
type ShardedStore struct {
	shards []*GlobalStore
	mask   uint32
}

// NewShardedStore builds a store with shardCount shards, which must be a power
// of two so the shard index is a bitmask rather than a modulo.
func NewShardedStore(clock Clock, shardCount int) *ShardedStore {
	if shardCount <= 0 || shardCount&(shardCount-1) != 0 {
		panic("engine: shard count must be a positive power of two")
	}

	shards := make([]*GlobalStore, shardCount)
	for i := range shards {
		shards[i] = NewGlobalStore(clock)
	}
	return &ShardedStore{shards: shards, mask: uint32(shardCount - 1)}
}

// fnv1a is a non-cryptographic hash: fast, and it spreads keys that share
// prefixes ("user:1", "user:2") across shards rather than clustering them.
func fnv1a(key string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return h
}

func (s *ShardedStore) shard(key string) *GlobalStore {
	return s.shards[fnv1a(key)&s.mask]
}

func (s *ShardedStore) View(key string, fn func(*Entry)) bool {
	return s.shard(key).View(key, fn)
}

func (s *ShardedStore) Update(key string, fn func(*Entry) *Entry) {
	s.shard(key).Update(key, fn)
}

func (s *ShardedStore) Delete(key string) bool {
	return s.shard(key).Delete(key)
}

// Len walks every shard, so it is not a snapshot of one instant. Fine for a
// stats counter, not fine for anything needing consistency.
func (s *ShardedStore) Len() int {
	total := 0
	for _, shard := range s.shards {
		total += shard.Len()
	}
	return total
}

func (s *ShardedStore) MemoryUsed() int64 {
	var total int64
	for _, shard := range s.shards {
		total += shard.MemoryUsed()
	}
	return total
}

func (s *ShardedStore) Keys() []string {
	var keys []string
	for _, shard := range s.shards {
		keys = append(keys, shard.Keys()...)
	}
	return keys
}

func (s *ShardedStore) Flush() {
	for _, shard := range s.shards {
		shard.Flush()
	}
}

// Sample draws one candidate from each of n randomly chosen shards. Sampling
// every shard would be O(shards) per eviction, which at 256 shards would make
// eviction more expensive than the work it protects.
func (s *ShardedStore) Sample(n int, volatileOnly bool) []Candidate {
	candidates := make([]Candidate, 0, n)

	for i := 0; i < n; i++ {
		shard := s.shards[rand.IntN(len(s.shards))]
		candidates = append(candidates, shard.Sample(1, volatileOnly)...)
	}
	return candidates
}

func (s *ShardedStore) ExpireCycle(sampleSize int, threshold float64) int {
	removed := 0
	for _, shard := range s.shards {
		removed += shard.ExpireCycle(sampleSize, threshold)
	}
	return removed
}
