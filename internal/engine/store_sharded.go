package engine

// ShardedStore splits the keyspace across N independently locked maps.
//
// The idea: two clients touching unrelated keys almost always land on
// different shards, so they never contend. With one global lock they always
// contend, regardless of whether their keys have anything to do with each
// other.
//
// A shard is just a GlobalStore, which is what makes the comparison honest:
// the two implementations differ only in how many locks there are.
type ShardedStore struct {
	shards []*GlobalStore
	mask   uint32
}

// NewShardedStore builds a store with shardCount shards. shardCount must be a
// power of two, so the shard index is a bitmask rather than a modulo.
func NewShardedStore(clock Clock, shardCount int) *ShardedStore {
	if shardCount <= 0 || shardCount&(shardCount-1) != 0 {
		panic("engine: shard count must be a positive power of two")
	}

	shards := make([]*GlobalStore, shardCount)
	for i := range shards {
		shards[i] = NewGlobalStore(clock)
	}
	return &ShardedStore{
		shards: shards,
		mask:   uint32(shardCount - 1),
	}
}

// fnv1a is a non-cryptographic hash: fast, and good enough spread for keys
// that often share prefixes ("user:1", "user:2").
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

// Len is O(shards) and takes every read lock in turn, so it is not a snapshot
// of a single instant. That is acceptable for a stats counter and would not be
// for anything that needs consistency.
func (s *ShardedStore) Len() int {
	total := 0
	for _, shard := range s.shards {
		total += shard.Len()
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

// ExpireCycle runs a cycle on every shard. Each shard's cycle is itself
// bounded, so the total work per tick stays proportional to shard count rather
// than to keyspace size. A production version would sample shards too.
func (s *ShardedStore) ExpireCycle(sampleSize int, threshold float64) int {
	removed := 0
	for _, shard := range s.shards {
		removed += shard.ExpireCycle(sampleSize, threshold)
	}
	return removed
}
