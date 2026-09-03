package engine

import "sync"

// GlobalStore guards one map with one RWMutex.
//
// This is the baseline the sharded implementation is measured against, and it
// is also the building block ShardedStore is made of: a shard is just one of
// these holding a slice of the keyspace.
type GlobalStore struct {
	mu    sync.RWMutex
	data  map[string]*Entry
	clock Clock
}

func NewGlobalStore(clock Clock) *GlobalStore {
	return &GlobalStore{
		data:  make(map[string]*Entry),
		clock: clock,
	}
}

func (s *GlobalStore) View(key string, fn func(*Entry)) bool {
	s.mu.RLock()
	entry, found := s.data[key]

	if !found {
		s.mu.RUnlock()
		return false
	}

	// Lazy expiration: a key is noticed as dead the moment someone reads it.
	// On its own this leaks keys nobody reads again, which is why
	// ExpireCycle exists too.
	if entry.expired(s.clock.Now()) {
		s.mu.RUnlock()
		s.Delete(key)
		return false
	}

	fn(entry)
	s.mu.RUnlock()
	return true
}

func (s *GlobalStore) Update(key string, fn func(*Entry) *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, found := s.data[key]
	if found && current.expired(s.clock.Now()) {
		delete(s.data, key)
		found = false
	}
	if !found {
		current = nil
	}

	next := fn(current)
	if next == nil {
		delete(s.data, key)
		return
	}
	s.data[key] = next
}

func (s *GlobalStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, found := s.data[key]; !found {
		return false
	}
	delete(s.data, key)
	return true
}

func (s *GlobalStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *GlobalStore) Keys() []string {
	now := s.clock.Now()

	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for key, entry := range s.data {
		if !entry.expired(now) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (s *GlobalStore) Flush() {
	s.mu.Lock()
	s.data = make(map[string]*Entry)
	s.mu.Unlock()
}

// ExpireCycle is Redis's sampling algorithm: inspect a small random sample of
// keys that have TTLs, delete the expired ones, and if a large fraction of the
// sample was expired, assume more are waiting and go again.
//
// It samples rather than scanning because a full sweep of a large keyspace
// would block every other operation.
func (s *GlobalStore) ExpireCycle(sampleSize int, threshold float64) int {
	removed := 0

	for round := 0; round < 16; round++ { // bounded, so one cycle cannot run away
		now := s.clock.Now()

		s.mu.Lock()
		sampled := 0
		expired := make([]string, 0, sampleSize)

		// Go randomises map iteration order, which gives a random sample free.
		for key, entry := range s.data {
			if entry.ExpireAt.IsZero() {
				continue // no TTL, not a candidate
			}
			sampled++
			if entry.expired(now) {
				expired = append(expired, key)
			}
			if sampled >= sampleSize {
				break
			}
		}
		for _, key := range expired {
			delete(s.data, key)
		}
		s.mu.Unlock()

		removed += len(expired)

		if sampled == 0 || float64(len(expired))/float64(sampled) < threshold {
			return removed
		}
	}
	return removed
}
