package engine

import (
	"sync"
)

// GlobalStore guards one map with one RWMutex. It is both the baseline the
// sharded store is measured against and the building block it is made of.
type GlobalStore struct {
	mu    sync.RWMutex
	data  map[string]*Entry
	bytes int64
	clock Clock
}

func NewGlobalStore(clock Clock) *GlobalStore {
	return &GlobalStore{
		data:  make(map[string]*Entry),
		clock: clock,
	}
}

func (s *GlobalStore) View(key string, fn func(*Entry)) bool {
	now := s.clock.Now()

	s.mu.RLock()
	entry, found := s.data[key]

	if !found {
		s.mu.RUnlock()
		return false
	}

	// Lazy expiration: a dead key is noticed the moment somebody reads it. On
	// its own this leaks keys nobody reads again, hence ExpireCycle.
	if entry.expired(now) {
		s.mu.RUnlock()
		s.Delete(key)
		return false
	}

	entry.touch(now)
	fn(entry)
	s.mu.RUnlock()
	return true
}

func (s *GlobalStore) Update(key string, fn func(*Entry) *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	current, found := s.data[key]

	if found {
		// Subtract before the callback, because fn may mutate the entry in
		// place and its size afterwards would not match what we added.
		s.bytes -= current.size(key)
		if current.expired(now) {
			delete(s.data, key)
			found = false
		}
	}
	if !found {
		current = nil
	}

	next := fn(current)
	if next == nil {
		delete(s.data, key)
		return
	}

	next.touch(now)
	s.data[key] = next
	s.bytes += next.size(key)
}

func (s *GlobalStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.data[key]
	if !found {
		return false
	}
	s.bytes -= entry.size(key)
	delete(s.data, key)
	return true
}

func (s *GlobalStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *GlobalStore) MemoryUsed() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bytes < 0 {
		return 0
	}
	return s.bytes
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
	s.bytes = 0
	s.mu.Unlock()
}

// Sample relies on Go randomising map iteration order, which makes a random
// sample free. A production store would need an explicit sampling structure.
func (s *GlobalStore) Sample(n int, volatileOnly bool) []Candidate {
	now := s.clock.Now()

	s.mu.RLock()
	defer s.mu.RUnlock()

	candidates := make([]Candidate, 0, n)
	for key, entry := range s.data {
		if volatileOnly && entry.ExpireAt.IsZero() {
			continue
		}
		candidates = append(candidates, Candidate{
			Key:      key,
			Idle:     entry.idle(now),
			Freq:     entry.decayedFreq(now),
			ExpireAt: entry.ExpireAt,
			Size:     entry.size(key),
		})
		if len(candidates) >= n {
			break
		}
	}
	return candidates
}

// ExpireCycle is Redis's sampling algorithm: inspect a small random sample of
// keys that carry TTLs, delete the expired ones, and if a large fraction of
// the sample was dead, assume more are waiting and go again.
//
// It samples rather than scanning because a full sweep of a large keyspace
// would block every other operation.
func (s *GlobalStore) ExpireCycle(sampleSize int, threshold float64) int {
	removed := 0

	for round := 0; round < 16; round++ { // bounded, so a cycle cannot run away
		now := s.clock.Now()

		s.mu.Lock()
		sampled := 0
		expired := make([]string, 0, sampleSize)

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
			if entry, found := s.data[key]; found {
				s.bytes -= entry.size(key)
				delete(s.data, key)
			}
		}
		s.mu.Unlock()

		removed += len(expired)

		if sampled == 0 || float64(len(expired))/float64(sampled) < threshold {
			return removed
		}
	}
	return removed
}

func (s *GlobalStore) Snapshot(fn func(string, *Entry)) {
	now := s.clock.Now()

	s.mu.RLock()
	defer s.mu.RUnlock()

	for key, entry := range s.data {
		if !entry.expired(now) {
			fn(key, entry)
		}
	}
}

func (s *GlobalStore) Restore(key string, entry *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, found := s.data[key]; found {
		s.bytes -= existing.size(key)
	}
	entry.touch(s.clock.Now())
	s.data[key] = entry
	s.bytes += entry.size(key)
}
