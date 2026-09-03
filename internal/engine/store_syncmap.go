package engine

import (
	"sync"
	"sync/atomic"
)

// SyncMapStore uses sync.Map for storage.
//
// Worth measuring because sync.Map is often reached for reflexively, but it is
// built for one shape: keys written once and read many times. It also cannot
// express an atomic read-modify-write, which a cache needs for INCR and SET NX,
// so writes still take a mutex here. The only thing it buys is lock-free reads.
type SyncMapStore struct {
	data    sync.Map // string -> *Entry
	writeMu sync.Mutex
	bytes   atomic.Int64
	clock   Clock
}

func NewSyncMapStore(clock Clock) *SyncMapStore {
	return &SyncMapStore{clock: clock}
}

func (s *SyncMapStore) View(key string, fn func(*Entry)) bool {
	value, found := s.data.Load(key)
	if !found {
		return false
	}

	now := s.clock.Now()
	entry := value.(*Entry)

	if entry.expired(now) {
		s.Delete(key)
		return false
	}

	entry.touch(now)
	fn(entry)
	return true
}

func (s *SyncMapStore) Update(key string, fn func(*Entry) *Entry) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	now := s.clock.Now()
	var current *Entry

	if value, found := s.data.Load(key); found {
		entry := value.(*Entry)
		s.bytes.Add(-entry.size(key))
		if entry.expired(now) {
			s.data.Delete(key)
		} else {
			current = entry
		}
	}

	next := fn(current)
	if next == nil {
		s.data.Delete(key)
		return
	}

	next.touch(now)
	s.data.Store(key, next)
	s.bytes.Add(next.size(key))
}

func (s *SyncMapStore) Delete(key string) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	value, found := s.data.Load(key)
	if !found {
		return false
	}
	s.bytes.Add(-value.(*Entry).size(key))
	s.data.Delete(key)
	return true
}

// Len has no shortcut: sync.Map keeps no count, so this walks everything. That
// is a real cost the map-plus-mutex implementations do not pay.
func (s *SyncMapStore) Len() int {
	count := 0
	s.data.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func (s *SyncMapStore) MemoryUsed() int64 {
	if n := s.bytes.Load(); n > 0 {
		return n
	}
	return 0
}

func (s *SyncMapStore) Keys() []string {
	now := s.clock.Now()

	var keys []string
	s.data.Range(func(key, value any) bool {
		if !value.(*Entry).expired(now) {
			keys = append(keys, key.(string))
		}
		return true
	})
	return keys
}

func (s *SyncMapStore) Flush() {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.data.Range(func(key, _ any) bool {
		s.data.Delete(key)
		return true
	})
	s.bytes.Store(0)
}

func (s *SyncMapStore) Sample(n int, volatileOnly bool) []Candidate {
	now := s.clock.Now()

	candidates := make([]Candidate, 0, n)
	s.data.Range(func(key, value any) bool {
		entry := value.(*Entry)
		if volatileOnly && entry.ExpireAt.IsZero() {
			return true
		}
		candidates = append(candidates, Candidate{
			Key:      key.(string),
			Idle:     entry.idle(now),
			Freq:     entry.decayedFreq(now),
			ExpireAt: entry.ExpireAt,
			Size:     entry.size(key.(string)),
		})
		return len(candidates) < n
	})
	return candidates
}

func (s *SyncMapStore) ExpireCycle(sampleSize int, threshold float64) int {
	removed := 0

	for round := 0; round < 16; round++ {
		now := s.clock.Now()

		s.writeMu.Lock()
		sampled := 0
		expired := make([]string, 0, sampleSize)

		s.data.Range(func(key, value any) bool {
			entry := value.(*Entry)
			if entry.ExpireAt.IsZero() {
				return true
			}
			sampled++
			if entry.expired(now) {
				expired = append(expired, key.(string))
			}
			return sampled < sampleSize
		})

		for _, key := range expired {
			if value, found := s.data.Load(key); found {
				s.bytes.Add(-value.(*Entry).size(key))
				s.data.Delete(key)
			}
		}
		s.writeMu.Unlock()

		removed += len(expired)

		if sampled == 0 || float64(len(expired))/float64(sampled) < threshold {
			return removed
		}
	}
	return removed
}

func (s *SyncMapStore) Snapshot(fn func(string, *Entry)) {
	now := s.clock.Now()

	s.data.Range(func(key, value any) bool {
		entry := value.(*Entry)
		if !entry.expired(now) {
			fn(key.(string), entry)
		}
		return true
	})
}

func (s *SyncMapStore) Restore(key string, entry *Entry) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if existing, found := s.data.Load(key); found {
		s.bytes.Add(-existing.(*Entry).size(key))
	}
	entry.touch(s.clock.Now())
	s.data.Store(key, entry)
	s.bytes.Add(entry.size(key))
}
