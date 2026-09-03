package engine

import "sync"

// SyncMapStore uses sync.Map for storage.
//
// Worth measuring because sync.Map is often reached for reflexively, but it
// is built for a specific shape: keys written once and read many times. It
// also cannot express an atomic read-modify-write, which a cache needs for
// INCR and for SET NX. So writes still take a mutex here, and the only thing
// sync.Map buys is lock-free reads. Expect it to win on read-heavy mixes and
// lose on write-heavy ones.
type SyncMapStore struct {
	data    sync.Map // string -> *Entry
	writeMu sync.Mutex
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

	entry := value.(*Entry)
	if entry.expired(s.clock.Now()) {
		s.Delete(key)
		return false
	}

	fn(entry)
	return true
}

func (s *SyncMapStore) Update(key string, fn func(*Entry) *Entry) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var current *Entry
	if value, found := s.data.Load(key); found {
		entry := value.(*Entry)
		if entry.expired(s.clock.Now()) {
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
	s.data.Store(key, next)
}

func (s *SyncMapStore) Delete(key string) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if _, found := s.data.Load(key); !found {
		return false
	}
	s.data.Delete(key)
	return true
}

// Len has no shortcut here: sync.Map keeps no count, so this walks everything.
// That is a real cost the map-plus-mutex implementations do not pay.
func (s *SyncMapStore) Len() int {
	count := 0
	s.data.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
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
			s.data.Delete(key)
		}
		s.writeMu.Unlock()

		removed += len(expired)

		if sampled == 0 || float64(len(expired))/float64(sampled) < threshold {
			return removed
		}
	}
	return removed
}
