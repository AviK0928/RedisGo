package engine

import (
	"sync"
	"time"
)

// Kind tags what a key holds, so commands can reject wrong-type operations.
type Kind byte

const (
	KindString Kind = iota
	KindList
	KindHash
)

func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindList:
		return "list"
	case KindHash:
		return "hash"
	default:
		return "unknown"
	}
}

// Entry is one value in the keyspace.
//
// Only the field matching Kind is meaningful. A zero ExpireAt means the key
// never expires, which is why expiry is checked with IsZero rather than
// comparing against a sentinel time.
type Entry struct {
	Kind     Kind
	Str      string
	List     []string
	Hash     map[string]string
	ExpireAt time.Time
}

func (e *Entry) expired(now time.Time) bool {
	return !e.ExpireAt.IsZero() && now.After(e.ExpireAt)
}

// Store is the keyspace. It is not safe for concurrent use on its own; the
// engine holds the lock. Phase 3 replaces this with a sharded implementation,
// so keep the method set small and the locking outside.
type Store struct {
	mu    sync.RWMutex
	data  map[string]*Entry
	clock Clock
}

func NewStore(clock Clock) *Store {
	return &Store{
		data:  make(map[string]*Entry),
		clock: clock,
	}
}

// Get returns the entry for a key, deleting it first if it has expired.
//
// This is lazy expiration: a key that is never read is never noticed. That is
// why activeExpiryCycle exists as well; on its own, lazy expiry leaks memory
// for keys nobody touches again.
func (s *Store) Get(key string) (*Entry, bool) {
	s.mu.RLock()
	entry, found := s.data[key]
	expired := found && entry.expired(s.clock.Now())
	s.mu.RUnlock()

	if !found {
		return nil, false
	}
	if expired {
		s.Delete(key)
		return nil, false
	}
	return entry, true
}

func (s *Store) Set(key string, entry *Entry) {
	s.mu.Lock()
	s.data[key] = entry
	s.mu.Unlock()
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.data[key]; !found {
		return false
	}
	delete(s.data, key)
	return true
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Store) Flush() {
	s.mu.Lock()
	s.data = make(map[string]*Entry)
	s.mu.Unlock()
}

// Keys returns every live key. Fine at this scale; a real KEYS on a large
// keyspace is a known footgun and phase 9 adds SCAN as the cursor-based
// alternative.
func (s *Store) Keys() []string {
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

// activeExpiryCycle is Redis's sampling algorithm: look at a small random
// sample of keys with TTLs, delete the expired ones, and if a large fraction
// of the sample was expired, assume there are more and go again.
//
// It samples rather than scanning because a full scan of a large keyspace
// would block the server. Returns how many keys it removed.
func (s *Store) activeExpiryCycle(sampleSize int, threshold float64) int {
	totalRemoved := 0

	for round := 0; round < 16; round++ { // bounded, so one cycle cannot run away
		now := s.clock.Now()

		s.mu.Lock()
		sampled, expired := 0, make([]string, 0, sampleSize)
		// Go randomises map iteration order, which gives us the random sample
		// for free.
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

		totalRemoved += len(expired)

		if sampled == 0 || float64(len(expired))/float64(sampled) < threshold {
			return totalRemoved
		}
	}
	return totalRemoved
}
