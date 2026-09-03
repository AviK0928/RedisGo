package engine

import "time"

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

// Entry is one value in the keyspace. Only the field matching Kind is
// meaningful. A zero ExpireAt means the key never expires.
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

// Store is the keyspace.
//
// The callback shape is deliberate. Phase 2 returned *Entry to callers, who
// then mutated it after the lock had been released, so two clients doing INCR
// on the same key could lose an update. Here every read runs inside View and
// every write inside Update, both of which hold the relevant lock for the
// duration of the callback, so a read-modify-write is atomic.
//
// The contract for implementations: never call a user callback while holding
// a lock the callback could try to take again.
type Store interface {
	// View runs fn under a read lock if the key holds a live entry. It
	// reports whether the key was found. fn must not mutate the entry.
	View(key string, fn func(*Entry)) bool

	// Update runs fn under a write lock. fn receives the current entry, or
	// nil if the key is absent or expired, and returns the entry to store.
	// Returning nil deletes the key.
	Update(key string, fn func(current *Entry) *Entry)

	Delete(key string) bool
	Len() int
	Keys() []string
	Flush()

	// ExpireCycle removes expired keys without anyone reading them, and
	// reports how many it removed.
	ExpireCycle(sampleSize int, threshold float64) int
}
