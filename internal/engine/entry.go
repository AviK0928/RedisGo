package engine

import (
	"math/rand/v2"
	"sync/atomic"
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

// lfuLogFactor controls how quickly the frequency counter saturates. Higher
// means slower growth, so the 8-bit counter can represent a wider range of
// access rates.
const lfuLogFactor = 10.0

// lfuDecayMinutes is how long a key goes untouched before its frequency
// counter drops by one. Without decay, a key that was hot last week would
// outrank one that is hot now, forever.
const lfuDecayMinutes = 1

// Entry is one value in the keyspace. Only the field matching Kind is
// meaningful. A zero ExpireAt means the key never expires.
//
// lastAccess and freq are atomics because they are written during reads, which
// hold only a read lock. Redis does the same thing: tracking recency is itself
// a mutation, and making every GET take a write lock would defeat the point.
type Entry struct {
	Kind     Kind
	Str      string
	List     []string
	Hash     map[string]string
	ExpireAt time.Time

	lastAccess atomic.Int64  // unix nanoseconds
	freq       atomic.Uint32 // 8-bit logarithmic counter, 0-255
}

func (e *Entry) expired(now time.Time) bool {
	return !e.ExpireAt.IsZero() && now.After(e.ExpireAt)
}

// touch records an access for both eviction policies at once.
func (e *Entry) touch(now time.Time) {
	e.lastAccess.Store(now.UnixNano())
	e.bumpFreq()
}

// bumpFreq increments the LFU counter probabilistically, so that eight bits
// can track access counts spanning several orders of magnitude. A key accessed
// ten times and one accessed ten million times need to be distinguishable, and
// a plain counter would saturate almost immediately.
func (e *Entry) bumpFreq() {
	for {
		current := e.freq.Load()
		if current >= 255 {
			return
		}
		// The higher the counter already is, the less likely another bump.
		if rand.Float64() > 1.0/(float64(current)*lfuLogFactor+1) {
			return
		}
		if e.freq.CompareAndSwap(current, current+1) {
			return
		}
		// CAS lost a race; another goroutine bumped it. Retry.
	}
}

// decayedFreq is the counter adjusted for how long the key has been idle.
func (e *Entry) decayedFreq(now time.Time) uint32 {
	current := e.freq.Load()
	idleMinutes := int64(e.idle(now).Minutes()) / lfuDecayMinutes

	if idleMinutes <= 0 || current == 0 {
		return current
	}
	if int64(current) <= idleMinutes {
		return 0
	}
	return current - uint32(idleMinutes)
}

func (e *Entry) idle(now time.Time) time.Duration {
	last := e.lastAccess.Load()
	if last == 0 {
		return 0
	}
	return now.Sub(time.Unix(0, last))
}

// size approximates the memory an entry occupies, including its key.
//
// Deliberately an estimate. Measuring real Go heap usage per object would need
// reflection or runtime introspection on every write, which would cost more
// than the accuracy is worth for an eviction trigger.
func (e *Entry) size(key string) int64 {
	// Map bucket slot, pointer, Entry header, and the key's own string header.
	const perKeyOverhead = 64

	total := int64(len(key)) + perKeyOverhead

	switch e.Kind {
	case KindString:
		total += int64(len(e.Str))
	case KindList:
		for _, item := range e.List {
			total += int64(len(item)) + 16 // string header per element
		}
	case KindHash:
		for field, value := range e.Hash {
			total += int64(len(field)+len(value)) + 48 // two headers plus bucket
		}
	}
	return total
}
