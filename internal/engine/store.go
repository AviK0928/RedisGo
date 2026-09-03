package engine

import "time"

// Candidate is one key offered to the eviction policy, with the metadata the
// policy needs to judge it. Sampling returns these rather than entries so the
// policy never holds a reference into the store after the lock is released.
type Candidate struct {
	Key      string
	Idle     time.Duration
	Freq     uint32
	ExpireAt time.Time
	Size     int64
}

// Store is the keyspace.
//
// Every read runs inside View and every write inside Update, both of which
// hold the relevant lock for the duration of the callback. That makes a
// read-modify-write such as INCR atomic; an earlier design returned *Entry to
// callers and lost updates under contention.
type Store interface {
	View(key string, fn func(*Entry)) bool
	Update(key string, fn func(current *Entry) *Entry)

	Delete(key string) bool
	Len() int
	Keys() []string
	Flush()

	ExpireCycle(sampleSize int, threshold float64) int

	// MemoryUsed is the approximate size of everything held.
	MemoryUsed() int64

	// Sample returns up to n randomly chosen keys for the eviction policy to
	// judge. volatileOnly restricts it to keys that have a TTL.
	Sample(n int, volatileOnly bool) []Candidate
}
