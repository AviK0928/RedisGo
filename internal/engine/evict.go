package engine

import "strings"

// Policy decides which key dies when memory runs out.
type Policy uint32

const (
	// NoEviction refuses writes instead of discarding data. The right choice
	// when the store is a source of truth rather than a cache.
	NoEviction Policy = iota

	AllKeysLRU
	AllKeysLFU
	AllKeysRandom

	// The volatile variants only consider keys that carry a TTL, on the
	// principle that a key with an expiry was always meant to be temporary.
	VolatileLRU
	VolatileTTL
	VolatileRandom
)

func (p Policy) String() string {
	switch p {
	case AllKeysLRU:
		return "allkeys-lru"
	case AllKeysLFU:
		return "allkeys-lfu"
	case AllKeysRandom:
		return "allkeys-random"
	case VolatileLRU:
		return "volatile-lru"
	case VolatileTTL:
		return "volatile-ttl"
	case VolatileRandom:
		return "volatile-random"
	default:
		return "noeviction"
	}
}

func ParsePolicy(s string) (Policy, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "noeviction":
		return NoEviction, true
	case "allkeys-lru":
		return AllKeysLRU, true
	case "allkeys-lfu":
		return AllKeysLFU, true
	case "allkeys-random":
		return AllKeysRandom, true
	case "volatile-lru":
		return VolatileLRU, true
	case "volatile-ttl":
		return VolatileTTL, true
	case "volatile-random":
		return VolatileRandom, true
	default:
		return NoEviction, false
	}
}

// volatileOnly reports whether the policy may only evict keys with a TTL.
func (p Policy) volatileOnly() bool {
	return p == VolatileLRU || p == VolatileTTL || p == VolatileRandom
}

// pickVictim chooses the worst key in a sample.
//
// This is approximated LRU, and the approximation is the interesting part.
// True LRU needs a linked list threaded through every entry, which costs two
// pointers per key and a list update on every read. Sampling a handful of keys
// and evicting the worst of them gets very close to the same hit rate for none
// of that memory. Redis made the same trade.
func pickVictim(candidates []Candidate, policy Policy) (string, bool) {
	if len(candidates) == 0 {
		return "", false
	}

	best := candidates[0]

	for _, c := range candidates[1:] {
		switch policy {
		case AllKeysLRU, VolatileLRU:
			if c.Idle > best.Idle {
				best = c
			}

		case AllKeysLFU:
			// Least frequently used, with idle time as the tiebreak so two
			// cold keys are separated by recency.
			if c.Freq < best.Freq || (c.Freq == best.Freq && c.Idle > best.Idle) {
				best = c
			}

		case VolatileTTL:
			// The key closest to dying anyway is the cheapest to lose.
			if !c.ExpireAt.IsZero() && (best.ExpireAt.IsZero() || c.ExpireAt.Before(best.ExpireAt)) {
				best = c
			}

		case AllKeysRandom, VolatileRandom:
			// The sample is already random, so the first element will do.
			return best.Key, true
		}
	}

	return best.Key, true
}
