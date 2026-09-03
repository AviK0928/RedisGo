package engine

import (
	"strconv"
	"strings"
	"time"

	"github.com/AviK0928/RedisGo/internal/resp"
)

// --- connection ---

func cmdPing(e *Engine, args []string) resp.Value {
	if len(args) > 2 {
		return resp.Err("ERR wrong number of arguments for 'ping' command")
	}
	if len(args) == 2 {
		return resp.BulkString(args[1])
	}
	return resp.Simple("PONG")
}

func cmdEcho(e *Engine, args []string) resp.Value {
	return resp.BulkString(args[1])
}

// cmdCommand is a stub. redis-cli sends COMMAND DOCS on connect and waits for
// a reply before showing a prompt, so without this the client appears to hang.
func cmdCommand(e *Engine, args []string) resp.Value {
	return resp.Arr()
}

// --- strings ---

// cmdSet handles SET key value [EX s] [PX ms] [NX|XX].
//
// The NX and XX checks happen inside the update callback, not before it. SET NX
// is the primitive people build distributed locks on, so "does the key exist"
// and "write the key" have to be one atomic step. Checking first and writing
// after would let two clients both believe they acquired the lock.
func cmdSet(e *Engine, args []string) resp.Value {
	key, value := args[1], args[2]

	var ttl time.Duration
	var onlyIfAbsent, onlyIfPresent bool

	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "EX", "PX":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || n <= 0 {
				return resp.Err("ERR invalid expire time in 'set' command")
			}
			if strings.EqualFold(args[i], "EX") {
				ttl = time.Duration(n) * time.Second
			} else {
				ttl = time.Duration(n) * time.Millisecond
			}
			i++
		case "NX":
			onlyIfAbsent = true
		case "XX":
			onlyIfPresent = true
		default:
			return resp.Err("ERR syntax error")
		}
	}

	if onlyIfAbsent && onlyIfPresent {
		return resp.Err("ERR syntax error")
	}

	var reply resp.Value
	e.store.Update(key, func(current *Entry) *Entry {
		if onlyIfAbsent && current != nil {
			reply = resp.NullBulk()
			return current
		}
		if onlyIfPresent && current == nil {
			reply = resp.NullBulk()
			return nil
		}

		entry := &Entry{Kind: KindString, Str: value}
		if ttl > 0 {
			entry.ExpireAt = e.clock.Now().Add(ttl)
		}
		reply = resp.Simple("OK")
		return entry
	})
	return reply
}

func cmdGet(e *Engine, args []string) resp.Value {
	return e.viewTyped(args[1], KindString, resp.NullBulk(), func(entry *Entry) resp.Value {
		return resp.BulkString(entry.Str)
	})
}

func cmdSetNX(e *Engine, args []string) resp.Value {
	var reply resp.Value
	e.store.Update(args[1], func(current *Entry) *Entry {
		if current != nil {
			reply = resp.Int(0)
			return current
		}
		reply = resp.Int(1)
		return &Entry{Kind: KindString, Str: args[2]}
	})
	return reply
}

func cmdSetEX(e *Engine, args []string) resp.Value {
	seconds, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || seconds <= 0 {
		return resp.Err("ERR invalid expire time in 'setex' command")
	}

	e.store.Update(args[1], func(current *Entry) *Entry {
		return &Entry{
			Kind:     KindString,
			Str:      args[3],
			ExpireAt: e.clock.Now().Add(time.Duration(seconds) * time.Second),
		}
	})
	return resp.Simple("OK")
}

func cmdAppend(e *Engine, args []string) resp.Value {
	return e.updateTyped(args[1], KindString, func(current *Entry) (*Entry, resp.Value) {
		if current == nil {
			return &Entry{Kind: KindString, Str: args[2]}, resp.Int(int64(len(args[2])))
		}
		current.Str += args[2]
		return current, resp.Int(int64(len(current.Str)))
	})
}

func cmdStrlen(e *Engine, args []string) resp.Value {
	return e.viewTyped(args[1], KindString, resp.Int(0), func(entry *Entry) resp.Value {
		return resp.Int(int64(len(entry.Str)))
	})
}

func cmdIncr(e *Engine, args []string) resp.Value { return incrBy(e, args[1], 1) }
func cmdDecr(e *Engine, args []string) resp.Value { return incrBy(e, args[1], -1) }

func cmdIncrBy(e *Engine, args []string) resp.Value {
	delta, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	return incrBy(e, args[1], delta)
}

func cmdDecrBy(e *Engine, args []string) resp.Value {
	delta, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	return incrBy(e, args[1], -delta)
}

// incrBy is the shared body of INCR, DECR, INCRBY and DECRBY. Redis stores
// counters as strings and parses on each operation, which is why a value set
// by SET can be incremented.
//
// This is the read-modify-write that phase 2 got wrong: the parse, the add and
// the store all have to happen without another client slipping in between.
func incrBy(e *Engine, key string, delta int64) resp.Value {
	return e.updateTyped(key, KindString, func(current *Entry) (*Entry, resp.Value) {
		var value int64
		if current != nil {
			parsed, err := strconv.ParseInt(current.Str, 10, 64)
			if err != nil {
				return current, resp.Err("ERR value is not an integer or out of range")
			}
			value = parsed
		}

		value += delta
		text := strconv.FormatInt(value, 10)

		if current == nil {
			return &Entry{Kind: KindString, Str: text}, resp.Int(value)
		}
		current.Str = text
		return current, resp.Int(value)
	})
}

func cmdMGet(e *Engine, args []string) resp.Value {
	values := make([]resp.Value, 0, len(args)-1)
	for _, key := range args[1:] {
		// Redis returns nil rather than an error for a wrong-type key in MGET.
		values = append(values, e.viewTyped(key, KindString, resp.NullBulk(), func(entry *Entry) resp.Value {
			return resp.BulkString(entry.Str)
		}))
	}
	return resp.Arr(values...)
}

func cmdMSet(e *Engine, args []string) resp.Value {
	pairs := args[1:]
	if len(pairs)%2 != 0 {
		return resp.Err("ERR wrong number of arguments for 'mset' command")
	}
	for i := 0; i < len(pairs); i += 2 {
		value := pairs[i+1]
		e.store.Update(pairs[i], func(current *Entry) *Entry {
			return &Entry{Kind: KindString, Str: value}
		})
	}
	return resp.Simple("OK")
}

// --- keyspace ---

func cmdDel(e *Engine, args []string) resp.Value {
	deleted := 0
	for _, key := range args[1:] {
		if e.store.Delete(key) {
			deleted++
		}
	}
	return resp.Int(int64(deleted))
}

func cmdExists(e *Engine, args []string) resp.Value {
	count := 0
	for _, key := range args[1:] {
		if e.store.View(key, func(*Entry) {}) {
			count++
		}
	}
	return resp.Int(int64(count))
}

func cmdType(e *Engine, args []string) resp.Value {
	kind := "none"
	e.store.View(args[1], func(entry *Entry) {
		kind = entry.Kind.String()
	})
	return resp.Simple(kind)
}

// cmdKeys supports only the * pattern for now. Full glob matching arrives with
// SCAN in a later phase.
func cmdKeys(e *Engine, args []string) resp.Value {
	if args[1] != "*" {
		return resp.Err("ERR only the '*' pattern is supported")
	}
	keys := e.store.Keys()
	values := make([]resp.Value, 0, len(keys))
	for _, key := range keys {
		values = append(values, resp.BulkString(key))
	}
	return resp.Arr(values...)
}

func cmdDBSize(e *Engine, args []string) resp.Value {
	return resp.Int(int64(e.store.Len()))
}

func cmdFlushAll(e *Engine, args []string) resp.Value {
	e.store.Flush()
	return resp.Simple("OK")
}

// --- expiry ---

func cmdExpire(e *Engine, args []string) resp.Value {
	return setExpiry(e, args[1], args[2], time.Second)
}

func cmdPExpire(e *Engine, args []string) resp.Value {
	return setExpiry(e, args[1], args[2], time.Millisecond)
}

func setExpiry(e *Engine, key, raw string, unit time.Duration) resp.Value {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	var reply resp.Value
	e.store.Update(key, func(current *Entry) *Entry {
		if current == nil {
			reply = resp.Int(0)
			return nil
		}
		// A non-positive TTL deletes the key immediately, as Redis does.
		if n <= 0 {
			reply = resp.Int(1)
			return nil
		}
		current.ExpireAt = e.clock.Now().Add(time.Duration(n) * unit)
		reply = resp.Int(1)
		return current
	})
	return reply
}

func cmdTTL(e *Engine, args []string) resp.Value  { return ttl(e, args[1], time.Second) }
func cmdPTTL(e *Engine, args []string) resp.Value { return ttl(e, args[1], time.Millisecond) }

// ttl returns -2 when the key does not exist and -1 when it exists but has no
// expiry. Those sentinels are part of the protocol, not error handling.
func ttl(e *Engine, key string, unit time.Duration) resp.Value {
	reply := resp.Int(-2)
	e.store.View(key, func(entry *Entry) {
		if entry.ExpireAt.IsZero() {
			reply = resp.Int(-1)
			return
		}
		reply = resp.Int(int64(entry.ExpireAt.Sub(e.clock.Now()) / unit))
	})
	return reply
}

func cmdPersist(e *Engine, args []string) resp.Value {
	var reply resp.Value
	e.store.Update(args[1], func(current *Entry) *Entry {
		if current == nil || current.ExpireAt.IsZero() {
			reply = resp.Int(0)
			return current
		}
		current.ExpireAt = time.Time{}
		reply = resp.Int(1)
		return current
	})
	return reply
}

// --- lists ---

func cmdLPush(e *Engine, args []string) resp.Value { return push(e, args, true) }
func cmdRPush(e *Engine, args []string) resp.Value { return push(e, args, false) }

func push(e *Engine, args []string, left bool) resp.Value {
	return e.updateTyped(args[1], KindList, func(current *Entry) (*Entry, resp.Value) {
		if current == nil {
			current = &Entry{Kind: KindList}
		}
		for _, value := range args[2:] {
			if left {
				current.List = append([]string{value}, current.List...)
			} else {
				current.List = append(current.List, value)
			}
		}
		return current, resp.Int(int64(len(current.List)))
	})
}

func cmdLPop(e *Engine, args []string) resp.Value { return pop(e, args[1], true) }
func cmdRPop(e *Engine, args []string) resp.Value { return pop(e, args[1], false) }

func pop(e *Engine, key string, left bool) resp.Value {
	return e.updateTyped(key, KindList, func(current *Entry) (*Entry, resp.Value) {
		if current == nil || len(current.List) == 0 {
			return nil, resp.NullBulk()
		}

		var value string
		if left {
			value, current.List = current.List[0], current.List[1:]
		} else {
			last := len(current.List) - 1
			value, current.List = current.List[last], current.List[:last]
		}

		// Redis deletes a collection key once it is empty.
		if len(current.List) == 0 {
			return nil, resp.BulkString(value)
		}
		return current, resp.BulkString(value)
	})
}

func cmdLLen(e *Engine, args []string) resp.Value {
	return e.viewTyped(args[1], KindList, resp.Int(0), func(entry *Entry) resp.Value {
		return resp.Int(int64(len(entry.List)))
	})
}

// cmdLRange supports negative indexes, where -1 is the last element.
func cmdLRange(e *Engine, args []string) resp.Value {
	start, err1 := strconv.Atoi(args[2])
	stop, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	return e.viewTyped(args[1], KindList, resp.Arr(), func(entry *Entry) resp.Value {
		length := len(entry.List)

		from, to := start, stop
		if from < 0 {
			from += length
		}
		if to < 0 {
			to += length
		}
		if from < 0 {
			from = 0
		}
		if to >= length {
			to = length - 1
		}
		if from > to || from >= length {
			return resp.Arr()
		}

		values := make([]resp.Value, 0, to-from+1)
		for _, item := range entry.List[from : to+1] {
			values = append(values, resp.BulkString(item))
		}
		return resp.Arr(values...)
	})
}

// --- hashes ---

func cmdHSet(e *Engine, args []string) resp.Value {
	pairs := args[2:]
	if len(pairs)%2 != 0 {
		return resp.Err("ERR wrong number of arguments for 'hset' command")
	}

	return e.updateTyped(args[1], KindHash, func(current *Entry) (*Entry, resp.Value) {
		if current == nil {
			current = &Entry{Kind: KindHash, Hash: make(map[string]string)}
		}

		added := 0
		for i := 0; i < len(pairs); i += 2 {
			if _, exists := current.Hash[pairs[i]]; !exists {
				added++
			}
			current.Hash[pairs[i]] = pairs[i+1]
		}
		return current, resp.Int(int64(added))
	})
}

func cmdHGet(e *Engine, args []string) resp.Value {
	return e.viewTyped(args[1], KindHash, resp.NullBulk(), func(entry *Entry) resp.Value {
		value, exists := entry.Hash[args[2]]
		if !exists {
			return resp.NullBulk()
		}
		return resp.BulkString(value)
	})
}

func cmdHDel(e *Engine, args []string) resp.Value {
	return e.updateTyped(args[1], KindHash, func(current *Entry) (*Entry, resp.Value) {
		if current == nil {
			return nil, resp.Int(0)
		}

		deleted := 0
		for _, field := range args[2:] {
			if _, exists := current.Hash[field]; exists {
				delete(current.Hash, field)
				deleted++
			}
		}

		if len(current.Hash) == 0 {
			return nil, resp.Int(int64(deleted))
		}
		return current, resp.Int(int64(deleted))
	})
}

func cmdHLen(e *Engine, args []string) resp.Value {
	return e.viewTyped(args[1], KindHash, resp.Int(0), func(entry *Entry) resp.Value {
		return resp.Int(int64(len(entry.Hash)))
	})
}

func cmdHExists(e *Engine, args []string) resp.Value {
	return e.viewTyped(args[1], KindHash, resp.Int(0), func(entry *Entry) resp.Value {
		if _, exists := entry.Hash[args[2]]; exists {
			return resp.Int(1)
		}
		return resp.Int(0)
	})
}

func cmdHGetAll(e *Engine, args []string) resp.Value {
	return e.viewTyped(args[1], KindHash, resp.Arr(), func(entry *Entry) resp.Value {
		values := make([]resp.Value, 0, len(entry.Hash)*2)
		for field, value := range entry.Hash {
			values = append(values, resp.BulkString(field), resp.BulkString(value))
		}
		return resp.Arr(values...)
	})
}
