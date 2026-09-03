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

	_, exists := e.store.Get(key)
	if onlyIfAbsent && exists {
		return resp.NullBulk()
	}
	if onlyIfPresent && !exists {
		return resp.NullBulk()
	}

	entry := &Entry{Kind: KindString, Str: value}
	if ttl > 0 {
		entry.ExpireAt = e.clock.Now().Add(ttl)
	}
	e.store.Set(key, entry)

	return resp.Simple("OK")
}

func cmdGet(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindString)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		return resp.NullBulk()
	}
	return resp.BulkString(entry.Str)
}

func cmdSetNX(e *Engine, args []string) resp.Value {
	if _, found := e.store.Get(args[1]); found {
		return resp.Int(0)
	}
	e.store.Set(args[1], &Entry{Kind: KindString, Str: args[2]})
	return resp.Int(1)
}

func cmdSetEX(e *Engine, args []string) resp.Value {
	seconds, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || seconds <= 0 {
		return resp.Err("ERR invalid expire time in 'setex' command")
	}
	e.store.Set(args[1], &Entry{
		Kind:     KindString,
		Str:      args[3],
		ExpireAt: e.clock.Now().Add(time.Duration(seconds) * time.Second),
	})
	return resp.Simple("OK")
}

func cmdAppend(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindString)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		e.store.Set(args[1], &Entry{Kind: KindString, Str: args[2]})
		return resp.Int(int64(len(args[2])))
	}
	entry.Str += args[2]
	return resp.Int(int64(len(entry.Str)))
}

func cmdStrlen(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindString)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		return resp.Int(0)
	}
	return resp.Int(int64(len(entry.Str)))
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
func incrBy(e *Engine, key string, delta int64) resp.Value {
	entry, ok, rightKind := e.fetch(key, KindString)
	if !rightKind {
		return wrongType()
	}

	var current int64
	if ok {
		parsed, err := strconv.ParseInt(entry.Str, 10, 64)
		if err != nil {
			return resp.Err("ERR value is not an integer or out of range")
		}
		current = parsed
	}

	next := current + delta
	if ok {
		entry.Str = strconv.FormatInt(next, 10)
	} else {
		e.store.Set(key, &Entry{Kind: KindString, Str: strconv.FormatInt(next, 10)})
	}
	return resp.Int(next)
}

func cmdMGet(e *Engine, args []string) resp.Value {
	values := make([]resp.Value, 0, len(args)-1)
	for _, key := range args[1:] {
		entry, ok, rightKind := e.fetch(key, KindString)
		if !ok || !rightKind {
			values = append(values, resp.NullBulk())
			continue
		}
		values = append(values, resp.BulkString(entry.Str))
	}
	return resp.Arr(values...)
}

func cmdMSet(e *Engine, args []string) resp.Value {
	pairs := args[1:]
	if len(pairs)%2 != 0 {
		return resp.Err("ERR wrong number of arguments for 'mset' command")
	}
	for i := 0; i < len(pairs); i += 2 {
		e.store.Set(pairs[i], &Entry{Kind: KindString, Str: pairs[i+1]})
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
		if _, found := e.store.Get(key); found {
			count++
		}
	}
	return resp.Int(int64(count))
}

func cmdType(e *Engine, args []string) resp.Value {
	entry, found := e.store.Get(args[1])
	if !found {
		return resp.Simple("none")
	}
	return resp.Simple(entry.Kind.String())
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

	entry, found := e.store.Get(key)
	if !found {
		return resp.Int(0)
	}

	// A non-positive TTL deletes the key immediately, which is what Redis does.
	if n <= 0 {
		e.store.Delete(key)
		return resp.Int(1)
	}

	entry.ExpireAt = e.clock.Now().Add(time.Duration(n) * unit)
	return resp.Int(1)
}

func cmdTTL(e *Engine, args []string) resp.Value  { return ttl(e, args[1], time.Second) }
func cmdPTTL(e *Engine, args []string) resp.Value { return ttl(e, args[1], time.Millisecond) }

// ttl returns -2 when the key does not exist and -1 when it exists but has no
// expiry. Those sentinels are part of the protocol, not error handling.
func ttl(e *Engine, key string, unit time.Duration) resp.Value {
	entry, found := e.store.Get(key)
	if !found {
		return resp.Int(-2)
	}
	if entry.ExpireAt.IsZero() {
		return resp.Int(-1)
	}
	remaining := entry.ExpireAt.Sub(e.clock.Now())
	return resp.Int(int64(remaining / unit))
}

func cmdPersist(e *Engine, args []string) resp.Value {
	entry, found := e.store.Get(args[1])
	if !found || entry.ExpireAt.IsZero() {
		return resp.Int(0)
	}
	entry.ExpireAt = time.Time{}
	return resp.Int(1)
}

// --- lists ---

func cmdLPush(e *Engine, args []string) resp.Value { return push(e, args, true) }
func cmdRPush(e *Engine, args []string) resp.Value { return push(e, args, false) }

func push(e *Engine, args []string, left bool) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindList)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		entry = &Entry{Kind: KindList}
		e.store.Set(args[1], entry)
	}

	for _, value := range args[2:] {
		if left {
			entry.List = append([]string{value}, entry.List...)
		} else {
			entry.List = append(entry.List, value)
		}
	}
	return resp.Int(int64(len(entry.List)))
}

func cmdLPop(e *Engine, args []string) resp.Value { return pop(e, args[1], true) }
func cmdRPop(e *Engine, args []string) resp.Value { return pop(e, args[1], false) }

func pop(e *Engine, key string, left bool) resp.Value {
	entry, ok, rightKind := e.fetch(key, KindList)
	if !rightKind {
		return wrongType()
	}
	if !ok || len(entry.List) == 0 {
		return resp.NullBulk()
	}

	var value string
	if left {
		value, entry.List = entry.List[0], entry.List[1:]
	} else {
		last := len(entry.List) - 1
		value, entry.List = entry.List[last], entry.List[:last]
	}

	// Redis deletes a collection key once it is empty.
	if len(entry.List) == 0 {
		e.store.Delete(key)
	}
	return resp.BulkString(value)
}

func cmdLLen(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindList)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		return resp.Int(0)
	}
	return resp.Int(int64(len(entry.List)))
}

// cmdLRange supports negative indexes, where -1 is the last element.
func cmdLRange(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindList)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		return resp.Arr()
	}

	start, err1 := strconv.Atoi(args[2])
	stop, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	length := len(entry.List)
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return resp.Arr()
	}

	values := make([]resp.Value, 0, stop-start+1)
	for _, item := range entry.List[start : stop+1] {
		values = append(values, resp.BulkString(item))
	}
	return resp.Arr(values...)
}

// --- hashes ---

func cmdHSet(e *Engine, args []string) resp.Value {
	pairs := args[2:]
	if len(pairs)%2 != 0 {
		return resp.Err("ERR wrong number of arguments for 'hset' command")
	}

	entry, ok, rightKind := e.fetch(args[1], KindHash)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		entry = &Entry{Kind: KindHash, Hash: make(map[string]string)}
		e.store.Set(args[1], entry)
	}

	added := 0
	for i := 0; i < len(pairs); i += 2 {
		if _, exists := entry.Hash[pairs[i]]; !exists {
			added++
		}
		entry.Hash[pairs[i]] = pairs[i+1]
	}
	return resp.Int(int64(added))
}

func cmdHGet(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindHash)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		return resp.NullBulk()
	}
	value, exists := entry.Hash[args[2]]
	if !exists {
		return resp.NullBulk()
	}
	return resp.BulkString(value)
}

func cmdHDel(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindHash)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		return resp.Int(0)
	}

	deleted := 0
	for _, field := range args[2:] {
		if _, exists := entry.Hash[field]; exists {
			delete(entry.Hash, field)
			deleted++
		}
	}
	if len(entry.Hash) == 0 {
		e.store.Delete(args[1])
	}
	return resp.Int(int64(deleted))
}

func cmdHLen(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindHash)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		return resp.Int(0)
	}
	return resp.Int(int64(len(entry.Hash)))
}

func cmdHExists(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindHash)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		return resp.Int(0)
	}
	if _, exists := entry.Hash[args[2]]; exists {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func cmdHGetAll(e *Engine, args []string) resp.Value {
	entry, ok, rightKind := e.fetch(args[1], KindHash)
	if !rightKind {
		return wrongType()
	}
	if !ok {
		return resp.Arr()
	}

	values := make([]resp.Value, 0, len(entry.Hash)*2)
	for field, value := range entry.Hash {
		values = append(values, resp.BulkString(field), resp.BulkString(value))
	}
	return resp.Arr(values...)
}
