package engine

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/AviK0928/RedisGo/internal/aof"
	"github.com/AviK0928/RedisGo/internal/snapshot"
)

// rewriteGrowthFactor triggers an automatic AOF rewrite once the log has grown
// to this multiple of its size after the last rewrite. Two means the log may
// double before it is compacted, which bounds the wasted space at roughly half
// the file without rewriting so often that the cost dominates.
const rewriteGrowthFactor = 2

// minRewriteSize stops the automatic trigger from firing constantly on a small
// log, where a rewrite would cost more than the space it reclaims.
const minRewriteSize = 64 * 1024

// Save writes a snapshot of the current keyspace.
//
// This is the blocking version, behind the SAVE command. It holds each shard's
// read lock in turn, so writes to a shard being dumped wait. Acceptable for an
// explicit administrative command, not for a background one.
func (e *Engine) Save() (int, error) {
	if e.cfg.SnapshotPath == "" {
		return 0, fmt.Errorf("snapshot: no path configured")
	}

	writer, err := snapshot.Create(e.cfg.SnapshotPath)
	if err != nil {
		return 0, err
	}

	var writeErr error
	e.store.Snapshot(func(key string, entry *Entry) {
		if writeErr != nil {
			return
		}
		writeErr = writer.Write(toRecord(key, entry))
	})

	if writeErr != nil {
		writer.Abort()
		return 0, writeErr
	}

	count, err := writer.Commit()
	if err != nil {
		return 0, err
	}

	e.mu.Lock()
	e.lastSave = e.clock.Now()
	e.mu.Unlock()

	return count, nil
}

// BGSave runs Save on a goroutine and reports whether it started.
//
// Note what this is not: real Redis forks, so the child writes a frozen copy
// of memory while the parent keeps serving. Go cannot fork usefully, so this
// walks the live keyspace instead, meaning the snapshot is not a single
// consistent instant. Keys written during the walk may or may not appear. For
// a cache that is an acceptable trade; for a database it would not be.
func (e *Engine) BGSave() bool {
	if !e.saving.CompareAndSwap(false, true) {
		return false // one already in flight
	}

	go func() {
		defer e.saving.Store(false)

		start := e.clock.Now()
		count, err := e.Save()
		if err != nil {
			log.Printf("bgsave: %v", err)
			return
		}
		log.Printf("bgsave: wrote %d keys in %v", count, e.clock.Now().Sub(start))
	}()

	return true
}

// LoadSnapshot restores the keyspace from disk. Keys already expired at load
// time are skipped rather than restored and then immediately expired.
func (e *Engine) LoadSnapshot() (int, error) {
	if e.cfg.SnapshotPath == "" {
		return 0, nil
	}

	now := e.clock.Now()
	restored := 0

	loaded, err := snapshot.Load(e.cfg.SnapshotPath, func(r snapshot.Record) {
		if !r.ExpireAt.IsZero() && now.After(r.ExpireAt) {
			return
		}
		e.store.Restore(r.Key, fromRecord(r))
		restored++
	})
	if err != nil {
		return 0, err
	}

	if loaded > 0 {
		log.Printf("snapshot: loaded %d keys (%d skipped as already expired)",
			restored, loaded-restored)
	}
	return restored, nil
}

func toRecord(key string, entry *Entry) snapshot.Record {
	r := snapshot.Record{
		Key:      key,
		Kind:     snapshot.Kind(entry.Kind),
		ExpireAt: entry.ExpireAt,
	}
	switch entry.Kind {
	case KindString:
		r.Str = entry.Str
	case KindList:
		r.List = append([]string(nil), entry.List...)
	case KindHash:
		r.Hash = make(map[string]string, len(entry.Hash))
		for field, value := range entry.Hash {
			r.Hash[field] = value
		}
	}
	return r
}

func fromRecord(r snapshot.Record) *Entry {
	entry := &Entry{
		Kind:     Kind(r.Kind),
		Str:      r.Str,
		List:     r.List,
		Hash:     r.Hash,
		ExpireAt: r.ExpireAt,
	}
	if entry.Kind == KindHash && entry.Hash == nil {
		entry.Hash = make(map[string]string)
	}
	return entry
}

// RewriteAOF replaces the log with the shortest command sequence that
// reproduces the current keyspace.
//
// This is what stops the log growing without bound. A key incremented a
// million times occupies a million records; after a rewrite it occupies one
// SET. The saving is not incidental: replay time is proportional to log
// length, so an uncompacted log makes startup slower every day it runs.
func (e *Engine) RewriteAOF() (int, error) {
	if e.aofLog == nil {
		return 0, fmt.Errorf("aof: not enabled")
	}

	path := e.cfg.AOFPath
	tmp := path + ".rewrite"

	// Build the replacement in a separate log, then swap it in. If anything
	// fails partway the original is untouched.
	replacement, err := aof.Open(tmp, aof.SyncNo)
	if err != nil {
		return 0, err
	}

	now := e.clock.Now()
	written := 0
	var writeErr error

	e.store.Snapshot(func(key string, entry *Entry) {
		if writeErr != nil {
			return
		}
		for _, args := range rebuildCommands(key, entry, now) {
			if err := replacement.Append(args); err != nil {
				writeErr = err
				return
			}
		}
		written++
	})

	if writeErr != nil {
		replacement.Close()
		os.Remove(tmp)
		return 0, writeErr
	}
	if err := replacement.Close(); err != nil {
		os.Remove(tmp)
		return 0, err
	}

	// Swap under the rewrite lock so no append lands on the old file between
	// closing it and renaming the new one into place.
	e.rewriteMu.Lock()
	defer e.rewriteMu.Unlock()

	if err := e.aofLog.Close(); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		// Reopen the original so the server is not left without a log.
		e.aofLog, _ = aof.Open(path, e.cfg.AOFSync)
		return 0, err
	}

	reopened, err := aof.Open(path, e.cfg.AOFSync)
	if err != nil {
		return 0, err
	}
	e.aofLog = reopened
	e.rewriteBaseline.Store(reopened.Size())

	return written, nil
}

// BGRewriteAOF runs RewriteAOF on a goroutine.
func (e *Engine) BGRewriteAOF() bool {
	if !e.rewriting.CompareAndSwap(false, true) {
		return false
	}

	go func() {
		defer e.rewriting.Store(false)

		start := e.clock.Now()
		before := e.aofLog.Size()

		count, err := e.RewriteAOF()
		if err != nil {
			log.Printf("bgrewriteaof: %v", err)
			return
		}
		log.Printf("bgrewriteaof: %d keys, %d bytes -> %d bytes in %v",
			count, before, e.aofLog.Size(), e.clock.Now().Sub(start))
	}()

	return true
}

// rebuildCommands is the minimum command sequence that recreates one key.
//
// Expiry is emitted as PEXPIREAT with an absolute timestamp, not EXPIRE with a
// duration. A duration would restart the clock on every rewrite and every
// restart, so a key with a one hour TTL would live forever across frequent
// restarts.
func rebuildCommands(key string, entry *Entry, now time.Time) [][]string {
	var commands [][]string

	switch entry.Kind {
	case KindString:
		commands = append(commands, []string{"SET", key, entry.Str})

	case KindList:
		if len(entry.List) == 0 {
			return nil
		}
		// One RPUSH with every element, rather than one per element.
		args := append([]string{"RPUSH", key}, entry.List...)
		commands = append(commands, args)

	case KindHash:
		if len(entry.Hash) == 0 {
			return nil
		}
		args := make([]string, 0, len(entry.Hash)*2+2)
		args = append(args, "HSET", key)
		for field, value := range entry.Hash {
			args = append(args, field, value)
		}
		commands = append(commands, args)
	}

	if !entry.ExpireAt.IsZero() && entry.ExpireAt.After(now) {
		commands = append(commands, []string{
			"PEXPIREAT", key,
			strconv.FormatInt(entry.ExpireAt.UnixMilli(), 10),
		})
	}

	return commands
}

// maybeRewrite triggers an automatic rewrite when the log has outgrown its
// post-rewrite size by the growth factor.
func (e *Engine) maybeRewrite() {
	if e.aofLog == nil || e.rewriting.Load() {
		return
	}

	size := e.aofLog.Size()
	if size < minRewriteSize {
		return
	}

	baseline := e.rewriteBaseline.Load()
	if baseline == 0 {
		e.rewriteBaseline.Store(size)
		return
	}

	if size >= baseline*rewriteGrowthFactor {
		e.BGRewriteAOF()
	}
}

// StartPersistenceLoop periodically checks whether the AOF needs compacting.
var _ = sync.Once{} // placeholder to keep the import if unused elsewhere

func (e *Engine) StartPersistenceLoop(done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			e.maybeRewrite()
		}
	}
}
