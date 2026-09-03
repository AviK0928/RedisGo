// Package aof implements the append-only file: a log of every write command,
// replayed on startup to rebuild the keyspace.
//
// Commands are stored in RESP array form, the same bytes a client would send.
// That means the log is inspectable with cat, replayable by feeding it to a
// socket, and needs no separate serializer.
package aof

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AviK0928/RedisGo/internal/resp"
)

// SyncPolicy decides when data reaches the disk.
//
// The distinction that matters: writing to a file only hands bytes to the
// operating system's page cache. Until fsync, a machine crash loses them, even
// though the process believed the write succeeded.
type SyncPolicy string

const (
	// SyncAlways fsyncs after every command. Nothing is lost on a crash, at
	// the cost of a disk round trip per write.
	SyncAlways SyncPolicy = "always"

	// SyncEverySec fsyncs once a second in the background. A crash loses at
	// most one second of writes. This is Redis's default and the sensible
	// trade for a cache.
	SyncEverySec SyncPolicy = "everysec"

	// SyncNo leaves flushing entirely to the OS. Fastest, least durable.
	SyncNo SyncPolicy = "no"
)

func ParseSyncPolicy(s string) (SyncPolicy, bool) {
	switch SyncPolicy(strings.ToLower(strings.TrimSpace(s))) {
	case SyncAlways:
		return SyncAlways, true
	case SyncEverySec:
		return SyncEverySec, true
	case SyncNo:
		return SyncNo, true
	default:
		return SyncEverySec, false
	}
}

// Log is an append-only command log.
type Log struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	policy SyncPolicy
	path   string

	// bytes tracks the file size, so the engine can decide when a rewrite is
	// worth doing without stat-ing the file on every write.
	bytes int64

	closed bool
	done   chan struct{}
	wg     sync.WaitGroup
}

// Open creates or reopens the log at path. The file is opened in append mode,
// so an existing log is continued rather than truncated.
func Open(path string, policy SyncPolicy) (*Log, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("aof: create directory: %w", err)
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("aof: open: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("aof: stat: %w", err)
	}

	l := &Log{
		file:   file,
		writer: bufio.NewWriterSize(file, 64*1024),
		policy: policy,
		path:   path,
		bytes:  info.Size(),
		done:   make(chan struct{}),
	}

	if policy == SyncEverySec {
		l.wg.Add(1)
		go l.syncLoop()
	}

	return l, nil
}

// Append records one command.
func (l *Log) Append(args []string) error {
	values := make([]resp.Value, 0, len(args))
	for _, arg := range args {
		values = append(values, resp.BulkString(arg))
	}

	var buf strings.Builder
	if err := resp.NewWriter(&buf).Write(resp.Arr(values...)); err != nil {
		return err
	}
	encoded := buf.String()

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return errors.New("aof: log is closed")
	}

	n, err := l.writer.WriteString(encoded)
	l.bytes += int64(n)
	if err != nil {
		return err
	}

	// Under always, the command is not acknowledged until it is on disk.
	if l.policy == SyncAlways {
		return l.flushAndSyncLocked()
	}

	// Under everysec and no, the buffer is flushed to the OS but not synced;
	// the background loop or the OS handles the rest.
	return l.writer.Flush()
}

func (l *Log) flushAndSyncLocked() error {
	if err := l.writer.Flush(); err != nil {
		return err
	}
	return l.file.Sync()
}

// syncLoop fsyncs once a second under the everysec policy.
func (l *Log) syncLoop() {
	defer l.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.done:
			return
		case <-ticker.C:
			l.mu.Lock()
			if !l.closed {
				l.flushAndSyncLocked()
			}
			l.mu.Unlock()
		}
	}
}

// Size is the current log size in bytes.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bytes
}

func (l *Log) Path() string { return l.path }

// Close flushes, syncs, and stops the background loop.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	err := l.flushAndSyncLocked()
	l.mu.Unlock()

	close(l.done)
	l.wg.Wait()

	if closeErr := l.file.Close(); err == nil {
		err = closeErr
	}
	return err
}

// Replay reads a log and calls apply for each command it contains.
//
// A truncated final command is not an error. If the process died mid-write,
// the tail of the file is a partial record, and the right response is to
// replay everything up to that point and carry on. Refusing to start because
// of a half-written command would turn a recoverable crash into data loss.
func Replay(path string, apply func(args []string)) (int, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil // first boot, nothing to replay
	}
	if err != nil {
		return 0, fmt.Errorf("aof: open for replay: %w", err)
	}
	defer file.Close()

	reader := resp.NewReader(file)
	applied := 0

	for {
		args, err := reader.ReadCommand()
		if err != nil {
			// Any read failure at this point means the end of usable data.
			return applied, nil
		}
		if len(args) == 0 {
			continue
		}
		apply(args)
		applied++
	}
}
