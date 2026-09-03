package aof

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")

	log, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	commands := [][]string{
		{"SET", "a", "1"},
		{"SET", "b", "two words"},
		{"RPUSH", "list", "x", "y", "z"},
		{"DEL", "a"},
	}
	for _, args := range commands {
		if err := log.Append(args); err != nil {
			t.Fatalf("append %v: %v", args, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var replayed [][]string
	count, err := Replay(path, func(args []string) {
		replayed = append(replayed, args)
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if count != len(commands) {
		t.Fatalf("replayed %d commands, want %d", count, len(commands))
	}
	for i, want := range commands {
		if strings.Join(replayed[i], " ") != strings.Join(want, " ") {
			t.Errorf("command %d = %v, want %v", i, replayed[i], want)
		}
	}
}

func TestReplayMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.aof")

	count, err := Replay(path, func([]string) {
		t.Error("apply called for a file that does not exist")
	})
	if err != nil {
		t.Errorf("replay of a missing file returned %v, want nil", err)
	}
	if count != 0 {
		t.Errorf("replayed %d commands from a missing file", count)
	}
}

// A crash mid-write leaves a partial record at the tail. Everything before it
// must still replay; refusing to start would turn a survivable crash into
// data loss.
func TestReplayTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.aof")

	log, _ := Open(path, SyncAlways)
	log.Append([]string{"SET", "a", "1"})
	log.Append([]string{"SET", "b", "2"})
	log.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Chop the last few bytes to simulate a write interrupted by a crash.
	if err := os.WriteFile(path, data[:len(data)-4], 0o644); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	count, err := Replay(path, func([]string) {})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 1 {
		t.Errorf("replayed %d commands, want 1 (the intact one)", count)
	}
}

// Reopening must continue an existing log rather than truncate it.
func TestReopenAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.aof")

	log, _ := Open(path, SyncAlways)
	log.Append([]string{"SET", "a", "1"})
	log.Close()

	log, err := Open(path, SyncAlways)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if log.Size() == 0 {
		t.Error("reopened log reports zero size; the file was truncated")
	}
	log.Append([]string{"SET", "b", "2"})
	log.Close()

	count, _ := Replay(path, func([]string) {})
	if count != 2 {
		t.Errorf("replayed %d commands after reopen, want 2", count)
	}
}

func TestParseSyncPolicy(t *testing.T) {
	for input, want := range map[string]SyncPolicy{
		"always":   SyncAlways,
		"EVERYSEC": SyncEverySec,
		" no ":     SyncNo,
	} {
		got, ok := ParseSyncPolicy(input)
		if !ok || got != want {
			t.Errorf("ParseSyncPolicy(%q) = %v, %v; want %v, true", input, got, ok, want)
		}
	}

	if _, ok := ParseSyncPolicy("nonsense"); ok {
		t.Error("ParseSyncPolicy accepted an invalid policy")
	}
	if _, ok := ParseSyncPolicy(""); ok {
		t.Error("ParseSyncPolicy accepted an empty string; unset must be distinguishable")
	}
}
