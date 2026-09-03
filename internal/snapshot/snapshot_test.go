package snapshot

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")
	expiry := time.Now().Add(time.Hour).Truncate(time.Nanosecond)

	want := []Record{
		{Key: "a", Kind: KindString, Str: "hello"},
		{Key: "empty", Kind: KindString, Str: ""},
		{Key: "with-expiry", Kind: KindString, Str: "temp", ExpireAt: expiry},
		{Key: "list", Kind: KindList, List: []string{"x", "y", "z"}},
		{Key: "binary", Kind: KindString, Str: "has\x00null\r\nand crlf"},
		{Key: "hash", Kind: KindHash, Hash: map[string]string{"f1": "v1", "f2": "v2"}},
	}

	w, err := Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range want {
		if err := w.Write(r); err != nil {
			t.Fatalf("write %s: %v", r.Key, err)
		}
	}
	count, err := w.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if count != len(want) {
		t.Errorf("committed %d records, want %d", count, len(want))
	}

	got := make(map[string]Record)
	loaded, err := Load(path, func(r Record) {
		got[r.Key] = r
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded != len(want) {
		t.Fatalf("loaded %d records, want %d", loaded, len(want))
	}

	for _, w := range want {
		g, found := got[w.Key]
		if !found {
			t.Errorf("key %q missing after load", w.Key)
			continue
		}
		if g.Kind != w.Kind || g.Str != w.Str {
			t.Errorf("key %q = %+v, want %+v", w.Key, g, w)
		}
		if !g.ExpireAt.Equal(w.ExpireAt) {
			t.Errorf("key %q expiry = %v, want %v", w.Key, g.ExpireAt, w.ExpireAt)
		}
		if len(g.List) != len(w.List) || len(g.Hash) != len(w.Hash) {
			t.Errorf("key %q collection size mismatch: %+v", w.Key, g)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	count, err := Load(filepath.Join(t.TempDir(), "nope.rdb"), func(Record) {
		t.Error("apply called for a file that does not exist")
	})
	if err != nil || count != 0 {
		t.Errorf("Load of a missing file = %d, %v; want 0, nil", count, err)
	}
}

func TestRejectsForeignFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreign.rdb")
	os.WriteFile(path, []byte("this is not a snapshot at all"), 0o644)

	if _, err := Load(path, func(Record) {}); !errors.Is(err, ErrBadMagic) {
		t.Errorf("Load of a foreign file = %v, want ErrBadMagic", err)
	}
}

// A half-written snapshot is half a database. Loading it silently would look
// like data loss with nothing to explain it, so truncation must be an error.
func TestTruncatedSnapshotIsFatal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.rdb")

	w, _ := Create(path)
	w.Write(Record{Key: "a", Kind: KindString, Str: "one"})
	w.Write(Record{Key: "b", Kind: KindString, Str: "two"})
	w.Commit()

	data, _ := os.ReadFile(path)
	os.WriteFile(path, data[:len(data)-5], 0o644)

	if _, err := Load(path, func(Record) {}); !errors.Is(err, ErrCorrupt) {
		t.Errorf("Load of a truncated file = %v, want ErrCorrupt", err)
	}
}

// Abort must leave no trace, and must not disturb an existing snapshot.
func TestAbortLeavesPreviousIntact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")

	w, _ := Create(path)
	w.Write(Record{Key: "keep", Kind: KindString, Str: "original"})
	w.Commit()

	w2, _ := Create(path)
	w2.Write(Record{Key: "discard", Kind: KindString, Str: "never committed"})
	w2.Abort()

	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp file survived Abort")
	}

	var keys []string
	Load(path, func(r Record) { keys = append(keys, r.Key) })

	if len(keys) != 1 || keys[0] != "keep" {
		t.Errorf("snapshot after abort holds %v, want [keep]", keys)
	}
}
