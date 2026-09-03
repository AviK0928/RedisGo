// Package snapshot writes and reads a point-in-time dump of the keyspace.
//
// Where the AOF records how the data got here, a snapshot records only what it
// is now. That makes it far smaller and far faster to load: recovering a
// million keys means reading a million records rather than replaying every
// command that ever touched them. The cost is everything written since the
// last dump, which is why the two mechanisms are used together.
//
// The format is a custom length-prefixed binary encoding rather than
// encoding/gob, so the layout is explicit and a corrupt file fails loudly
// instead of decoding into something plausible.
package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// magic identifies the file and its version. A file that does not start with
// these bytes is rejected outright rather than parsed hopefully.
var magic = [8]byte{'R', 'E', 'D', 'I', 'S', 'G', 'O', '1'}

// Kind mirrors engine.Kind. Duplicated rather than imported so the on-disk
// format is pinned here: renumbering the engine's constants must not silently
// change what an existing file means.
type Kind byte

const (
	KindString Kind = 0
	KindList   Kind = 1
	KindHash   Kind = 2
)

// Record is one key as it appears on disk.
type Record struct {
	Key      string
	Kind     Kind
	Str      string
	List     []string
	Hash     map[string]string
	ExpireAt time.Time // zero means no expiry
}

var (
	ErrBadMagic = errors.New("snapshot: not a redisgo snapshot file")
	ErrCorrupt  = errors.New("snapshot: file is corrupt")
	ErrTooLarge = errors.New("snapshot: record exceeds sane limits")
)

// Guards against a corrupt length prefix making the loader allocate wildly.
const (
	maxStringLen = 64 << 20 // 64 MiB
	maxElements  = 1 << 24
)

// Writer streams records to a file.
type Writer struct {
	file   *os.File
	buf    *bufio.Writer
	path   string
	tmp    string
	count  int
	closed bool
}

// Create opens a snapshot for writing at a temporary path. Commit renames it
// into place; Abort discards it. Writing to a temp file and renaming is what
// makes the operation atomic: a reader either sees the whole previous snapshot
// or the whole new one, never a half-written file.
func Create(path string) (*Writer, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("snapshot: create directory: %w", err)
		}
	}

	tmp := path + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return nil, fmt.Errorf("snapshot: create: %w", err)
	}

	w := &Writer{
		file: file,
		buf:  bufio.NewWriterSize(file, 256*1024),
		path: path,
		tmp:  tmp,
	}

	if _, err := w.buf.Write(magic[:]); err != nil {
		w.Abort()
		return nil, err
	}
	return w, nil
}

func (w *Writer) Write(r Record) error {
	if err := w.buf.WriteByte(byte(r.Kind)); err != nil {
		return err
	}
	if err := w.writeString(r.Key); err != nil {
		return err
	}

	// A zero time is written as 0 rather than as an encoded timestamp, so the
	// common case of no expiry costs eight bytes and no interpretation.
	var expiry int64
	if !r.ExpireAt.IsZero() {
		expiry = r.ExpireAt.UnixNano()
	}
	if err := w.writeInt64(expiry); err != nil {
		return err
	}

	switch r.Kind {
	case KindString:
		if err := w.writeString(r.Str); err != nil {
			return err
		}

	case KindList:
		if err := w.writeInt64(int64(len(r.List))); err != nil {
			return err
		}
		for _, item := range r.List {
			if err := w.writeString(item); err != nil {
				return err
			}
		}

	case KindHash:
		if err := w.writeInt64(int64(len(r.Hash))); err != nil {
			return err
		}
		for field, value := range r.Hash {
			if err := w.writeString(field); err != nil {
				return err
			}
			if err := w.writeString(value); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("snapshot: unknown kind %d", r.Kind)
	}

	w.count++
	return nil
}

func (w *Writer) writeString(s string) error {
	if err := w.writeInt64(int64(len(s))); err != nil {
		return err
	}
	_, err := w.buf.WriteString(s)
	return err
}

func (w *Writer) writeInt64(n int64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(n))
	_, err := w.buf.Write(buf[:])
	return err
}

// Commit flushes, syncs, and atomically moves the file into place.
func (w *Writer) Commit() (int, error) {
	if w.closed {
		return w.count, errors.New("snapshot: already closed")
	}
	w.closed = true

	if err := w.buf.Flush(); err != nil {
		w.file.Close()
		os.Remove(w.tmp)
		return 0, err
	}
	// Sync before rename. Without it the rename could land while the file's
	// contents are still only in the page cache, so a crash would leave a
	// correctly named but empty snapshot.
	if err := w.file.Sync(); err != nil {
		w.file.Close()
		os.Remove(w.tmp)
		return 0, err
	}
	if err := w.file.Close(); err != nil {
		os.Remove(w.tmp)
		return 0, err
	}
	if err := os.Rename(w.tmp, w.path); err != nil {
		os.Remove(w.tmp)
		return 0, fmt.Errorf("snapshot: rename: %w", err)
	}
	return w.count, nil
}

// Abort discards a snapshot in progress.
func (w *Writer) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	w.file.Close()
	os.Remove(w.tmp)
}

// Load reads a snapshot and calls apply for each record. A missing file is not
// an error; it means there is nothing to restore.
func Load(path string, apply func(Record)) (int, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("snapshot: open: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 256*1024)

	var header [8]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, ErrBadMagic
	}
	if header != magic {
		return 0, ErrBadMagic
	}

	loaded := 0
	for {
		record, err := readRecord(reader)
		if errors.Is(err, io.EOF) {
			return loaded, nil
		}
		if err != nil {
			// Unlike the AOF, a truncated snapshot is fatal. The AOF can be
			// replayed up to the break because each command is independent;
			// half a snapshot is half a database, and silently loading it
			// would look like data loss with no error to explain it.
			return loaded, fmt.Errorf("%w: after %d records: %v", ErrCorrupt, loaded, err)
		}
		apply(record)
		loaded++
	}
}

func readRecord(r *bufio.Reader) (Record, error) {
	kindByte, err := r.ReadByte()
	if err != nil {
		return Record{}, err // io.EOF here means a clean end of file
	}

	record := Record{Kind: Kind(kindByte)}

	if record.Key, err = readString(r); err != nil {
		return Record{}, err
	}

	expiry, err := readInt64(r)
	if err != nil {
		return Record{}, err
	}
	if expiry != 0 {
		record.ExpireAt = time.Unix(0, expiry)
	}

	switch record.Kind {
	case KindString:
		if record.Str, err = readString(r); err != nil {
			return Record{}, err
		}

	case KindList:
		n, err := readCount(r)
		if err != nil {
			return Record{}, err
		}
		record.List = make([]string, 0, n)
		for i := int64(0); i < n; i++ {
			item, err := readString(r)
			if err != nil {
				return Record{}, err
			}
			record.List = append(record.List, item)
		}

	case KindHash:
		n, err := readCount(r)
		if err != nil {
			return Record{}, err
		}
		record.Hash = make(map[string]string, n)
		for i := int64(0); i < n; i++ {
			field, err := readString(r)
			if err != nil {
				return Record{}, err
			}
			value, err := readString(r)
			if err != nil {
				return Record{}, err
			}
			record.Hash[field] = value
		}

	default:
		return Record{}, fmt.Errorf("unknown kind byte %d", kindByte)
	}

	return record, nil
}

func readString(r *bufio.Reader) (string, error) {
	n, err := readInt64(r)
	if err != nil {
		return "", err
	}
	if n < 0 || n > maxStringLen {
		return "", ErrTooLarge
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readCount(r *bufio.Reader) (int64, error) {
	n, err := readInt64(r)
	if err != nil {
		return 0, err
	}
	if n < 0 || n > maxElements {
		return 0, ErrTooLarge
	}
	return n, nil
}

func readInt64(r *bufio.Reader) (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(buf[:])), nil
}
