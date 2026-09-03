// Package resp implements the Redis serialization protocol (RESP2).
//
// The protocol is length-prefixed rather than delimiter-based, which is why a
// bulk string carries its byte count: the payload can contain \r\n safely.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Type is the leading byte that identifies a RESP value.
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	Bulk         Type = '$'
	Array        Type = '*'
)

// Real Redis allows 512MB bulk strings. This server runs on a 512MB free
// instance, so the limits are deliberately small: a bogus length prefix must
// not be able to make the server allocate its way to death.
const (
	maxBulkLength    = 1 << 20 // 1 MiB
	maxArrayElements = 1 << 20
)

var (
	ErrProtocol = errors.New("resp: protocol error")
	ErrTooLarge = errors.New("resp: value exceeds limit")
)

// Value is any RESP value. Only the fields relevant to Type are meaningful.
type Value struct {
	Type   Type
	Str    string
	Num    int64
	Array  []Value
	IsNull bool
}

func Simple(s string) Value     { return Value{Type: SimpleString, Str: s} }
func Err(s string) Value        { return Value{Type: Error, Str: s} }
func Int(n int64) Value         { return Value{Type: Integer, Num: n} }
func BulkString(s string) Value { return Value{Type: Bulk, Str: s} }
func NullBulk() Value           { return Value{Type: Bulk, IsNull: true} }
func Arr(items ...Value) Value  { return Value{Type: Array, Array: items} }

// Errf builds an error value with a formatted message.
func Errf(format string, a ...any) Value { return Err(fmt.Sprintf(format, a...)) }

// Writer serializes values onto a connection.
type Writer struct {
	w *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

// Write encodes one value and flushes it. Nothing reaches the socket until the
// flush, so forgetting it leaves the client waiting forever.
func (w *Writer) Write(v Value) error {
	if err := w.encode(v); err != nil {
		return err
	}
	return w.w.Flush()
}

func (w *Writer) encode(v Value) error {
	switch v.Type {
	case SimpleString:
		_, err := fmt.Fprintf(w.w, "+%s\r\n", v.Str)
		return err

	case Error:
		_, err := fmt.Fprintf(w.w, "-%s\r\n", v.Str)
		return err

	case Integer:
		_, err := fmt.Fprintf(w.w, ":%d\r\n", v.Num)
		return err

	case Bulk:
		if v.IsNull {
			_, err := w.w.WriteString("$-1\r\n")
			return err
		}
		if _, err := fmt.Fprintf(w.w, "$%d\r\n", len(v.Str)); err != nil {
			return err
		}
		if _, err := w.w.WriteString(v.Str); err != nil {
			return err
		}
		_, err := w.w.WriteString("\r\n")
		return err

	case Array:
		if v.IsNull {
			_, err := w.w.WriteString("*-1\r\n")
			return err
		}
		if _, err := fmt.Fprintf(w.w, "*%d\r\n", len(v.Array)); err != nil {
			return err
		}
		for _, item := range v.Array {
			if err := w.encode(item); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("resp: cannot encode type %q", byte(v.Type))
	}
}

// Reader parses values off a connection.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// ReadCommand reads one command as a flat argument list.
//
// It accepts both forms Redis accepts: a RESP array, which is what every real
// client sends, and an inline command, which is what you get from telnet or nc.
func (r *Reader) ReadCommand() ([]string, error) {
	prefix, err := r.r.Peek(1)
	if err != nil {
		return nil, err
	}

	if Type(prefix[0]) != Array {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		return strings.Fields(line), nil
	}

	v, err := r.Read()
	if err != nil {
		return nil, err
	}
	if v.Type != Array || v.IsNull {
		return nil, fmt.Errorf("%w: command must be an array", ErrProtocol)
	}

	args := make([]string, 0, len(v.Array))
	for _, item := range v.Array {
		args = append(args, item.Str)
	}
	return args, nil
}

// Read parses one value of any type.
func (r *Reader) Read() (Value, error) {
	typeByte, err := r.r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch Type(typeByte) {
	case SimpleString:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Simple(line), nil

	case Error:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Err(line), nil

	case Integer:
		n, err := r.readInt()
		if err != nil {
			return Value{}, err
		}
		return Int(n), nil

	case Bulk:
		return r.readBulk()

	case Array:
		return r.readArray()

	default:
		return Value{}, fmt.Errorf("%w: unknown type byte %q", ErrProtocol, typeByte)
	}
}

func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (r *Reader) readInt() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: bad integer %q", ErrProtocol, line)
	}
	return n, nil
}

func (r *Reader) readBulk() (Value, error) {
	n, err := r.readInt()
	if err != nil {
		return Value{}, err
	}
	if n == -1 {
		return NullBulk(), nil
	}
	if n < 0 {
		return Value{}, fmt.Errorf("%w: negative bulk length %d", ErrProtocol, n)
	}
	if n > maxBulkLength {
		return Value{}, ErrTooLarge
	}

	buf := make([]byte, n+2) // payload plus the trailing CRLF
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return Value{}, err
	}
	return BulkString(string(buf[:n])), nil
}

func (r *Reader) readArray() (Value, error) {
	n, err := r.readInt()
	if err != nil {
		return Value{}, err
	}
	if n == -1 {
		return Value{Type: Array, IsNull: true}, nil
	}
	if n < 0 {
		return Value{}, fmt.Errorf("%w: negative array length %d", ErrProtocol, n)
	}
	if n > maxArrayElements {
		return Value{}, ErrTooLarge
	}

	// Deliberately not preallocating to n: a declared length is untrusted
	// input, and a huge one would otherwise let a client reserve memory it
	// never has to send.
	var items []Value
	for i := int64(0); i < n; i++ {
		item, err := r.Read()
		if err != nil {
			return Value{}, err
		}
		items = append(items, item)
	}
	return Value{Type: Array, Array: items}, nil
}
