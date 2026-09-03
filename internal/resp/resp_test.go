package resp

import (
	"bytes"
	"strings"
	"testing"
)

func TestEncode(t *testing.T) {
	tests := []struct {
		name string
		in   Value
		want string
	}{
		{"simple string", Simple("OK"), "+OK\r\n"},
		{"error", Err("ERR nope"), "-ERR nope\r\n"},
		{"integer", Int(42), ":42\r\n"},
		{"negative integer", Int(-7), ":-7\r\n"},
		{"bulk string", BulkString("hello"), "$5\r\nhello\r\n"},
		{"empty bulk string", BulkString(""), "$0\r\n\r\n"},
		{"null bulk", NullBulk(), "$-1\r\n"},
		{"empty array", Arr(), "*0\r\n"},
		{"array", Arr(BulkString("SET"), BulkString("k")), "*2\r\n$3\r\nSET\r\n$1\r\nk\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := NewWriter(&buf).Write(tt.in); err != nil {
				t.Fatalf("write: %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	values := []Value{
		Simple("OK"),
		Err("ERR bad"),
		Int(0),
		Int(-1),
		BulkString("hello world"),
		BulkString("payload with \r\n inside"),
		BulkString(""),
		NullBulk(),
		Arr(BulkString("a"), Int(2), Simple("c")),
		Arr(),
	}

	for _, want := range values {
		var buf bytes.Buffer
		if err := NewWriter(&buf).Write(want); err != nil {
			t.Fatalf("write %+v: %v", want, err)
		}
		got, err := NewReader(&buf).Read()
		if err != nil {
			t.Fatalf("read %+v: %v", want, err)
		}
		if !equal(got, want) {
			t.Errorf("got %+v, want %+v", got, want)
		}
	}
}

func equal(a, b Value) bool {
	if a.Type != b.Type || a.Str != b.Str || a.Num != b.Num || a.IsNull != b.IsNull {
		return false
	}
	if len(a.Array) != len(b.Array) {
		return false
	}
	for i := range a.Array {
		if !equal(a.Array[i], b.Array[i]) {
			return false
		}
	}
	return true
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"resp array", "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", []string{"GET", "foo"}},
		{"inline", "PING\r\n", []string{"PING"}},
		{"inline with args", "SET k v\n", []string{"SET", "k", "v"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tt.in)).ReadCommand()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("arg %d = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestReadRejectsMalformed(t *testing.T) {
	inputs := []string{
		"$5\r\nab\r\n",      // declared length longer than the payload
		"*2\r\n$1\r\na\r\n", // fewer elements than declared
		":notanumber\r\n",
		"$-5\r\n",
		"%bad\r\n",
	}

	for _, in := range inputs {
		if _, err := NewReader(strings.NewReader(in)).Read(); err == nil {
			t.Errorf("input %q: expected an error, got none", in)
		}
	}
}

// The decoder faces untrusted bytes on every connection. Errors are fine;
// panics are not. Run longer with: go test -fuzz=FuzzRead ./internal/resp/
func FuzzRead(f *testing.F) {
	f.Add("+OK\r\n")
	f.Add("$3\r\nfoo\r\n")
	f.Add("*1\r\n$4\r\nPING\r\n")
	f.Add(":-1\r\n")
	f.Add("*-1\r\n")

	f.Fuzz(func(t *testing.T, in string) {
		NewReader(strings.NewReader(in)).Read()
	})
}
