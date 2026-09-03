package server

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/AviK0928/RedisGo/internal/engine"
	"github.com/AviK0928/RedisGo/internal/resp"
)

func startServer(t testing.TB, addr string) *engine.Engine {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	eng := engine.New(engine.DefaultConfig())
	go ListenTCP(ctx, eng, addr)
	time.Sleep(100 * time.Millisecond) // let the listener bind

	return eng
}

// Pipelining means a client may send many commands without waiting for the
// replies, and expects every reply back in order. It works because the reader
// consumes whatever is already buffered rather than one command per round
// trip, but that is worth proving rather than assuming.
func TestPipelining(t *testing.T) {
	startServer(t, "127.0.0.1:16380")

	conn, err := net.Dial("tcp", "127.0.0.1:16380")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	const count = 1000

	// Every command encoded into one buffer, sent as a single write.
	var batch strings.Builder
	encoder := resp.NewWriter(&batch)
	for i := 0; i < count; i++ {
		encoder.WriteNoFlush(resp.Arr(
			resp.BulkString("SET"),
			resp.BulkString(fmt.Sprintf("key%d", i)),
			resp.BulkString(fmt.Sprintf("value%d", i)),
		))
	}
	if err := encoder.Flush(); err != nil {
		t.Fatalf("encode batch: %v", err)
	}
	if _, err := conn.Write([]byte(batch.String())); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	reader := resp.NewReader(conn)
	for i := 0; i < count; i++ {
		reply, err := reader.Read()
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		if reply.Str != "OK" {
			t.Fatalf("reply %d = %+v, want OK", i, reply)
		}
	}

	// Replies must correspond to the commands in order, so a spot check of
	// the resulting data catches any interleaving.
	writer := resp.NewWriter(conn)
	if err := writer.Write(resp.Arr(resp.BulkString("GET"), resp.BulkString("key500"))); err != nil {
		t.Fatalf("verification write: %v", err)
	}

	reply, err := reader.Read()
	if err != nil {
		t.Fatalf("verification read: %v", err)
	}
	if reply.Str != "value500" {
		t.Errorf("key500 = %q, want value500", reply.Str)
	}
}

// The throughput difference is the reason clients pipeline at all: the cost of
// a command over a socket is dominated by syscalls and round trips, not by the
// work the server does.
//
// On loopback the gap is narrower than it would be over a real network, since
// there is no propagation delay to amortize. What remains is the syscall
// saving, which is why both sides have to batch: the client flushes once per
// batch, and the server holds its replies while more commands are pending.
func BenchmarkPipelineVsSequential(b *testing.B) {
	startServer(b, "127.0.0.1:16381")

	b.Run("sequential", func(b *testing.B) {
		conn, err := net.Dial("tcp", "127.0.0.1:16381")
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		defer conn.Close()

		writer := resp.NewWriter(conn)
		reader := resp.NewReader(conn)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Write flushes, so this is one syscall out and one round trip
			// waited on, per command.
			writer.Write(resp.Arr(resp.BulkString("PING")))
			reader.Read()
		}
	})

	for _, depth := range []int{10, 100} {
		b.Run(fmt.Sprintf("pipelined-%d", depth), func(b *testing.B) {
			conn, err := net.Dial("tcp", "127.0.0.1:16381")
			if err != nil {
				b.Fatalf("dial: %v", err)
			}
			defer conn.Close()

			writer := resp.NewWriter(conn)
			reader := resp.NewReader(conn)

			b.ResetTimer()
			for i := 0; i < b.N; i += depth {
				batch := depth
				if remaining := b.N - i; remaining < batch {
					batch = remaining
				}

				// The whole batch is encoded into the buffer and sent with a
				// single flush.
				for j := 0; j < batch; j++ {
					writer.WriteNoFlush(resp.Arr(resp.BulkString("PING")))
				}
				writer.Flush()

				for j := 0; j < batch; j++ {
					reader.Read()
				}
			}
		})
	}
}
