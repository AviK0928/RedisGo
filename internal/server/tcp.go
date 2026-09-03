// Package server is the TCP front door.
//
// PHASE 0: it reads inline commands (one space-separated line each) and writes
// RESP simple strings and errors. Enough for nc and cmd/cli, but not for the
// real redis-cli, which speaks RESP arrays. Phase 1 swaps the line reader for a
// resp.Reader; the accept loop below stays as it is.
package server

import (
	"bufio"
	"context"
	"log"
	"net"
	"strings"
	"time"

	"github.com/AviK0928/RedisGo/internal/engine"
)

const idleTimeout = 10 * time.Minute

// ListenTCP accepts connections until the context is cancelled.
func ListenTCP(ctx context.Context, e *engine.Engine, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("tcp listening on %s", addr)

	go func() {
		<-ctx.Done()
		ln.Close() // unblocks Accept during shutdown
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil // expected during shutdown
			default:
				return err
			}
		}
		go handleConn(e, conn) // one goroutine per connection
	}
}

func handleConn(e *engine.Engine, conn net.Conn) {
	defer conn.Close()

	e.AddConn(1)
	defer e.AddConn(-1)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// drop connections that go quiet, so idle clients do not pile up
		if err := conn.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
			return
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return // client disconnected, or hit the deadline
		}

		args := strings.Fields(strings.TrimSpace(line))
		if len(args) == 0 {
			continue
		}
		if strings.EqualFold(args[0], "QUIT") {
			writeReply(writer, engine.Reply{Text: "OK"})
			writer.Flush()
			return
		}

		writeReply(writer, e.Execute(args))
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

// writeReply encodes a reply as a RESP simple string or simple error.
func writeReply(w *bufio.Writer, r engine.Reply) {
	if r.IsErr {
		w.WriteString("-")
	} else {
		w.WriteString("+")
	}
	w.WriteString(r.Text)
	w.WriteString("\r\n")
}
