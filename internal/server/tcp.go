// Package server is the TCP front door.
package server

import (
	"context"
	"log"
	"net"
	"strings"
	"time"

	"github.com/AviK0928/RedisGo/internal/engine"
	"github.com/AviK0928/RedisGo/internal/resp"
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

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	for {
		// drop connections that go quiet, so idle clients do not pile up
		if err := conn.SetReadDeadline(time.Now().Add(idleTimeout)); err != nil {
			return
		}

		args, err := reader.ReadCommand()
		if err != nil {
			return // client disconnected, malformed input, or idle timeout
		}
		if len(args) == 0 {
			continue
		}

		// QUIT is handled here rather than in the engine, because it is the
		// only command whose effect is on the connection itself.
		if strings.EqualFold(args[0], "QUIT") {
			writer.Write(resp.Simple("OK"))
			return
		}

		if err := writer.Write(e.Execute(args)); err != nil {
			return
		}
	}
}
