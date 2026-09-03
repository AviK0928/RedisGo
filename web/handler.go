// Package web is the HTTP front door: the playground UI, a health check, a
// stats endpoint, and the WebSocket bridge that lets a browser drive the cache.
//
// On Render this is the only thing listening, because a free web service
// exposes exactly one port.
package web

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/AviK0928/RedisGo/internal/engine"
	"github.com/AviK0928/RedisGo/internal/resp"
	"github.com/gorilla/websocket"
)

//go:embed static
var staticFiles embed.FS

const (
	// writeWait bounds how long a slow client can block a write.
	writeWait = 10 * time.Second

	// pongWait is how long to wait for a pong before assuming the connection
	// is dead. A browser tab that goes to sleep stops responding, and without
	// this the goroutine and its session would linger.
	pongWait = 60 * time.Second

	// pingPeriod must be shorter than pongWait, or a ping would never arrive
	// before the deadline it is meant to reset.
	pingPeriod = 30 * time.Second

	// maxMessageSize caps one command line from the browser.
	maxMessageSize = 64 * 1024
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,

	// The demo is meant to be tried from anywhere and a session holds nothing
	// sensitive, so any origin is allowed. A server holding real data would
	// check this.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// message is what crosses the WebSocket in either direction.
//
// Session key counts ride along with each reply rather than being polled,
// because they are per connection and the stats endpoint is not.
type message struct {
	Type    string `json:"type"`              // "reply", "error", "welcome"
	Command string `json:"command,omitempty"` // echoed back so the terminal can print it
	Text    string `json:"text,omitempty"`
	Session string `json:"session,omitempty"`
	Keys    int    `json:"keys"`
	MaxKeys int    `json:"max_keys"`
}

// Handler wires up every route. The static files are compiled into the binary,
// so there is no separate frontend build or deploy.
func Handler(e *engine.Engine, done <-chan struct{}) http.Handler {
	static, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err) // only possible if the embed directive is wrong
	}

	sessions := newSessionManager(e, engine.DefaultSessionLimits())
	go sessions.StartReaper(done)

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(static)))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")

		payload := struct {
			engine.Stats
			ActiveSessions int `json:"active_sessions"`
		}{Stats: e.Stats(), ActiveSessions: sessions.count()}

		if err := json.NewEncoder(w).Encode(payload); err != nil {
			http.Error(w, "failed to encode stats", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(sessions, w, r)
	})

	return mux
}

func serveWS(sessions *sessionManager, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade has already written an error response
	}
	defer conn.Close()

	session, err := sessions.create()
	if err != nil {
		log.Printf("ws: could not create session: %v", err)
		return
	}
	defer sessions.remove(session.ID())

	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	// Writes must be serialised: the ping ticker and the command loop both
	// write, and a gorilla connection does not tolerate concurrent writers.
	writes := make(chan message, 16)
	writerDone := make(chan struct{})
	go writeLoop(conn, writes, writerDone)
	defer close(writes)

	stats := session.Stats()
	send(writes, message{
		Type:    "welcome",
		Session: session.ID()[:8],
		Text:    "Connected. Every visitor gets an isolated keyspace. Try: SET greeting hello",
		Keys:    stats.SessionKeys,
		MaxKeys: stats.SessionMaxKeys,
	})

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return // closed, timed out, or oversized
		}

		sessions.touch(session.ID())

		line := strings.TrimSpace(string(raw))
		if line == "" {
			continue
		}

		reply := session.Execute(strings.Fields(line))

		msgType := "reply"
		if reply.Type == resp.Error {
			msgType = "error"
		}

		select {
		case <-writerDone:
			return // the write side gave up, so the connection is finished
		default:
		}

		// Counting the session's keys walks the keyspace, which is acceptable
		// here only because the demo's keyspace is small and capped. A server
		// holding real data would keep a running count instead.
		stats := session.Stats()

		send(writes, message{
			Type:    msgType,
			Command: line,
			Text:    format(reply),
			Keys:    stats.SessionKeys,
			MaxKeys: stats.SessionMaxKeys,
		})
	}
}

// writeLoop owns the connection's write side and keeps it alive with pings.
func writeLoop(conn *websocket.Conn, messages <-chan message, done chan<- struct{}) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		close(done)
	}()

	for {
		select {
		case msg, open := <-messages:
			if !open {
				conn.SetWriteDeadline(time.Now().Add(writeWait))
				conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteJSON(msg); err != nil {
				return
			}

		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// send drops the message rather than blocking when the client is not keeping
// up. A slow consumer must not be able to stall the command loop.
func send(ch chan<- message, msg message) {
	select {
	case ch <- msg:
	default:
	}
}

// format renders a RESP value the way redis-cli does.
func format(v resp.Value) string {
	switch v.Type {
	case resp.SimpleString:
		return v.Str

	case resp.Error:
		return "(error) " + v.Str

	case resp.Integer:
		return "(integer) " + strconv.FormatInt(v.Num, 10)

	case resp.Bulk:
		if v.IsNull {
			return "(nil)"
		}
		// Multi-line payloads such as INFO are printed raw; quoting them would
		// turn every line break into a literal escape.
		if strings.Contains(v.Str, "\n") {
			return v.Str
		}
		return strconv.Quote(v.Str)

	case resp.Array:
		if v.IsNull {
			return "(nil)"
		}
		if len(v.Array) == 0 {
			return "(empty array)"
		}
		var b strings.Builder
		for i, item := range v.Array {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString(strconv.Itoa(i + 1))
			b.WriteString(") ")
			b.WriteString(format(item))
		}
		return b.String()

	default:
		return "(unknown reply type)"
	}
}
