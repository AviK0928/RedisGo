// Command server runs redisgo.
//
// Two modes, decided by whether PORT is set:
//
//	local  no PORT   cache over TCP on :6379, playground on :8080
//	cloud  PORT set  playground only, on $PORT
//
// The switch exists because a Render free web service exposes exactly one port
// and it must be the one named in PORT. The engine is identical either way.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AviK0928/RedisGo/internal/engine"
	"github.com/AviK0928/RedisGo/internal/server"
	"github.com/AviK0928/RedisGo/web"
)

func main() {
	log.SetFlags(log.Ltime)

	cfg := engine.ConfigFromEnv()
	eng := engine.New(cfg)

	// Recovery happens before anything can connect: the snapshot restores the
	// keyspace as of the last dump, then the AOF replays whatever followed.
	if err := eng.EnableAOF(); err != nil {
		log.Fatalf("recovery: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Active expiration, without which the server only notices an expired key
	// when someone reads it, so keys nobody touches again would leak. The
	// persistence loop compacts the AOF once it has outgrown its last rewrite.
	background := make(chan struct{})
	go eng.StartExpiryLoop(background)
	go eng.StartPersistenceLoop(background)

	httpAddr := cfg.HTTPAddr
	cloud := false
	if port := os.Getenv("PORT"); port != "" {
		httpAddr = ":" + port
		cloud = true
	}

	if !cloud {
		go func() {
			if err := server.ListenTCP(ctx, eng, cfg.TCPAddr); err != nil {
				log.Printf("tcp listener stopped: %v", err)
			}
		}()
	}

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           web.Handler(eng),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		mode := "local"
		if cloud {
			mode = "cloud"
		}
		log.Printf("redisgo %s starting in %s mode, http on %s", engine.Version, mode, httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http listener: %v", err)
		}
	}()

	<-ctx.Done()

	// Render sends SIGTERM and allows 30 seconds before killing the process.
	log.Println("shutdown signal received, draining connections")

	// Stop background work first, so a rewrite cannot be in flight while the
	// log is being closed.
	close(background)

	// Drain before persisting. Requests still in flight are writes that have
	// to reach the log, so closing it first would drop them.
	drain, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(drain); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	// A final snapshot, so the next start does not replay the whole log.
	if count, err := eng.Save(); err != nil {
		log.Printf("shutdown save: %v", err)
	} else {
		log.Printf("saved %d keys", count)
	}

	if err := eng.CloseAOF(); err != nil {
		log.Printf("aof close: %v", err)
	}

	log.Println("stopped")
}
