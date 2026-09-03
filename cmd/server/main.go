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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	drain, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := srv.Shutdown(drain); err != nil {
		log.Printf("shutdown: %v", err)
	}
	log.Println("stopped")
}
