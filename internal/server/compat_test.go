package server

import (
	"context"
	"testing"
	"time"

	"github.com/AviK0928/RedisGo/internal/engine"
	"github.com/redis/go-redis/v9"
)

// Proves the server speaks RESP well enough for an unmodified Redis client.
func TestGoRedisCompatibility(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eng := engine.New(engine.DefaultConfig())
	go ListenTCP(ctx, eng, "127.0.0.1:16379")
	time.Sleep(100 * time.Millisecond) // let the listener bind

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:16379"})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("PING: %v", err)
	}
	if err := client.Set(ctx, "name", "avik", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := client.Get(ctx, "name").Result(); err != nil || got != "avik" {
		t.Fatalf("GET = %q, %v; want \"avik\", nil", got, err)
	}
	if _, err := client.Get(ctx, "missing").Result(); err != redis.Nil {
		t.Fatalf("GET missing: expected redis.Nil, got %v", err)
	}
	if got, err := client.Del(ctx, "name").Result(); err != nil || got != 1 {
		t.Fatalf("DEL = %d, %v; want 1, nil", got, err)
	}
}
