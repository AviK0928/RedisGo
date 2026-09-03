package engine

import (
	"sync"
	"testing"
)

func TestExecute(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		want  string
		isErr bool
	}{
		{"ping", []string{"PING"}, "PONG", false},
		{"ping lowercase", []string{"ping"}, "PONG", false},
		{"ping with message", []string{"PING", "hello"}, "hello", false},
		{"echo", []string{"ECHO", "hi"}, "hi", false},
		{"echo wrong arity", []string{"ECHO"}, "ERR wrong number of arguments for 'echo' command", true},
		{"empty", []string{}, "ERR empty command", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(DefaultConfig())
			got := e.Execute(tt.args)
			if got.Text != tt.want {
				t.Errorf("text = %q, want %q", got.Text, tt.want)
			}
			if got.IsErr != tt.isErr {
				t.Errorf("isErr = %v, want %v", got.IsErr, tt.isErr)
			}
		})
	}
}

// Guards the counters against data races. Run with: go test -race ./...
func TestConcurrentCounters(t *testing.T) {
	e := New(DefaultConfig())

	const goroutines, each = 50, 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				e.Execute([]string{"PING"})
				e.AddConn(1)
				e.Stats()
				e.AddConn(-1)
			}
		}()
	}
	wg.Wait()

	if got := e.Stats().CommandsProcessed; got != goroutines*each {
		t.Errorf("commands = %d, want %d", got, goroutines*each)
	}
}
