// Command cli is a small client for redisgo, so you can drive the server on a
// laptop without installing redis-tools.
//
// Usage:
//
//	go run ./cmd/cli
//	go run ./cmd/cli -addr host:port
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "server address")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not connect to %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("connected to %s. type QUIT to exit.\n", *addr)

	serverOut := bufio.NewReader(conn)
	stdin := bufio.NewScanner(os.Stdin)

	for {
		fmt.Printf("%s> ", *addr)
		if !stdin.Scan() {
			return
		}
		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}

		if _, err := fmt.Fprintf(conn, "%s\r\n", line); err != nil {
			fmt.Fprintf(os.Stderr, "write failed: %v\n", err)
			return
		}

		reply, err := serverOut.ReadString('\n')
		if err != nil {
			fmt.Fprintf(os.Stderr, "connection closed: %v\n", err)
			return
		}
		fmt.Println(render(reply))

		if strings.EqualFold(line, "QUIT") {
			return
		}
	}
}

// render turns a RESP simple string or error into something readable.
func render(raw string) string {
	body := strings.TrimRight(raw, "\r\n")
	if body == "" {
		return "(empty)"
	}
	switch body[0] {
	case '+':
		return body[1:]
	case '-':
		return "(error) " + body[1:]
	default:
		return body
	}
}
