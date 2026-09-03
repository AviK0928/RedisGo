// Command cli is a small Redis client, so you can drive the server on a laptop
// without installing redis-tools.
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
	"strconv"
	"strings"

	"github.com/AviK0928/RedisGo/internal/resp"
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

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)
	stdin := bufio.NewScanner(os.Stdin)

	for {
		fmt.Printf("%s> ", *addr)
		if !stdin.Scan() {
			return
		}

		fields := strings.Fields(stdin.Text())
		if len(fields) == 0 {
			continue
		}

		// Commands go out as arrays of bulk strings, which is what every real
		// Redis client sends.
		args := make([]resp.Value, 0, len(fields))
		for _, field := range fields {
			args = append(args, resp.BulkString(field))
		}

		if err := writer.Write(resp.Arr(args...)); err != nil {
			fmt.Fprintf(os.Stderr, "write failed: %v\n", err)
			return
		}

		reply, err := reader.Read()
		if err != nil {
			fmt.Fprintf(os.Stderr, "connection closed: %v\n", err)
			return
		}
		fmt.Println(format(reply))

		if strings.EqualFold(fields[0], "QUIT") {
			return
		}
	}
}

// format renders a reply the way redis-cli does.
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
			fmt.Fprintf(&b, "%d) %s", i+1, format(item))
		}
		return b.String()
	default:
		return "(unknown reply type)"
	}
}
