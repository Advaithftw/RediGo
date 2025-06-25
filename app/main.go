package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"strconv"
	"sync"
	"time"
)

type entry struct {
	value    string
	expireAt time.Time // zero time if no expiry
}

var store = make(map[string]entry)
var mu sync.RWMutex

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "*") {
			numArgs, _ := strconv.Atoi(line[1:])
			parts := make([]string, 0, numArgs)

			for i := 0; i < numArgs; i++ {
				_, err := reader.ReadString('\n') // skip $<len>
				if err != nil {
					return
				}
				arg, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				parts = append(parts, strings.TrimSpace(arg))
			}

			if len(parts) == 0 {
				continue
			}

			cmd := strings.ToUpper(parts[0])

			switch cmd {
			case "PING":
				conn.Write([]byte("+PONG\r\n"))
			case "ECHO":
				if len(parts) == 2 {
					msg := parts[1]
					resp := fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
					conn.Write([]byte(resp))
				}
			case "SET":
				key := parts[1]
				val := parts[2]
				var expireAt time.Time

				// Handle optional PX expiry
				if len(parts) == 5 && strings.ToUpper(parts[3]) == "PX" {
					ms, err := strconv.Atoi(parts[4])
					if err == nil {
						expireAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
					}
				}

				mu.Lock()
				store[key] = entry{value: val, expireAt: expireAt}
				mu.Unlock()

				conn.Write([]byte("+OK\r\n"))

			case "GET":
				key := parts[1]
				mu.RLock()
				e, exists := store[key]
				mu.RUnlock()

				if !exists || ( !e.expireAt.IsZero() && time.Now().After(e.expireAt)) {
					// expired
					mu.Lock()
					delete(store, key)
					mu.Unlock()
					conn.Write([]byte("$-1\r\n"))
				} else {
					resp := fmt.Sprintf("$%d\r\n%s\r\n", len(e.value), e.value)
					conn.Write([]byte(resp))
				}

			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}
}
