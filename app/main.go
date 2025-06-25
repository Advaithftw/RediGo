package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"strconv"
	"sync"
)

var store = make(map[string]string) // In-memory key-value store
var mu sync.RWMutex                 // Mutex to safely handle concurrent access

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
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
				_, err := reader.ReadString('\n') // skip $<length>
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
				if len(parts) == 3 {
					key := parts[1]
					val := parts[2]
					mu.Lock()
					store[key] = val
					mu.Unlock()
					conn.Write([]byte("+OK\r\n"))
				}
			case "GET":
				if len(parts) == 2 {
					key := parts[1]
					mu.RLock()
					val, exists := store[key]
					mu.RUnlock()
					if exists {
						resp := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
						conn.Write([]byte(resp))
					} else {
						conn.Write([]byte("$-1\r\n")) // null bulk string
					}
				}
			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}
}
