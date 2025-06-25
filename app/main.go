package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"strconv"
)

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
			numArgs, _ := strconv.Atoi(line[1:]) // e.g., *2 -> 2 args

			parts := make([]string, 0, numArgs)
			for i := 0; i < numArgs; i++ {
				// Read $<len>
				_, err := reader.ReadString('\n') // skip $<len>
				if err != nil {
					return
				}
				// Read actual string
				arg, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				parts = append(parts, strings.TrimSpace(arg))
			}

			if len(parts) == 0 {
				continue
			}

			cmd := strings.ToUpper(parts[0]) // command like "ECHO"

			if cmd == "PING" {
				conn.Write([]byte("+PONG\r\n"))
			} else if cmd == "ECHO" && len(parts) == 2 {
				msg := parts[1]
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
				conn.Write([]byte(resp))
			} else {
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}
}
