package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)



var replicaOffset = 0
var replicaOffsetMu sync.Mutex

type replicaConn struct {
	conn      net.Conn
	offset    int
	mu        sync.Mutex
	ackChan   chan int
	isReplica bool
}

// ----------- Replica Logic -----------
func startReplica(masterAddr string, replicaPort int) {
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Println("Replica dial error:", err)
		return
	}

	r := bufio.NewReader(conn)

	// Send PING
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	r.ReadString('\n')

	// Send REPLCONF listening-port
	portStr := strconv.Itoa(replicaPort)
	replconfPort := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(portStr), portStr)
	conn.Write([]byte(replconfPort))
	r.ReadString('\n')

	// Send REPLCONF capa psync2
	replconfCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	conn.Write([]byte(replconfCapa))
	r.ReadString('\n')

	// Send PSYNC
	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))

	// Read FULLRESYNC response
	fullresyncResp, _ := r.ReadString('\n')
	fmt.Printf("Replica received FULLRESYNC: %s", fullresyncResp)

	// Read RDB file
	rdbHeader, _ := r.ReadString('\n')
	fmt.Printf("Replica received RDB header: %s", rdbHeader)

	// Parse RDB length from header like "$88\r\n"
	rdbLenStr := strings.TrimSpace(rdbHeader[1:])
	rdbLen, _ := strconv.Atoi(rdbLenStr)

	// Read the RDB content
	rdbData := make([]byte, rdbLen)
	_, err = r.Read(rdbData)
	if err != nil {
		fmt.Println("Error reading RDB data:", err)
		return
	}
	fmt.Printf("Replica received RDB data of length: %d\n", len(rdbData))

	// Listen for propagated commands
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			fmt.Println("Replica connection closed:", err)
			break
		}
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "*") {
			numArgs, _ := strconv.Atoi(line[1:])
			parts := make([]string, 0, numArgs)

			// Track total bytes for offset
			commandBytes := len(line) + 2

			for i := 0; i < numArgs; i++ {
				lengthLine, err := r.ReadString('\n')
				if err != nil {
					return
				}
				commandBytes += len(lengthLine)

				arg, err := r.ReadString('\n')
				if err != nil {
					return
				}
				commandBytes += len(arg)
				parts = append(parts, strings.TrimSpace(arg))
			}

			if len(parts) > 0 {
				cmd := strings.ToUpper(parts[0])
				fmt.Printf("Replica received propagated command: %v (bytes: %d)\n", parts, commandBytes)

				// Handle REPLCONF GETACK (no offset update)
				if cmd == "REPLCONF" && len(parts) >= 3 && strings.ToUpper(parts[1]) == "GETACK" {
					replicaOffsetMu.Lock()
					currentOffset := replicaOffset
					replicaOffsetMu.Unlock()

					offsetStr := strconv.Itoa(currentOffset)
					response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offsetStr), offsetStr)
					_, err := conn.Write([]byte(response))
					if err != nil {
						fmt.Printf("Failed to send REPLCONF ACK: %v\n", err)
					} else {
						fmt.Printf("Replica sent ACK response with offset: %d\n", currentOffset)
					}
					continue
				} else {
					// Handle actual commands
					switch cmd {
					case "SET":
						if len(parts) >= 3 {
							key := parts[1]
							val := parts[2]
							var expireAt time.Time
							if len(parts) == 5 && strings.ToUpper(parts[3]) == "PX" {
								ms, err := strconv.Atoi(parts[4])
								if err == nil {
									expireAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
								}
							}
							mu.Lock()
							store[key] = entry{value: val, expireAt: expireAt}
							mu.Unlock()
							fmt.Printf("Replica stored: %s = %s\n", key, val)
						}
					case "PING":
						fmt.Printf("Replica processed PING\n")
					}

					// Update offset
					replicaOffsetMu.Lock()
					replicaOffset += commandBytes
					replicaOffsetMu.Unlock()
					fmt.Printf("Replica offset updated to: %d\n", replicaOffset)
				}
			}
		}
	}

	conn.Close()
}
