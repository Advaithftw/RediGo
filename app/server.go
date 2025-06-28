package main

import (
	"fmt"
	"net"
	"strings"
	"time"
	"strconv"
	"bufio"
)

var masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
var masterReplOffset = 0


func handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		// Remove this connection from replica list if it was a replica
		replicaMu.Lock()
		newConnections := make([]*replicaConn, 0, len(replicaConnections))
		for _, replica := range replicaConnections {
			if replica.conn != conn {
				newConnections = append(newConnections, replica)
			}
		}
		replicaConnections = newConnections
		replicaMu.Unlock()
	}()

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
				if len(parts) < 3 {
					conn.Write([]byte("-ERR wrong number of arguments for 'set'\r\n"))
					continue
				}
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
				conn.Write([]byte("+OK\r\n"))

				// ✅ Track last write and propagate if master
				if !isReplica {
					propagateToReplicas(parts)
					
				}

			case "GET":
				if len(parts) < 2 {
					conn.Write([]byte("-ERR wrong number of arguments for 'get'\r\n"))
					continue
				}
				key := parts[1]
				mu.RLock()
				e, exists := store[key]
				mu.RUnlock()
				if !exists || (!e.expireAt.IsZero() && time.Now().After(e.expireAt)) {
					mu.Lock()
					delete(store, key)
					mu.Unlock()
					conn.Write([]byte("$-1\r\n"))
				} else {
					resp := fmt.Sprintf("$%d\r\n%s\r\n", len(e.value), e.value)
					conn.Write([]byte(resp))
				}

			case "CONFIG":
				if len(parts) == 3 && strings.ToUpper(parts[1]) == "GET" {
					param := strings.ToLower(parts[2])
					var value string
					if param == "dir" {
						value = configDir
					} else if param == "dbfilename" {
						value = configFilename
					} else {
						conn.Write([]byte("*0\r\n"))
						continue
					}
					resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
						len(param), param, len(value), value)
					conn.Write([]byte(resp))
				} else {
					conn.Write([]byte("-ERR unknown CONFIG command\r\n"))
				}

			case "KEYS":
				if len(parts) == 2 && parts[1] == "*" {
					mu.RLock()
					var resp strings.Builder
					resp.WriteString(fmt.Sprintf("*%d\r\n", len(store)))
					for k := range store {
						resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
					}
					mu.RUnlock()
					conn.Write([]byte(resp.String()))
				} else {
					conn.Write([]byte("*0\r\n"))
				}

			case "INFO":
				if len(parts) == 2 && strings.ToLower(parts[1]) == "replication" {
					var info strings.Builder
					role := "master"
					if isReplica {
						role = "slave"
					}
					info.WriteString(fmt.Sprintf("role:%s\r\n", role))
					if !isReplica {
						masterReplOffsetMu.Lock()
						info.WriteString(fmt.Sprintf("master_replid:%s\r\n", masterReplId))
						info.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", masterReplOffset))
						masterReplOffsetMu.Unlock()
					}
					resp := fmt.Sprintf("$%d\r\n%s\r\n", info.Len(), info.String())
					conn.Write([]byte(resp))
				}

			case "WAIT":
				if len(parts) >= 3 {
					numReplicas, _ := strconv.Atoi(parts[1])
					timeoutMs, _ := strconv.Atoi(parts[2])

					ackCount := waitForReplicas(numReplicas, timeoutMs)
conn.Write([]byte(fmt.Sprintf(":%d\r\n", ackCount)))

				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'wait' command\r\n"))
				}

			case "REPLCONF":
				if len(parts) >= 3 && strings.ToUpper(parts[1]) == "ACK" {
					offset, err := strconv.Atoi(parts[2])
					if err == nil {
						replicaMu.Lock()
						for _, replica := range replicaConnections {
							if replica.conn == conn {
								select {
								case replica.ackChan <- offset:
									fmt.Printf("Master received ACK from replica with offset: %d\n", offset)
								default:
									fmt.Printf("Master ACK channel full, dropping ACK\n")
								}
								break
							}
						}
						replicaMu.Unlock()
					}
				} else {
					conn.Write([]byte("+OK\r\n"))
				}

			case "PSYNC":
				if len(parts) == 3 && parts[1] == "?" && parts[2] == "-1" {
					masterReplOffsetMu.Lock()
					fullResync := fmt.Sprintf("+FULLRESYNC %s %d\r\n", masterReplId, masterReplOffset)
					masterReplOffsetMu.Unlock()
					conn.Write([]byte(fullResync))

					emptyRDB := []byte{
						0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x37,
						0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
					}
					conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(emptyRDB))))
					conn.Write(emptyRDB)

					replicaMu.Lock()
					replica := &replicaConn{
						conn:      conn,
						offset:    0,
						ackChan:   make(chan int, 1),
						isReplica: true,
					}
					replicaConnections = append(replicaConnections, replica)
					replicaMu.Unlock()
				} else {
					conn.Write([]byte("-ERR unsupported PSYNC format\r\n"))
				}

			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}
}
