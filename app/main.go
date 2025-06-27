package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"io"
)

type entry struct {
	value    string
	expireAt time.Time
}

type replicaConnection struct {
	conn   net.Conn
	reader *bufio.Reader
	mu     sync.Mutex
}

var store = make(map[string]entry)
var mu sync.RWMutex

var configDir string
var configFilename string
var isReplica bool
var masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
var masterReplOffset = 0
var replicaConnections []*replicaConnection
var replicaMu sync.Mutex
var replicaOffset = 0
var replicaOffsetMu sync.Mutex

func main() {
	dirFlag := flag.String("dir", ".", "Directory for RDB file")
	fileFlag := flag.String("dbfilename", "dump.rdb", "RDB file name")
	portFlag := flag.Int("port", 6379, "Port number to run the server on")
	replicaFlag := flag.String("replicaof", "", "Replica of host:port (e.g., 'localhost 6379')")

	flag.Parse()

	configDir = *dirFlag
	configFilename = *fileFlag
	isReplica = *replicaFlag != ""

	rdbPath := configDir + "/" + configFilename
	loadRDB(rdbPath)

	if isReplica {
		parts := strings.Fields(*replicaFlag)
		if len(parts) == 2 {
			masterHost := parts[0]
			masterPort := parts[1]
			go startReplica(fmt.Sprintf("%s:%s", masterHost, masterPort), *portFlag)
		} else {
			fmt.Println("Invalid replicaof format. Use: host port")
		}
	}

	addr := fmt.Sprintf("0.0.0.0:%d", *portFlag)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", *portFlag)
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

func startReplica(masterAddr string, replicaPort int) {
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Println("Replica dial error:", err)
		return
	}
	// Don't defer close here - we need to keep the connection open
	
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	// Send PING
	w.WriteString("*1\r\n$4\r\nPING\r\n")
	w.Flush()
	r.ReadString('\n')

	// Send REPLCONF listening-port
	portStr := strconv.Itoa(replicaPort)
	replconfPort := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(portStr), portStr)
	w.WriteString(replconfPort)
	w.Flush()
	r.ReadString('\n')

	// Send REPLCONF capa psync2
	replconfCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	w.WriteString(replconfCapa)
	w.Flush()
	r.ReadString('\n')

	// Send PSYNC
	w.WriteString("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
	w.Flush()

	// Read FULLRESYNC response
	fullresyncResp, _ := r.ReadString('\n')
	fmt.Printf("Replica received FULLRESYNC: %s", fullresyncResp)

	// Read RDB file header
	rdbHeader, _ := r.ReadString('\n')
	fmt.Printf("Replica received RDB header: %s", rdbHeader)

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

			commandStart := len(line) + 2
			totalCommandBytes := commandStart

			for i := 0; i < numArgs; i++ {
				lengthLine, err := r.ReadString('\n')
				if err != nil {
					return
				}
				totalCommandBytes += len(lengthLine)

				arg, err := r.ReadString('\n')
				if err != nil {
					return
				}
				totalCommandBytes += len(arg)
				parts = append(parts, strings.TrimSpace(arg))
			}

			if len(parts) > 0 {
				cmd := strings.ToUpper(parts[0])
				fmt.Printf("Replica received propagated command: %v (bytes: %d)\n", parts, totalCommandBytes)

				if cmd == "REPLCONF" && len(parts) >= 3 && strings.ToUpper(parts[1]) == "GETACK" {
	replicaOffsetMu.Lock()
	currentOffset := replicaOffset
	replicaOffsetMu.Unlock()

	offsetStr := strconv.Itoa(currentOffset)
	response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offsetStr), offsetStr)

	fmt.Printf("[REPLICA] Preparing ACK for offset %d: %q\n", currentOffset, response)
	_, err := w.WriteString(response)
	if err != nil {
		fmt.Printf("[REPLICA] ERROR writing ACK: %v\n", err)
	}
	err = w.Flush()
	if err != nil {
		fmt.Printf("[REPLICA] ERROR flushing ACK: %v\n", err)
	} else {
		fmt.Printf("[REPLICA] ACK successfully flushed for offset %d\n", currentOffset)
	}
	


	replicaOffsetMu.Lock()
	replicaOffset += totalCommandBytes
	replicaOffsetMu.Unlock()
}
 else {
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

					replicaOffsetMu.Lock()
					replicaOffset += totalCommandBytes
					replicaOffsetMu.Unlock()
					fmt.Printf("Replica offset updated to: %d\n", replicaOffset)
				}
			}
		}
	}

	conn.Close()
}


func handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	isReplicaConn := false

	defer func() {
		conn.Close()
		if isReplicaConn {
			removeReplicaConnection(conn)
		}
	}()

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
				reader.ReadString('\n') // skip $<len>
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
				
				// Only propagate if this is the master
				if !isReplica {
					propagateToReplicas(parts)
				}

			case "GET":
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
						info.WriteString(fmt.Sprintf("master_replid:%s\r\n", masterReplId))
						info.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", masterReplOffset))
					}
					resp := fmt.Sprintf("$%d\r\n%s\r\n", info.Len(), info.String())
					conn.Write([]byte(resp))
				}

			case "WAIT":
				if len(parts) >= 3 {
					numReplicas, _ := strconv.Atoi(parts[1])
					timeoutMs, _ := strconv.Atoi(parts[2])
					
					ackCount := waitForReplicas(numReplicas, timeoutMs)
					resp := fmt.Sprintf(":%d\r\n", ackCount)
					conn.Write([]byte(resp))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'wait' command\r\n"))
				}

			case "REPLCONF":
				conn.Write([]byte("+OK\r\n"))
				
			case "PSYNC":
				if len(parts) == 3 && parts[1] == "?" && parts[2] == "-1" {
					isReplicaConn = true
					
					// 1. Send FULLRESYNC line
					fullResync := fmt.Sprintf("+FULLRESYNC %s %d\r\n", masterReplId, masterReplOffset)
					conn.Write([]byte(fullResync))

					// 2. Prepare empty RDB file bytes
					emptyRDB := []byte{
						0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x37, // "REDIS0007"
						0xFF, // End of file opcode
						0x00, 0x00, // Checksum (placeholder, still accepted)
						0x00, 0x00,
						0x00, 0x00,
						0x00, 0x00,
						0x00, 0x00,
					}

					// 3. Send RESP-like bulk string header (without trailing \r\n in content)
					rdbLen := len(emptyRDB)
					conn.Write([]byte(fmt.Sprintf("$%d\r\n", rdbLen)))
					conn.Write(emptyRDB) // no trailing \r\n after content
					
					replicaMu.Lock()
					replicaConnections = append(replicaConnections, &replicaConnection{
						conn:   conn,
						reader: reader,
					})
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




func waitForReplicas(numReplicas int, timeoutMs int) int {
    replicaMu.Lock()
    replicas := make([]*replicaConnection, len(replicaConnections))
    copy(replicas, replicaConnections)
    replicaMu.Unlock()

    if len(replicas) == 0 {
        return 0
    }

    if numReplicas <= 0 {
        return len(replicas)
    }

    fmt.Printf("waitForReplicas called with timeoutMs: %d, numReplicas: %d\n", timeoutMs, numReplicas)

    // Send REPLCONF GETACK * to all replicas
    getackCmd := "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    ackChan := make(chan struct{}, len(replicas))
    timeout := time.After(time.Duration(timeoutMs) * time.Millisecond)

    for _, replica := range replicas {
        go func(r *replicaConnection) {
            r.mu.Lock()
            defer r.mu.Unlock()

            // Write GETACK command
            _, err := r.conn.Write([]byte(getackCmd))
            if err != nil {
                fmt.Printf("Error sending GETACK to replica: %v\n", err)
                return
            }

            // Set read deadline
            if err := r.conn.SetReadDeadline(time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)); err != nil {
                fmt.Printf("Error setting read deadline: %v\n", err)
                return
            }
            defer r.conn.SetReadDeadline(time.Time{})

            // Read directly from the connection
            buf := make([]byte, 1024)
            data := ""
            for {
                n, err := r.conn.Read(buf)
                if err != nil {
                    if err == io.EOF || strings.Contains(err.Error(), "use of closed network connection") {
                        fmt.Printf("Connection closed or EOF while reading from replica: %v, data so far: %q\n", err, data)
                    } else if strings.Contains(err.Error(), "i/o timeout") {
                        fmt.Printf("Read timeout from replica: %v, data so far: %q\n", err, data)
                    } else {
                        fmt.Printf("Error reading from replica: %v, data so far: %q\n", err, data)
                    }
                    return
                }

                data += string(buf[:n])
                fmt.Printf("Data received from replica: %q\n", string(buf[:n]))
                if strings.Contains(data, "ACK") {
                    fmt.Printf("ACK received from replica: %q\n", data)
                    select {
                    case ackChan <- struct{}{}:
                    default:
                    }
                    return
                }
            }
        }(replica)
    }

    // Count ACKs until we reach the desired number or timeout
    ackCount := 0
    for {
        select {
        case <-ackChan:
            ackCount++
            fmt.Printf("Processed ACK, total: %d\n", ackCount)
            if ackCount >= numReplicas {
                return ackCount
            }
        case <-timeout:
            fmt.Printf("Timeout reached after %dms, received %d ACKs\n", timeoutMs, ackCount)
            return ackCount
        }
    }
}

func removeReplicaConnection(conn net.Conn) {
	replicaMu.Lock()
	defer replicaMu.Unlock()
	
	for i, replica := range replicaConnections {
		if replica.conn == conn {
			replicaConnections = append(replicaConnections[:i], replicaConnections[i+1:]...)
			break
		}
	}
}

// Load RDB file with proper expiry handling
func loadRDB(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Read and verify header
	header := make([]byte, 9)
	_, err = reader.Read(header)
	if err != nil || string(header[:5]) != "REDIS" {
		return
	}

	for {
		opcode, err := reader.ReadByte()
		if err != nil {
			break
		}

		switch opcode {
		case 0xFF: // EOF
			return
		case 0xFE: // Database selector
			_, _ = readLength(reader) // Skip database number
		case 0xFB: // Resizedb
			_, _ = readLength(reader) // Skip hash table size
			_, _ = readLength(reader) // Skip expire hash table size
		case 0xFA: // Auxiliary field
			_, _ = readString(reader) // Skip key
			_, _ = readString(reader) // Skip value
		case 0xFD: // Expiry in seconds
			expiry := make([]byte, 4)
			_, err = reader.Read(expiry)
			if err != nil {
				return
			}
			expiryTime := time.Unix(int64(binary.LittleEndian.Uint32(expiry)), 0)
			
			valueType, err := reader.ReadByte()
			if err != nil {
				return
			}
			
			if valueType == 0x00 { // String type
				key, _ := readString(reader)
				val, _ := readString(reader)
				mu.Lock()
				store[key] = entry{value: val, expireAt: expiryTime}
				mu.Unlock()
			}
		case 0xFC: // Expiry in milliseconds
			expiry := make([]byte, 8)
			_, err = reader.Read(expiry)
			if err != nil {
				return
			}
			expiryMs := int64(binary.LittleEndian.Uint64(expiry))
			expiryTime := time.Unix(expiryMs/1000, (expiryMs%1000)*1000000)
			
			valueType, err := reader.ReadByte()
			if err != nil {
				return
			}
			
			if valueType == 0x00 { // String type
				key, _ := readString(reader)
				val, _ := readString(reader)
				mu.Lock()
				store[key] = entry{value: val, expireAt: expiryTime}
				mu.Unlock()
			}
		case 0x00: // String type without expiry
			key, _ := readString(reader)
			val, _ := readString(reader)
			mu.Lock()
			store[key] = entry{value: val}
			mu.Unlock()
		default:
			// Skip unknown opcodes or other value types
			continue
		}
	}
}

// Read length-encoded integer
func readLength(r *bufio.Reader) (int, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch b >> 6 {
	case 0:
		return int(b & 0x3F), nil
	case 1:
		b2, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		return int(b&0x3F)<<8 | int(b2), nil
	case 2:
		bytes := make([]byte, 4)
		_, err := r.Read(bytes)
		if err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(bytes)), nil
	default:
		return 0, fmt.Errorf("unsupported length encoding")
	}
}

// Read string from RDB
func readString(r *bufio.Reader) (string, error) {
	length, err := readLength(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	_, err = r.Read(buf)
	return string(buf), err
}

func propagateToReplicas(parts []string) {
	replicaMu.Lock()
	defer replicaMu.Unlock()

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
	for _, part := range parts {
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(part), part))
	}
	data := []byte(builder.String())

	for _, replica := range replicaConnections {
		replica.mu.Lock()
		replica.conn.Write(data)
		replica.mu.Unlock()
	}
}