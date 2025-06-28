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
)

type entry struct {
	value    string
	expireAt time.Time
}

type replicaConn struct {
	conn     net.Conn
	offset   int
	mu       sync.Mutex
	ackChan  chan int
	isReplica bool
}

var store = make(map[string]entry)
var mu sync.RWMutex

var configDir string
var configFilename string
var isReplica bool
var masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
var masterReplOffset = 0
var masterReplOffsetMu sync.Mutex
var replicaConnections []*replicaConn
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
	rdbHeader, _ := r.ReadString('\n') // Read $<length>\r\n
	fmt.Printf("Replica received RDB header: %s", rdbHeader)
	
	// Parse RDB length from header like "$88\r\n"
	rdbLenStr := strings.TrimSpace(rdbHeader[1:]) // Remove $ and \r\n
	rdbLen, _ := strconv.Atoi(rdbLenStr)
	
	// Read the RDB content
	rdbData := make([]byte, rdbLen)
	_, err = r.Read(rdbData)
	if err != nil {
		fmt.Println("Error reading RDB data:", err)
		return
	}
	fmt.Printf("Replica received RDB data of length: %d\n", len(rdbData))

	// Now listen for propagated commands
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
			
			// Calculate the total bytes for this command for offset tracking
			commandStart := len(line) + 2 // +2 for \r\n
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
				
				// Handle REPLCONF GETACK before updating offset
				if cmd == "REPLCONF" && len(parts) >= 3 && strings.ToUpper(parts[1]) == "GETACK" {
    // 👎 Wrong order:
    // - You're sending ACK before updating the replicaOffset

    // ✅ Fix: update replicaOffset *before* sending the ACK
    replicaOffsetMu.Lock()
    replicaOffset += totalCommandBytes
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

    fmt.Printf("Replica offset updated to: %d after GETACK\n", currentOffset)
    continue
} else {
					// For all other commands, process them and then update offset
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
						// Just process silently, no response needed
						fmt.Printf("Replica processed PING\n")
					}
					
					// Update offset after processing
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
					resp := fmt.Sprintf(":%d\r\n", ackCount)
					conn.Write([]byte(resp))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'wait' command\r\n"))
				}

			case "REPLCONF":
    if len(parts) >= 2 && strings.ToUpper(parts[1]) == "ACK" && len(parts) >= 3 {
        // This is an ACK response from a replica
        offset, err := strconv.Atoi(parts[2])
        if err == nil {
            // Find the replica and send the offset to its channel
            replicaMu.Lock()
            for _, replica := range replicaConnections {
                if replica.conn == conn {
                    replica.ackChan <- offset // Blocking send
                    fmt.Printf("Master received ACK from replica with offset: %d\n", offset)
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
        // 1. Send FULLRESYNC line
        masterReplOffsetMu.Lock()
        fullResync := fmt.Sprintf("+FULLRESYNC %s %d\r\n", masterReplId, masterReplOffset)
        masterReplOffsetMu.Unlock()
        conn.Write([]byte(fullResync))

        // 2. Prepare empty RDB file bytes
        emptyRDB := []byte{
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x37, // "REDIS0007"
            0xFF, // End of file opcode
            0x00, 0x00, // Checksum (placeholder, still accepted)
            0x00, 0x00,
            0x00, 0x00,
            0x00, 0x00,
        }

        // 3. Send RESP-like bulk string header (without trailing \r\n in content)
        rdbLen := len(emptyRDB)
        conn.Write([]byte(fmt.Sprintf("$%d\r\n", rdbLen)))
        conn.Write(emptyRDB) // no trailing \r\n after content
        
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

func waitForReplicas(numReplicas int, timeoutMs int) int {
    if numReplicas == 0 {
        return 0
    }

    replicaMu.Lock()
    fmt.Printf("WAIT: %d replicas connected\n", len(replicaConnections))
    if len(replicaConnections) == 0 {
        replicaMu.Unlock()
        return 0
    }

    // Send GETACK to all replicas
    getackCmd := "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    getackCmdBytes := len(getackCmd)
    activeReplicas := make([]*replicaConn, 0, len(replicaConnections))
    
    for _, replica := range replicaConnections {
        _, err := replica.conn.Write([]byte(getackCmd))
        if err != nil {
            fmt.Printf("Failed to send GETACK to replica: %v\n", err)
            continue
        }
        activeReplicas = append(activeReplicas, replica)
    }
    
    // Update the list with only active replicas
    replicaConnections = activeReplicas
    replicaMu.Unlock()

    if len(activeReplicas) == 0 {
        fmt.Println("No active replicas after sending GETACK")
        return 0
    }

    // Update master offset to include GETACK command
    masterReplOffsetMu.Lock()
    currentOffset := masterReplOffset
    masterReplOffset += getackCmdBytes * len(activeReplicas)
    masterReplOffsetMu.Unlock()

    // Wait for ACK responses
    ackCount := 0
    timeout := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
    defer timeout.Stop()

    // Use a channel to collect all ACKs
    done := make(chan struct{})
    go func() {
        for _, replica := range activeReplicas {
            select {
            case replicaOffset := <-replica.ackChan:
                fmt.Printf("Received ACK from replica with offset: %d (master offset: %d)\n", replicaOffset, currentOffset)
                // Allow replica offset to be slightly behind due to GETACK command
                if replicaOffset >= currentOffset-getackCmdBytes {
                    ackCount++
                }
            case <-timeout.C:
                fmt.Println("Timeout waiting for replica ACKs")
                return
            }
        }
        close(done)
    }()

    select {
    case <-done:
        // All replicas responded
        fmt.Printf("All replicas responded, ackCount: %d\n", ackCount)
    case <-timeout.C:
        fmt.Printf("WAIT timeout reached, ackCount: %d\n", ackCount)
    }

    return ackCount
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

	// Update master offset
	masterReplOffsetMu.Lock()
	masterReplOffset += len(data)
	masterReplOffsetMu.Unlock()

	activeReplicas := make([]*replicaConn, 0, len(replicaConnections))
	for _, replica := range replicaConnections {
		_, err := replica.conn.Write(data)
		if err == nil {
			activeReplicas = append(activeReplicas, replica)
		}
	}
	replicaConnections = activeReplicas
}