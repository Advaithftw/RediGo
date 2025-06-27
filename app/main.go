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

var store = make(map[string]entry)
var mu sync.RWMutex

var configDir string
var configFilename string
var isReplica bool
var masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
var masterReplOffset = 0

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
	defer conn.Close()
	r := bufio.NewReader(conn)

	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	r.ReadString('\n')

	portStr := strconv.Itoa(replicaPort)
	replconfPort := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(portStr), portStr)
	conn.Write([]byte(replconfPort))
	r.ReadString('\n')

	replconfCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	conn.Write([]byte(replconfCapa))
	r.ReadString('\n')

	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	r.ReadString('\n')
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

			case "REPLCONF":
				conn.Write([]byte("+OK\r\n"))
			case "PSYNC":
	if len(parts) == 3 && parts[1] == "?" && parts[2] == "-1" {
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
		}

		// 3. Send RESP-like bulk string header (without trailing \r\n in content)
		rdbLen := len(emptyRDB)
		conn.Write([]byte(fmt.Sprintf("$%d\r\n", rdbLen)))
		conn.Write(emptyRDB) // no trailing \r\n after content
	} else {
		conn.Write([]byte("-ERR unsupported PSYNC format\r\n"))
	}



			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
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