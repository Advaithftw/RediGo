package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type entry struct {
	value    string
	expireAt time.Time
}

var (
	store           = make(map[string]entry)
	mu              sync.RWMutex
	configDir       string
	configFilename  string
	isReplica       bool
	masterReplId    = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	masterReplOffset = 0
)

func main() {
	// CLI flags
	dirFlag     := flag.String("dir", ".", "Directory for RDB file")
	fileFlag    := flag.String("dbfilename", "dump.rdb", "RDB file name")
	portFlag    := flag.Int("port", 6379, "Port to listen on")
	replicaFlag := flag.String("replicaof", "", "Master address (host:port) to replicate from")
	flag.Parse()

	configDir      = *dirFlag
	configFilename = *fileFlag
	isReplica      = *replicaFlag != ""

	// Master: preload from local RDB snapshot
	localRDB := filepath.Join(configDir, configFilename)
	loadRDBFromFile(localRDB)

	// Replica: start handshake in background
	if isReplica {
		go startReplica(*replicaFlag, *portFlag)
	}

	// Start TCP server
	addr := fmt.Sprintf("0.0.0.0:%d", *portFlag)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to bind to port %d: %v\n", *portFlag, err)
		os.Exit(1)
	}
	fmt.Println("Listening on", addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		go handleConnection(conn)
	}
}

// startReplica performs the PING/REPLCONF/PSYNC flow then loads the RDB from the master.
func startReplica(masterAddr string, replicaPort int) {
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Println("Replica dial error:", err)
		return
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	// 1) PING
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	r.ReadString('\n') // +PONG

	// 2) REPLCONF listening-port
	portStr := strconv.Itoa(replicaPort)
	cmd := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(portStr), portStr)
	conn.Write([]byte(cmd))
	r.ReadString('\n') // +OK

	// 3) REPLCONF capa psync2
	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	r.ReadString('\n') // +OK

	// 4) PSYNC ? -1
	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	r.ReadString('\n') // +FULLRESYNC <replid> 0

	// 5) Stream and parse the RDB bytes
	loadRDBFromReader(r)
}

// handleConnection parses RESP commands and dispatches them.
func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "*") {
			continue
		}

		n, _ := strconv.Atoi(line[1:])
		parts := make([]string, 0, n)
		for i := 0; i < n; i++ {
			reader.ReadString('\n')         // skip $len
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

		// --- Basic commands ---
		case "PING":
			conn.Write([]byte("+PONG\r\n"))

		case "ECHO":
			if len(parts) == 2 {
				msg := parts[1]
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)))
			}

		case "SET":
			key, val := parts[1], parts[2]
			var exp time.Time
			if len(parts) == 5 && strings.ToUpper(parts[3]) == "PX" {
				if ms, err := strconv.Atoi(parts[4]); err == nil {
					exp = time.Now().Add(time.Duration(ms) * time.Millisecond)
				}
			}
			mu.Lock()
			store[key] = entry{value: val, expireAt: exp}
			mu.Unlock()
			conn.Write([]byte("+OK\r\n"))

		case "GET":
			key := parts[1]
			mu.RLock()
			e, ok := store[key]
			mu.RUnlock()
			if !ok || (!e.expireAt.IsZero() && time.Now().After(e.expireAt)) {
				mu.Lock()
				delete(store, key)
				mu.Unlock()
				conn.Write([]byte("$-1\r\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(e.value), e.value)))
			}

		// --- Config ---
		case "CONFIG":
			if len(parts) == 3 && strings.ToUpper(parts[1]) == "GET" {
				param := strings.ToLower(parts[2])
				var val string
				switch param {
				case "dir":
					val = configDir
				case "dbfilename":
					val = configFilename
				default:
					conn.Write([]byte("*0\r\n"))
					continue
				}
				conn.Write([]byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
					len(param), param, len(val), val)))
			} else {
				conn.Write([]byte("-ERR unknown CONFIG command\r\n"))
			}

		// --- Key listing ---
		case "KEYS":
			if len(parts) == 2 && parts[1] == "*" {
				mu.RLock()
				var sb strings.Builder
				sb.WriteString(fmt.Sprintf("*%d\r\n", len(store)))
				for k := range store {
					sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
				}
				mu.RUnlock()
				conn.Write([]byte(sb.String()))
			} else {
				conn.Write([]byte("*0\r\n"))
			}

		// --- Replication info ---
		case "INFO":
			if len(parts) == 2 && strings.ToLower(parts[1]) == "replication" {
				var sb strings.Builder
				role := "master"
				if isReplica {
					role = "slave"
				}
				sb.WriteString("role:" + role + "\r\n")
				if !isReplica {
					sb.WriteString("master_replid:" + masterReplId + "\r\n")
					sb.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", masterReplOffset))
				}
				out := sb.String()
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(out), out)))
			}

		// --- Replication handshake on master ---
		case "REPLCONF":
			conn.Write([]byte("+OK\r\n"))

		case "PSYNC":
			// master acknowledges and streams its RDB file
			conn.Write([]byte("+FULLRESYNC " + masterReplId + " 0\r\n"))
			file, err := os.Open(filepath.Join(configDir, configFilename))
			if err == nil {
				io.Copy(conn, file)
				file.Close()
			}

		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

// loadRDBFromFile loads one key/value pair from a local RDB file on startup
func loadRDBFromFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	loadRDBFromReader(bufio.NewReader(f))
}

// loadRDBFromReader parses a binary RDB stream from any io.Reader
func loadRDBFromReader(r *bufio.Reader) {
	// Check magic header
	h := make([]byte, 9)
	if _, err := io.ReadFull(r, h); err != nil || string(h[:5]) != "REDIS" {
		return
	}
	for {
		prefix, err := r.ReadByte()
		if err != nil || prefix == 0xFF {
			break
		}
		if prefix == 0x00 { // string type
			key, _ := readString(r)
			val, _ := readString(r)
			mu.Lock()
			store[key] = entry{value: val}
			mu.Unlock()
		} else {
			// skip other types for now
			break
		}
	}
}

// RESP RDB helpers
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
		buf := make([]byte, 4)
		if _, err := r.Read(buf); err != nil {
			return 0, err
		}
		return int(buf[0])<<24 | int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3]), nil
	default:
		return 0, fmt.Errorf("unsupported length encoding")
	}
}

func readString(r *bufio.Reader) (string, error) {
	length, err := readLength(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := r.Read(buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
