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
	"encoding/binary"
)

type entry struct {
	value    string
	expireAt time.Time
}

var store = make(map[string]entry)
var mu sync.RWMutex

var configDir string
var configFilename string

func main() {
	dirFlag := flag.String("dir", ".", "Directory for RDB file")
	fileFlag := flag.String("dbfilename", "dump.rdb", "RDB file name")
	flag.Parse()

	configDir = *dirFlag
	configFilename = *fileFlag

	loadRDB(filepath.Join(configDir, configFilename))

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
				_, _ = reader.ReadString('\n') // skip $<len>
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
					keys := make([]string, 0, len(store))
					for k := range store {
						keys = append(keys, k)
					}
					mu.RUnlock()
					resp := fmt.Sprintf("*%d\r\n", len(keys))
					for _, k := range keys {
						resp += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
					}
					conn.Write([]byte(resp))
				} else {
					conn.Write([]byte("*0\r\n"))
				}

			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}
}

func loadRDB(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	header := make([]byte, 9)
	if _, err := io.ReadFull(f, header); err != nil {
		return
	}
	if !strings.HasPrefix(string(header), "REDIS") {
		return
	}

	for {
		t := make([]byte, 1)
		if _, err := f.Read(t); err != nil {
			break
		}
		if t[0] == 0xFE || t[0] == 0xFF {
			break
		}
		if t[0] == 0 {
			keyLen, _ := readLength(f)
			key := make([]byte, keyLen)
			f.Read(key)

			valLen, _ := readLength(f)
			val := make([]byte, valLen)
			f.Read(val)

			mu.Lock()
			store[string(key)] = entry{value: string(val)}
			mu.Unlock()
		}
	}
}

func readLength(r io.Reader) int {
	b := make([]byte, 1)
	r.Read(b)
	prefix := (b[0] & 0xC0) >> 6

	switch prefix {
	case 0:
		// 6-bit length
		return int(b[0] & 0x3F)
	case 1:
		// 14-bit length
		b2 := make([]byte, 1)
		r.Read(b2)
		return int(b[0]&0x3F)<<8 | int(b2[0])
	case 2:
		// 32-bit length
		b4 := make([]byte, 4)
		r.Read(b4)
		return int(binary.BigEndian.Uint32(b4))
	default:
		return 0
	}
}
