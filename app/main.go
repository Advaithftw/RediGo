package main

import (
	"bufio"
	"encoding/base64"
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

type Storage struct {
	data map[string]entry
	mu   sync.RWMutex
}

func (s *Storage) get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, exists := s.data[key]
	if !exists || (!e.expireAt.IsZero() && time.Now().After(e.expireAt)) {
		return "", false
	}
	return e.value, true
}

func (s *Storage) set(key, value string, expireAt int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var expiry time.Time
	if expireAt != NotExpired {
		expiry = time.UnixMilli(int64(expireAt))
	}
	s.data[key] = entry{value: value, expireAt: expiry}
}

func (s *Storage) getKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		if e, exists := s.data[k]; exists && (e.expireAt.IsZero() || !time.Now().After(e.expireAt)) {
			keys = append(keys, k)
		}
	}
	return keys
}

type ServerConfig struct {
	dir        string
	dbfilename string
}

type ServerInfo struct {
	role             string
	masterReplId     string
	masterReplOffset int
}

type Server struct {
	storage         *Storage
	config          *ServerConfig
	replicasConn    []net.Conn
	replicaMu       sync.Mutex
	masterReplId    string
	masterReplOffset int
	masterOffsetMu   sync.Mutex
	isMasterFlag     bool
}

func (s *Server) isMaster() bool {
	return s.isMasterFlag
}

func (s *Server) getInfo() ServerInfo {
	s.masterOffsetMu.Lock()
	defer s.masterOffsetMu.Unlock()
	return ServerInfo{
		role:             map[bool]string{true: "master", false: "slave"}[s.isMasterFlag],
		masterReplId:     s.masterReplId,
		masterReplOffset: s.masterReplOffset,
	}
}

const NotExpired = -1

type CommandHandler interface {
	handle(s *Server, clientConn net.Conn, commandArgs []string)
}

type EchoCommandHandler struct{}
type PingCommandHandler struct{}
type GetCommandHandler struct{}
type SetCommandHandler struct{}
type ConfigCommandHandler struct{}
type KeysCommandHandler struct{}
type InfoCommandHandler struct{}
type ReplconfCommandHandler struct{}
type PsyncCommandHandler struct{}
type WaitCommandHandler struct{}

type Command string

const (
	EchoCommand     Command = "ECHO"
	PingCommand     Command = "PING"
	GetCommand      Command = "GET"
	SetCommand      Command = "SET"
	ConfigCommand   Command = "CONFIG"
	KeysCommand     Command = "KEYS"
	InfoCommand     Command = "INFO"
	ReplconfCommand Command = "REPLCONF"
	PsyncCommand    Command = "PSYNC"
	WaitCommand     Command = "WAIT"
)

var WriteCommands = []Command{SetCommand}

func (c EchoCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("ECHO command was received")
	clientConn.Write(toBulkString(commandArgs[0]))
}

func (c PingCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("PING command was received")
	if s.isMaster() {
		clientConn.Write(toSimpleString("PONG"))
	}
}

func (c GetCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("GET command was received")
	fmt.Println("Storage state: ", s.storage.data)
	val, exists := s.storage.get(commandArgs[0])
	if !exists {
		clientConn.Write([]byte("$-1\r\n"))
	} else {
		clientConn.Write(toBulkString(val))
	}
}

func (c SetCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("SET command was received")
	expiredAt := NotExpired
	if len(commandArgs) > 2 && strings.ToUpper(commandArgs[2]) == "PX" {
		i, _ := strconv.Atoi(commandArgs[3])
		expiredAt = int(time.Now().UnixMilli()) + i
	}
	s.storage.set(commandArgs[0], commandArgs[1], expiredAt)

	if s.isMaster() {
		clientConn.Write(toSimpleString("OK"))
		s.propagateToReplicas(commandArgs)
	}
}

func (c ConfigCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("CONFIG command was received")
	switch commandArgs[1] {
	case "dir":
		clientConn.Write(toBulkArray([]string{"dir", s.config.dir}))
	case "dbfilename":
		clientConn.Write(toBulkArray([]string{"dbfilename", s.config.dbfilename}))
	default:
		fmt.Printf("UNKNOWN CONFIG PARAMETER: %s\n", commandArgs[1])
	}
}

func (c KeysCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("KEYS command was received")
	keys := s.storage.getKeys()
	clientConn.Write(toBulkArray(keys))
}

func (c InfoCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("INFO command was received")
	serverInfo := s.getInfo()
	clientConn.Write(toBulkString(fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d", serverInfo.role, serverInfo.masterReplId, serverInfo.masterReplOffset)))
}

func (c ReplconfCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("REPLCONF command was received")
	if commandArgs[0] == "listening-port" {
		s.replicaMu.Lock()
		s.replicasConn = append(s.replicasConn, clientConn)
		s.replicaMu.Unlock()
		fmt.Println("Replica was added")
		clientConn.Write(toSimpleString("OK"))
	} else if commandArgs[0] == "capa" && commandArgs[1] == "psync2" {
		clientConn.Write(toSimpleString("OK"))
	} else if commandArgs[0] == "GETACK" {
		// Send replica's offset if slave, or master's offset if master
		offset := s.getInfo().masterReplOffset
		if !s.isMaster() {
			offset = s.replicaOffset // Assuming replicaOffset is tracked
		}
		clientConn.Write(toBulkArray([]string{"REPLCONF", "ACK", strconv.Itoa(offset)}))
		fmt.Printf("Sent REPLCONF ACK with offset: %d\n", offset)
	} else if commandArgs[0] == "ACK" {
		if s.isMaster() {
			offset, err := strconv.Atoi(commandArgs[1])
			if err == nil {
				s.replicaMu.Lock()
				for _, conn := range s.replicasConn {
					if conn == clientConn {
						select {
						case s.ackChan <- offset:
							fmt.Printf("Master received ACK from replica with offset: %d\n", offset)
						default:
							fmt.Println("ACK channel full, dropping ACK")
						}
						break
					}
				}
				s.replicaMu.Unlock()
			}
		}
	} else {
		clientConn.Write(toSimpleString("OK"))
	}
}

func (c PsyncCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("PSYNC command was received")
	clientConn.Write(toSimpleString(fmt.Sprintf("FULLRESYNC %s %d", s.getInfo().masterReplId, s.getInfo().masterReplOffset)))

	rdb := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(rdb)))
	_, err := base64.StdEncoding.Decode(dst, []byte(rdb))
	if err != nil {
		fmt.Printf("Failed to decode RDB: %v\n", err)
		return
	}
	clientConn.Write(toFileString(dst))
}

func (c WaitCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	fmt.Println("WAIT command was received")
	requiredReplicaCount, _ := strconv.Atoi(commandArgs[0])
	timeout, _ := strconv.Atoi(commandArgs[1])

	s.replicaMu.Lock()
	if len(s.replicasConn) == 0 {
		s.replicaMu.Unlock()
		clientConn.Write([]byte(":0\r\n"))
		return
	}

	currentOffset := s.getInfo().masterReplOffset
	getackCmd := toBulkArray([]string{"REPLCONF", "GETACK", "*"})
	activeReplicas := make([]net.Conn, 0, len(s.replicasConn))

	for _, replicaConn := range s.replicasConn {
		err := replicaConn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		if err != nil {
			fmt.Printf("Failed to set write deadline for replica: %v\n", err)
			continue
		}
		_, err = replicaConn.Write(getackCmd)
		if err == nil {
			activeReplicas = append(activeReplicas, replicaConn)
		} else {
			fmt.Printf("Failed to send GETACK to replica: %v\n", err)
		}
		_ = replicaConn.SetWriteDeadline(time.Time{})
	}
	s.replicasConn = activeReplicas
	s.replicaMu.Unlock()

	fmt.Printf("Active replicas: %d, required: %d, master offset: %d\n", len(activeReplicas), requiredReplicaCount, currentOffset)

	if len(activeReplicas) == 0 {
		clientConn.Write([]byte(":0\r\n"))
		return
	}

	timeoutChan := time.After(time.Duration(timeout) * time.Millisecond)
	ackCount := 0
	ackChan := make(chan int, len(activeReplicas))

	go func() {
		for {
			reader := bufio.NewReader(clientConn)
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Error reading ACK from client: %v\n", err)
				return
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "*") {
				numArgs, _ := strconv.Atoi(line[1:])
				parts := make([]string, 0, numArgs)
				for i := 0; i < numArgs; i++ {
					_, _ = reader.ReadString('\n') // Skip length
					arg, err := reader.ReadString('\n')
					if err != nil {
						fmt.Printf("Error reading ACK arg: %v\n", err)
						return
					}
					parts = append(parts, strings.TrimSpace(arg))
				}
				if len(parts) >= 3 && parts[0] == "REPLCONF" && parts[1] == "ACK" {
					offset, err := strconv.Atoi(parts[2])
					if err == nil && offset >= currentOffset {
						select {
						case ackChan <- offset:
							fmt.Printf("Received valid ACK with offset: %d\n", offset)
						default:
							fmt.Println("ACK channel full, dropping ACK")
						}
					}
				}
			}
		}
	}()

	for ackCount < requiredReplicaCount && ackCount < len(activeReplicas) {
		select {
		case offset := <-ackChan:
			if offset >= currentOffset {
				ackCount++
				fmt.Printf("Received ack: %d/%d\n", ackCount, requiredReplicaCount)
			}
		case <-timeoutChan:
			fmt.Printf("Timeout reached, final ack count: %d\n", ackCount)
			break
		}
	}

	clientConn.Write([]byte(fmt.Sprintf(":%d\r\n", ackCount)))
}

func getCommandHandler(command Command) CommandHandler {
	switch command {
	case PingCommand:
		return PingCommandHandler{}
	case EchoCommand:
		return EchoCommandHandler{}
	case GetCommand:
		return GetCommandHandler{}
	case SetCommand:
		return SetCommandHandler{}
	case KeysCommand:
		return KeysCommandHandler{}
	case ConfigCommand:
		return ConfigCommandHandler{}
	case InfoCommand:
		return InfoCommandHandler{}
	case ReplconfCommand:
		return ReplconfCommandHandler{}
	case PsyncCommand:
		return PsyncCommandHandler{}
	case WaitCommand:
		return WaitCommandHandler{}
	default:
		panic(fmt.Sprintf("No command handler was found for command: %s", command))
	}
}

func (s *Server) propagateToReplicas(parts []string) {
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
	for _, part := range parts {
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(part), part))
	}
	data := []byte(builder.String())

	s.masterOffsetMu.Lock()
	s.masterReplOffset += len(data)
	fmt.Printf("Propagating command: %v, new offset: %d\n", parts, s.masterReplOffset)
	s.masterOffsetMu.Unlock()

	activeReplicas := make([]net.Conn, 0, len(s.replicasConn))
	for _, replica := range s.replicasConn {
		err := replica.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		if err != nil {
			fmt.Printf("Failed to set write deadline for replica: %v\n", err)
			continue
		}
		_, err = replica.Write(data)
		if err == nil {
			activeReplicas = append(activeReplicas, replica)
		} else {
			fmt.Printf("Failed to propagate to replica: %v\n", err)
		}
		_ = replica.SetWriteDeadline(time.Time{})
	}
	s.replicasConn = activeReplicas
	fmt.Printf("Propagated to %d replicas\n", len(activeReplicas))
}

func toSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

func toBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

func toBulkArray(args []string) []byte {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	return []byte(builder.String())
}

func toFileString(data []byte) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s", len(data), data))
}

func main() {
	dirFlag := flag.String("dir", ".", "Directory for RDB file")
	fileFlag := flag.String("dbfilename", "dump.rdb", "RDB file name")
	portFlag := flag.Int("port", 6379, "Port number to run the server on")
	replicaFlag := flag.String("replicaof", "", "Replica of host:port (e.g., 'localhost 6379')")

	flag.Parse()

	server := &Server{
		storage:         &Storage{data: make(map[string]entry)},
		config:          &ServerConfig{dir: *dirFlag, dbfilename: *fileFlag},
		masterReplId:    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		masterReplOffset: 0,
		isMasterFlag:     *replicaFlag == "",
		ackChan:          make(chan int, 100), // Buffer to avoid blocking
		replicaOffset:    0,
	}

	rdbPath := *dirFlag + "/" + *fileFlag
	loadRDB(rdbPath, server.storage)

	if !server.isMasterFlag {
		parts := strings.Fields(*replicaFlag)
		if len(parts) == 2 {
			masterHost := parts[0]
			masterPort := parts[1]
			go startReplica(fmt.Sprintf("%s:%s", masterHost, masterPort), *portFlag, server)
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
		go handleConnection(conn, server)
	}
}

func startReplica(masterAddr string, replicaPort int, server *Server) {
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Println("Replica dial error:", err)
		return
	}
	defer conn.Close()

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

	rdbLenStr := strings.TrimSpace(rdbHeader[1:])
	rdbLen, _ := strconv.Atoi(rdbLenStr)

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

		originalLine := line
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "*") {
			numArgs, _ := strconv.Atoi(line[1:])
			parts := make([]string, 0, numArgs)
			commandBytes := len(originalLine)

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

			fmt.Printf("Replica received propagated command: %v (bytes: %d)\n", parts, commandBytes)

			if len(parts) > 0 {
				cmd := strings.ToUpper(parts[0])
				if cmd == "REPLCONF" && len(parts) >= 3 && strings.ToUpper(parts[1]) == "GETACK" {
					server.replicaMu.Lock()
					currentOffset := server.replicaOffset
					server.replicaMu.Unlock()
					response := toBulkArray([]string{"REPLCONF", "ACK", strconv.Itoa(currentOffset)})
					_, err := conn.Write(response)
					if err != nil {
						fmt.Printf("Failed to send REPLCONF ACK: %v\n", err)
					} else {
						fmt.Printf("Replica sent ACK response with offset: %d\n", currentOffset)
					}
					continue
				} else {
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
							server.storage.set(key, val, NotExpired)
							if !expireAt.IsZero() {
								server.storage.set(key, val, int(expireAt.UnixMilli()))
							}
							fmt.Printf("Replica stored: %s = %s\n", key, val)
						}
					case "PING":
						fmt.Printf("Replica processed PING\n")
					}
					server.replicaMu.Lock()
					server.replicaOffset += commandBytes
					server.replicaMu.Unlock()
					fmt.Printf("Replica offset updated to: %d\n", server.replicaOffset)
				}
			}
		}
	}
}

func handleConnection(conn net.Conn, server *Server) {
	defer func() {
		conn.Close()
		server.replicaMu.Lock()
		newConnections := make([]net.Conn, 0, len(server.replicasConn))
		for _, replica := range server.replicasConn {
			if replica != conn {
				newConnections = append(newConnections, replica)
			}
		}
		server.replicasConn = newConnections
		server.replicaMu.Unlock()
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
				_, err := reader.ReadString('\n')
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
			handler := getCommandHandler(Command(cmd))
			handler.handle(server, conn, parts[1:])
		}
	}
}

func loadRDB(path string, storage *Storage) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
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
		case 0xFF:
			return
		case 0xFE:
			_, _ = readLength(reader)
		case 0xFB:
			_, _ = readLength(reader)
			_, _ = readLength(reader)
		case 0xFA:
			_, _ = readString(reader)
			_, _ = readString(reader)
		case 0xFD:
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
			if valueType == 0x00 {
				key, _ := readString(reader)
				val, _ := readString(reader)
				storage.set(key, val, int(expiryTime.UnixMilli()))
			}
		case 0xFC:
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
			if valueType == 0x00 {
				key, _ := readString(reader)
				val, _ := readString(reader)
				storage.set(key, val, int(expiryTime.UnixMilli()))
			}
		case 0x00:
			key, _ := readString(reader)
			val, _ := readString(reader)
			storage.set(key, val, NotExpired)
		default:
			continue
		}
	}
}

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

func readString(r *bufio.Reader) (string, error) {
	length, err := readLength(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	_, err = r.Read(buf)
	return string(buf), err
}