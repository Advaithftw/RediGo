package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

var configDir string
var configFilename string
var isReplica bool

func main() {
	dirFlag := flag.String("dir", ".", "Directory for RDB file")
	fileFlag := flag.String("dbfilename", "dump.rdb", "RDB file name")
	portFlag := flag.Int("port", 6379, "Port number to run the server on")
	replicaFlag := flag.String("replicaof", "", "Replica of host:port (e.g., 'localhost 6379')")

	flag.Parse()

	configDir = *dirFlag
	configFilename = *fileFlag
	isReplica = *replicaFlag != ""

	loadRDB(configDir + "/" + configFilename)

	if isReplica {
		parts := strings.Fields(*replicaFlag)
		if len(parts) == 2 {
			go startReplica(fmt.Sprintf("%s:%s", parts[0], parts[1]), *portFlag)
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
