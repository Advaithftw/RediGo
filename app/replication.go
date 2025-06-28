package main

import (
	"fmt"
	"strings"
	"sync"
)

var (
	replicaConnections []*replicaConn
	replicaMu          sync.Mutex
	masterReplOffsetMu sync.Mutex
)

func propagateToReplicas(parts []string) {
	replicaMu.Lock()
	defer replicaMu.Unlock()

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
	for _, part := range parts {
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(part), part))
	}
	data := []byte(builder.String())

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
