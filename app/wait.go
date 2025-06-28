package main

import (

	"time"
)

func waitForReplicas(numReplicas int, timeoutMs int) int {
	if numReplicas == 0 {
		return 0
	}

	replicaMu.Lock()
	if len(replicaConnections) == 0 {
		replicaMu.Unlock()
		return 0
	}

	masterReplOffsetMu.Lock()
	expectedOffset := masterReplOffset
	masterReplOffsetMu.Unlock()

	getackCmd := "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
	activeReplicas := make([]*replicaConn, 0, len(replicaConnections))

	for _, replica := range replicaConnections {
		_, err := replica.conn.Write([]byte(getackCmd))
		if err == nil {
			activeReplicas = append(activeReplicas, replica)
		}
	}
	replicaConnections = activeReplicas
	replicaMu.Unlock()

	ackCount := 0
	timeout := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timeout.Stop()

	for i := 0; i < len(activeReplicas) && i < numReplicas; i++ {
		select {
		case replicaOffset := <-activeReplicas[i].ackChan:
			if replicaOffset >= expectedOffset {
				ackCount++
			}
		case <-timeout.C:
			return ackCount
		}
	}

	return ackCount
}
