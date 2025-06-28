package main

import (
	"bufio"
	"encoding/binary"
	"os"
	"time"
)

func loadRDB(path string) {
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
		case 0xFE, 0xFB:
			readLength(reader)
			readLength(reader)
		case 0xFA:
			readString(reader)
			readString(reader)
		case 0xFD:
			expiry := make([]byte, 4)
			reader.Read(expiry)
			expiryTime := time.Unix(int64(binary.LittleEndian.Uint32(expiry)), 0)
			if typ, _ := reader.ReadByte(); typ == 0x00 {
				key, _ := readString(reader)
				val, _ := readString(reader)
				mu.Lock()
				store[key] = entry{value: val, expireAt: expiryTime}
				mu.Unlock()
			}
		case 0xFC:
			expiry := make([]byte, 8)
			reader.Read(expiry)
			ms := int64(binary.LittleEndian.Uint64(expiry))
			expiryTime := time.Unix(ms/1000, (ms%1000)*1000000)
			if typ, _ := reader.ReadByte(); typ == 0x00 {
				key, _ := readString(reader)
				val, _ := readString(reader)
				mu.Lock()
				store[key] = entry{value: val, expireAt: expiryTime}
				mu.Unlock()
			}
		case 0x00:
			key, _ := readString(reader)
			val, _ := readString(reader)
			mu.Lock()
			store[key] = entry{value: val}
			mu.Unlock()
		default:
			continue
		}
	}
}
