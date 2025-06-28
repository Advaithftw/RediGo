package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

func readLength(r *bufio.Reader) (int, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch b >> 6 {
	case 0:
		return int(b & 0x3F), nil
	case 1:
		b2, _ := r.ReadByte()
		return int(b&0x3F)<<8 | int(b2), nil
	case 2:
		bytes := make([]byte, 4)
		r.Read(bytes)
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
