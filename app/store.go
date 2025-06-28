package main

import (
	"sync"
	"time"
)

type entry struct {
	value    string
	expireAt time.Time
}

var store = make(map[string]entry)
var mu sync.RWMutex
