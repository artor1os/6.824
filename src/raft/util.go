package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(a ...interface{}) {
	if Debug > 0 {
		log.Println(a...)
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func RandIntRange(left int, right int) int {
	return left + rand.Intn(right-left)
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
