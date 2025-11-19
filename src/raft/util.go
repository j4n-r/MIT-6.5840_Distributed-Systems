package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
