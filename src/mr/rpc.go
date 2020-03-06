package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// TaskType ...
type TaskType int

// TaskTypes
const (
	Map TaskType = iota + 1
	Reduce
	Kill
)

// Task ...
// TODO: (Lab1) reduce size of Task
type Task struct {
	ID int
	Type TaskType
	Filename string
	MapTaskNum int
	NReduce int
	ReduceTaskNum int
	NMap int
}

// CompleteArgs ...
type CompleteArgs struct {
	ID int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
