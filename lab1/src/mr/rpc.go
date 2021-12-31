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

/*
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
*/
// Add your RPC definitions here.

//RequestTask struct
type RequestTask struct {
	WorkerID int
}

//Task struct
type Task struct {
	TaskID     string
	TaskType   string
	File       string
	SubmitTime int64
	TaskStatus string
	Reducers   int
}

//SubmitResponse of submit task
type SubmitResponse struct {
	Reponse int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
