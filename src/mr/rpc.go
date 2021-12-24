package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type JobArgs struct{}

type JobReply struct {
	JobType  int // 0 for no jobs, 1 for Map, 2 for Reduce
	Filename string
	MapId    int
	ReduceId int
	NReduce  int
}

type StatusArgs struct {
	JobType  int // 1 for Map, 2 for Reduce
	Done     bool
	MapId    int
	ReduceId int
}

type StatusReply struct {
	Complete bool
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
