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

// RequestTask RPC - Worker请求任务
type RequestTaskArgs struct {
	WorkerId int // Worker的ID
}

type RequestTaskReply struct {
	TaskType TaskType // 任务类型：Map, Reduce, Waiting, Exiting
	TaskId   int      // 任务ID
	File     []string // 对于Map任务，是输入文件；对于Reduce任务，是中间文件
	NReduce  int      // Reduce任务的数量
	NMap     int      // Map任务的数量
}

// FinishTask RPC - Worker完成任务
type FinishTaskArgs struct {
	WorkerId int      // Worker的ID
	TaskType TaskType // 完成的任务类型
	TaskId   int      // 完成的任务ID
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
