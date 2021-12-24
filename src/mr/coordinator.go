package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex
	MapStatus        []int    // 0 for not allocated, 1 for in progress, 2 for finished
	MapFiles         []string // path to the files to be mapped
	MapStartTime     []int64  // start running time for Map task
	MapIncomplete    int
	ReduceStatus     []int   // 0 for not allocated, 1 for in progress, 2 for finished
	ReduceStartTime  []int64 // start running time for Reduce task
	ReduceIncomplete int
	NReduce          int
	Complete         bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) UpdateStatus(args *StatusArgs, reply *StatusReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.JobType == 1 {
		// fmt.Printf("Updating status for Map Job: %d: success?: %t\n", args.MapId, args.Done)
		if c.MapStatus[args.MapId] != 1 {
			return nil
		}
		if args.Done {
			c.MapStatus[args.MapId] = 2
			c.MapIncomplete--
		} else {
			c.MapStatus[args.MapId] = 0
		}
	} else {
		// fmt.Printf("Updating status for Reduce Job: %d: success?: %t\n", args.ReduceId, args.Done)
		if c.ReduceStatus[args.ReduceId] != 1 {
			return nil
		}
		if args.Done {
			c.ReduceStatus[args.ReduceId] = 2
			c.ReduceIncomplete--
		} else {
			c.ReduceStatus[args.ReduceId] = 0
		}
	}
	if c.MapIncomplete == 0 && c.ReduceIncomplete == 0 {
		reply.Complete = true
	}
	return nil
}

func (c *Coordinator) AssignJob(args *JobArgs, reply *JobReply) error {
	// fmt.Println("Assigning Jobs to workers")
	c.mu.Lock()
	defer c.mu.Unlock()
	mapFinished := true
	for i, status := range c.MapStatus {
		if status == 0 {
			reply.JobType = 1
			reply.MapId = i
			reply.Filename = c.MapFiles[i]
			reply.NReduce = c.NReduce
			c.MapStatus[i] = 1
			c.MapStartTime[i] = time.Now().UnixMilli() // start task time count
			// fmt.Printf("Assign Map Job %d, filename: %s to worker\n", reply.MapId, reply.Filename)
			return nil
		} else if status == 1 {
			mapFinished = false
		}
	}

	// If all map tasks are assigned, but some of them not finished,
	// do not assign jobs to workers.
	if !mapFinished {
		// fmt.Println("All Map tasks assigned")
		reply.JobType = 0
		return nil
	}

	reduceFinished := true
	for i, status := range c.ReduceStatus {
		if status == 0 {
			reply.JobType = 2
			reply.ReduceId = i
			reply.NReduce = c.NReduce
			c.ReduceStatus[i] = 1
			c.ReduceStartTime[i] = time.Now().UnixMilli()
			// fmt.Printf("Assign Reduce Job %d to worker\n", reply.ReduceId)
			return nil
		} else if status == 1 {
			reduceFinished = false
		}
	}

	// fmt.Println("All Reduce tasks assigned")

	reply.JobType = 0
	reply.Complete = reduceFinished

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, status := range c.MapStatus {
		if status != 2 {
			return false
		}
	}

	for _, status := range c.ReduceStatus {
		if status != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) CheckTimeOut() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, status := range c.MapStatus {
		if status == 1 && time.Now().UnixMilli()-c.MapStartTime[i] >= 10000 {
			c.MapStatus[i] = 0
			c.MapStartTime[i] = -1
		}
	}

	for i, status := range c.ReduceStatus {
		if status == 1 && time.Now().UnixMilli()-c.ReduceStartTime[i] >= 10000 {
			c.ReduceStatus[i] = 0
			c.ReduceStartTime[i] = -1
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{NReduce: nReduce}
	for _, filename := range files {
		c.MapStatus = append(c.MapStatus, 0)
		c.MapFiles = append(c.MapFiles, filename)
		c.MapStartTime = append(c.MapStartTime, -1)
	}
	c.MapIncomplete = len(c.MapStatus)

	for i := 0; i < nReduce; i++ {
		c.ReduceStatus = append(c.ReduceStatus, 0)
		c.ReduceStartTime = append(c.ReduceStartTime, -1)
	}
	c.ReduceIncomplete = len(c.ReduceStatus)

	c.server()
	return &c
}
