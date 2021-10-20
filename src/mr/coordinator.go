package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	sync.Mutex
	Files      []string
	MapNum     int
	ReduceNum  int
	mapStat    map[int]int
	reduceStat map[int]int
}

func (c *Coordinator) PickIndex(jobType string) (index int, ok bool) {
	c.Lock()
	defer c.Unlock()

	if jobType == "map" {
		for i := 0; i < c.MapNum; i++ {
			if c.mapStat[i] == 0 {
				return i, true
			}
		}
		return -1, false
	} else if jobType == "reduce" {
		for i := 0; i < c.ReduceNum; i++ {
			if c.reduceStat[i] == 0 {
				return i, true
			}
		}
		return -1, false
	} else {
		return -1, false
	}
}

func (c *Coordinator) IsAllFinished(jobType string) (ok bool) {
	c.Lock()
	defer c.Unlock()

	if jobType == "map" {
		for i := 0; i < c.MapNum; i++ {
			if c.mapStat[i] != 2 {
				return false
			}
		}
		return true
	} else if jobType == "reduce" {
		for i := 0; i < c.ReduceNum; i++ {
			if c.reduceStat[i] != 2 {
				return false
			}
		}
		return true
	} else {
		return false
	}
}

func (c *Coordinator) GetInt(jobType string, index int) int {
	c.Lock()
	defer c.Unlock()

	if jobType == "map" {
		return c.mapStat[index]
	} else if jobType == "reduce" {
		return c.reduceStat[index]
	} else {
		return -1
	}
}

func (c *Coordinator) SetInt(jobType string, index, val int) bool {
	c.Lock()
	defer c.Unlock()

	if jobType == "map" {
		c.mapStat[index] = val
		return true
	} else if jobType == "reduce" {
		c.reduceStat[index] = val
		return true
	} else {
		return false
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Notify(args *NotifyArgs, reply *NotifyReply) error {
	if args.NotifyType == "map" {
		c.SetInt("map", args.NotifyIndex, 2)
		return nil
	} else if args.NotifyType == "reduce" {
		c.SetInt("reduce", args.NotifyIndex, 2)
		return nil
	}
	return nil
}

func (c *Coordinator) Handler(args *HandlerArgs, reply *HandlerReply) error {

	if !c.IsAllFinished("map") {
		index, ok := c.PickIndex("map")
		if !ok {
			reply.JobType = "retry"
			return nil
		} else {
			reply.JobType = "map"
			reply.MapFile = c.Files[index]
			reply.MapIndex = index
			reply.ReduceNum = c.ReduceNum

			c.SetInt("map", index, 1)

			time.AfterFunc(10*time.Second, func() {
				if c.GetInt("map", index) == 1 {
					c.SetInt("map", index, 0)
				}
			})
			return nil
		}
	}
	if !c.IsAllFinished("reduce") {

		index, ok := c.PickIndex("reduce")
		if !ok {
			reply.JobType = "retry"
			return nil
		} else {
			reply.JobType = "reduce"
			reply.ReduceIndex = index
			reply.MapNum = len(c.Files)
			c.SetInt("reduce", index, 1)

			time.AfterFunc(10*time.Second, func() {
				if c.GetInt("reduce", index) == 1 {
					c.SetInt("reduce", index, 0)
				}
			})
			return nil
		}
	}

	reply.JobType = "alldone"
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// Your code here.

	return c.IsAllFinished("map") && c.IsAllFinished("reduce")
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.MapNum = len(c.Files)
	c.ReduceNum = nReduce
	c.mapStat = make(map[int]int)
	c.reduceStat = make(map[int]int)

	for i := 0; i < c.MapNum; i++ {
		c.mapStat[i] = 0
	}

	for i := 0; i < c.ReduceNum; i++ {
		c.reduceStat[i] = 0
	}

	c.server()
	return &c
}
