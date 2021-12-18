package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// Coordinator variables go here
type Coordinator struct {
	// Your definitions here.
	mapTasks    chan *Task
	reduceTasks chan *Task
	taskStatus  map[string]*Task
	keySpace    map[string]bool
	nReduce     int
	reducePhase bool
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
/*
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
*/
func (c *Coordinator) checkTaskStatus() {
	for {
		now := time.Now()
		if c.Done() {
			return
		}
		fmt.Println("Checking for timed out tasks")
		c.mu.Lock()
		for id := range c.taskStatus {
			if c.taskStatus[id].TaskStatus == "Running" && now.Unix()-c.taskStatus[id].SubmitTime > 10 {
				fmt.Printf("Task %v timeout from worker, reassigning \n", c.taskStatus[id])
				c.taskStatus[id].SubmitTime = 0
				c.taskStatus[id].TaskStatus = "New"
				if c.taskStatus[id].TaskType == "map" {
					c.mapTasks <- c.taskStatus[id]
				} else {
					c.reduceTasks <- c.taskStatus[id]
				}
			}
		}
		c.mu.Unlock()
		time.Sleep(2000 * time.Millisecond)
	}
}

func (c *Coordinator) startReduce() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.nReduce; i++ {
		rTask := &Task{
			TaskType:   "reduce",
			TaskID:     "reduce-" + strconv.Itoa(len(c.reduceTasks)),
			TaskStatus: "New",
			File:       "mr-map-" + strconv.Itoa(i),
		}
		fmt.Printf("Created reduce task %v\n", *rTask)
		c.reduceTasks <- rTask
	}
	c.reducePhase = true
}

//GetTask called by workers
func (c *Coordinator) GetTask(req *RequestTask, response *Task) error {
	select {
	case task := <-c.mapTasks:
		c.mu.Lock()
		fmt.Printf("Assigning map task %v\n", *task)
		response.File = task.File
		response.TaskID = task.TaskID
		response.TaskType = task.TaskType
		task.SubmitTime = time.Now().Unix()
		c.taskStatus[task.TaskID] = task
		c.taskStatus[task.TaskID].TaskStatus = "Running"
		c.mu.Unlock()
		return nil
	case task := <-c.reduceTasks:
		c.mu.Lock()
		fmt.Printf("Assigning reduce task %v\n", *task)
		response.File = task.File
		response.TaskID = task.TaskID
		response.TaskType = task.TaskType
		task.SubmitTime = time.Now().Unix()
		c.taskStatus[task.TaskID] = task
		c.taskStatus[task.TaskID].TaskStatus = "Running"
		c.mu.Unlock()
		return nil
	default:
		//Map tasks ended create reduce tasks
		if len(c.reduceTasks) == 0 && !c.reducePhase {
			go c.startReduce()
		}
		return nil
	}
}

//SubmitTaskResult called by workers after task is done
func (c *Coordinator) SubmitTaskResult(req *Task, response *SubmitResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.TaskType == "map" {
		c.taskStatus[req.TaskID].TaskStatus = "Finished"
		response = &SubmitResponse{
			Reponse: 0,
		}
	} else {
		c.taskStatus[req.TaskID].TaskStatus = "Finished"
		response = &SubmitResponse{
			Reponse: 0,
		}
	}
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

// Done is called by main/mrcoordinator.go periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reducePhase && len(c.reduceTasks) == 0 {
		fmt.Println("Done with all tasks")
		return true
	}
	return false
}

// MakeCoordinator is called main/mrcoordinator.go.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make(chan *Task, len(files))
	c.reduceTasks = make(chan *Task, nReduce)
	fmt.Println("Setting up map tasks")
	for i, fileName := range files {
		c.mapTasks <- &Task{
			TaskID:     "map-" + strconv.Itoa(i),
			TaskType:   "map",
			File:       fileName,
			TaskStatus: "New",
		}
	}
	c.keySpace = make(map[string]bool)
	c.taskStatus = make(map[string]*Task)
	c.nReduce = nReduce
	c.reducePhase = false
	c.server()
	go c.checkTaskStatus()
	return &c
}
