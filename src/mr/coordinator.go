package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskId string

type SafeTaskMap struct {
	mu sync.Mutex
	m  map[TaskId]TaskReply
}

type Coordinator struct {
	// Your definitions here.
	MapIsDone    chan bool
	ReduceIsDone chan bool
	TaskChannel     chan TaskReply
	ReduceTasks  SafeTaskMap
	TaskMap      SafeTaskMap
	allIsDone    bool
}

func (c *Coordinator) DistributeTasks(args *TaskArgs, reply *TaskReply) error {
	log.Printf("Entered Distribute Task with %+v, and %+v\n", args, reply)
	task := <-c.TaskChannel
	*reply = task
	log.Printf("TaskReply: %+v", reply)
	return nil
}

func (c *Coordinator) TaskDone(args *TaskArgs, reply *TaskReply) error {
	c.TaskMap.mu.Lock()
	delete(c.TaskMap.m, args.TaskId)
	log.Printf("Task: %+v DELETED", args.TaskId)

	log.Printf("TASKMAP: %+v\n", c.TaskMap.m)
	if len(c.TaskMap.m) == 0 {
		log.Println("TASKS ARE DONE")
		c.MapIsDone <- true
	}
	c.TaskMap.mu.Unlock()

	return nil
}

// start a thread that listens for RPCs from worker.go
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
	log.Println("Start server")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.allIsDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskChannel:     make(chan TaskReply, nReduce),
		TaskMap:      SafeTaskMap{m: make(map[TaskId]TaskReply)},
		MapIsDone:    make(chan bool, 1),
		ReduceIsDone: make(chan bool, 1),
		ReduceTasks:  SafeTaskMap{},
		allIsDone:    false,
	}

	go c.fillTaskQueue(files, nReduce)

	log.Println("Created Coordinator")

	c.server()
	return &c
}

func (c *Coordinator) fillTaskQueue(files []string, nReduce int) {
	c.handleMapTasks(files, nReduce)

	c.handleReduceTasks(nReduce)

	for _ = range nReduce {
		reply := TaskReply{
			Task: TaskType(Quit),
		}
		c.TaskChannel <- reply
	}
	c.allIsDone = true
}

func (c *Coordinator) handleMapTasks(files []string, nReduce int) {
	// Fill TaskMap with Tasks
	for i, file := range files {
		taskId := TaskId(fmt.Sprintf("%v-%d", TaskType(Map), i))
		reply := TaskReply{
			Task:           TaskType(Map),
			FileInputName:  file,
			FileOutputName: fmt.Sprintf("%d-intermediate.txt", i),
			NReduce:        nReduce,
			TaskNum:        i,
			TaskId:         taskId,
			IsDone:         false,
		}
		c.TaskMap.mu.Lock()
		c.TaskMap.m[taskId] = reply
		c.TaskMap.mu.Unlock()
		log.Println("Created Map Taskreply: ", reply)
	}
	// first time send all the tasks
	_ = c.sendIncompleteTasks()

	for {
		select {
		case <-c.MapIsDone:

			return
		case <-time.After(10 * time.Second):
			c.sendIncompleteTasks()
		}
	}
}

func (c *Coordinator) handleReduceTasks(nReduce int) {
	// Fill TaskMap with Tasks

	for i := range nReduce {
		taskId := TaskId(fmt.Sprintf("%v-%d", TaskType(Reduce), i))
		reply := TaskReply{
			Task:           TaskType(Reduce),
			FileInputName:  fmt.Sprintf("%d-intermediate.txt", i),
			FileOutputName: fmt.Sprintf("mr-out-%d", i),
			NReduce:        nReduce,
			TaskNum:        i,
			TaskId:         taskId,
		}

		c.TaskMap.mu.Lock()
		c.TaskMap.m[taskId] = reply
		c.TaskMap.mu.Unlock()

		log.Println("Created Reduce Taskreply: ", reply)
	}
	// first time send all the tasks
	_ = c.sendIncompleteTasks()

	for {
		select {
		case <-c.MapIsDone:
			return
		case <-time.After(10 * time.Second):
			c.sendIncompleteTasks()
		}
	}
}

func (c *Coordinator) sendIncompleteTasks() (allDone bool) {
	c.TaskMap.mu.Lock()
	allDone = true
	for _, task := range c.TaskMap.m {
		allDone = false
		c.TaskChannel <- task
	}
	c.TaskMap.mu.Unlock()
	return allDone
}
