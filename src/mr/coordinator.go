package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	MapTasks chan TaskReply
}

func (c *Coordinator) DistributeTasks(args *TaskArgs, reply *TaskReply) error {
	log.Printf("Entered Distribute Task with %+v, and %+v\n", args, reply)
	task := <- c.MapTasks
	*reply = task
	log.Printf("TaskReply: %+v", reply)
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks: make(chan TaskReply, 10),
	}

	log.Println("Created Coordinator")

	go func() {
		for i, file := range files {
			reply := TaskReply{
				Task:           TaskKind(Map),
				FileInputName:  file,
				FileOutputName: fmt.Sprintf("%d-intermediate.txt", i),
				NReduce: nReduce,
			}
			log.Println("Created Taskreply: ", reply)
			c.MapTasks <- reply
			log.Println("TaskReply was read from channel")
		}
	}()

	c.server()
	return &c
}
