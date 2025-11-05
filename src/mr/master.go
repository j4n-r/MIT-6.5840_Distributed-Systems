package mr

import (
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type SafeTaskMap struct {
	mu sync.Mutex
	m  map[int]TaskMessage
}

type Master struct {
	// Your definitions here.
	taskChan chan TaskMessage
	tasks    SafeTaskMap
	done     bool
}

func (m *Master) createTasks(files []string, nReduce int) {
	for i, file := range files {
		task := TaskMessage{
			TaskNum:       i,
			TaskT:         TaskType(MapTask),
			NReduce:       nReduce,
			InputFileName: file,
			Done:          false,
		}
		m.tasks.mu.Lock()
		m.tasks.m[i] = task
		m.tasks.mu.Unlock()
		m.taskChan <- task
	}
	// check if all tasks are done
	time.Sleep(5 * time.Second)
	slog.Debug("Map Tasks Done")
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) DistributeTasks(args *ExampleArgs, reply *TaskMessage) error {
	select {
	case task := <-m.taskChan:
		*reply = task
	default:
		reply.TaskT = NoTask
	}
	return nil
}

func (m *Master) ReportTaskComplete(args *TaskMessage, reply *ExampleReply) error {
	m.tasks.mu.Lock()
	m.tasks.m[args.TaskNum] = *args
	m.tasks.mu.Unlock()
	return nil
}

func (m *Master) AllDone(args *ExampleArgs, reply *DoneMessage) error {
	reply.AllDone = m.done
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.done
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	m := Master{
		taskChan: make(chan TaskMessage),
		tasks:    SafeTaskMap{
			mu: sync.Mutex{},
			m:  map[int]TaskMessage{},
		},
		done:     false,
	}

	go m.createTasks(files, nReduce)

	m.server()
	return &m
}
