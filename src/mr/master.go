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

func (m *Master) createMapTasks(files []string, nReduce int) {
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
	time.Sleep(5 * time.Second)
	tasksToRedo := m.getTasksToRedo()
	if len(tasksToRedo) != 0 {
		slog.Error("CreateMapTasks", "tasksToRedo", tasksToRedo)
	}
	slog.Debug("Map Tasks Done")
	m.createReduceTasks(nReduce)
}

func (m *Master) createReduceTasks(nReduce int) {
	for i := range nReduce {
		task := TaskMessage{
			// the Tasknum should be enough to get the right files
			// it has to take the second number in the intermediate file
			// i.e. the bucket number
			TaskNum:       i,
			TaskT:         TaskType(ReduceTask),
			NReduce:       nReduce,
			Done:          false,
		}
		m.tasks.mu.Lock()
		m.tasks.m[i] = task
		m.tasks.mu.Unlock()
		slog.Info("CreateReduceTask", "ReduceTask:", task)
		m.taskChan <- task
	}
	time.Sleep(5 * time.Second)
	tasksToRedo := m.getTasksToRedo()
	if len(tasksToRedo) != 0 {
		slog.Error("CreateReduceTasks", "tasksToRedo", tasksToRedo)
	}
	m.done = true
}

// returns a slice of task numbers or empty if none
func (m *Master) getTasksToRedo() (tasksToRedo []int) {
	m.tasks.mu.Lock()
	for i, task := range m.tasks.m {
		if !task.Done {
			tasksToRedo = append(tasksToRedo, i)
		}
	}
	m.tasks.mu.Unlock()
	return tasksToRedo
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) DistributeTasks(args *ExampleArgs, reply *TaskMessage) error {
	select {
	case task := <-m.taskChan:
		*reply = task
	default:
		reply.TaskT = NoTask
	}
	slog.Info("Distributed", "TaskMessage", reply)
	return nil
}

func (m *Master) ReportTaskComplete(args *TaskMessage, reply *ExampleReply) error {
	m.tasks.mu.Lock()
	task := m.tasks.m[args.TaskNum]
	task.Done = true
	m.tasks.m[args.TaskNum] = task
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
		tasks: SafeTaskMap{
			mu: sync.Mutex{},
			m:  map[int]TaskMessage{},
		},
		done: false,
	}

	go m.createMapTasks(files, nReduce)

	m.server()
	return &m
}
