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

type SafeDone struct {
	mu    sync.Mutex
	value bool
}

type Master struct {
	// Your definitions here.
	taskChan chan TaskMessage
	tasks    SafeTaskMap
	done     SafeDone
}

func (m *Master) redoTasks() {
	for {
		var tasksToRedo []int
		for range 10 {
			tasksToRedo = m.getTasksToRedo()
			slog.Debug("RedoTasks", "TasksToRedo", tasksToRedo)
			if len(tasksToRedo) == 0 {
				slog.Debug("No Tasks To redo anymore, returning")
				return
			}
			time.Sleep(1 * time.Second)
		}

		if len(tasksToRedo) != 0 {
			slog.Debug("RedoTasks", "Redoing tasks", tasksToRedo)
			for id := range tasksToRedo {
				m.tasks.mu.Lock()
				m.taskChan <- m.tasks.m[id]
				m.tasks.mu.Unlock()
			}
		}
	}
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

	m.redoTasks()

	slog.Debug("Map Tasks Done")
	m.createReduceTasks(nReduce)
}

func (m *Master) createReduceTasks(nReduce int) {
	for i := range nReduce {
		task := TaskMessage{
			// the Tasknum should be enough to get the right files
			// it has to take the second number in the intermediate file
			// i.e. the bucket number
			TaskNum: i,
			TaskT:   TaskType(ReduceTask),
			NReduce: nReduce,
			Done:    false,
		}
		m.tasks.mu.Lock()
		m.tasks.m[i] = task
		m.tasks.mu.Unlock()
		slog.Info("CreateReduceTask", "ReduceTask:", task)
		m.taskChan <- task
	}

	m.redoTasks()

	m.done.mu.Lock()
	m.done.value = true
	m.done.mu.Unlock()
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
func (m *Master) DistributeTasks(args *UnusedArgs, reply *TaskMessage) error {
	select {
	case task := <-m.taskChan:
		slog.Info("Distributed", "TaskMessage", task)
		*reply = task
	default:
		reply.TaskT = NoTask
	}
	return nil
}

func (m *Master) ReportTaskComplete(args *TaskMessage, reply *UnusedArgs) error {
	m.tasks.mu.Lock()
	task := m.tasks.m[args.TaskNum]
	task.Done = true
	m.tasks.m[args.TaskNum] = task
	m.tasks.mu.Unlock()
	return nil
}

func (m *Master) AllDone(args *UnusedArgs, reply *DoneMessage) error {
	m.done.mu.Lock()
	reply.AllDone = m.done.value
	m.done.mu.Unlock()
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
	m.done.mu.Lock()
	defer m.done.mu.Unlock()
	return m.done.value

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
		done: SafeDone{
			mu:    sync.Mutex{},
			value: false,
		},
	}

	go m.createMapTasks(files, nReduce)

	m.server()
	return &m
}
