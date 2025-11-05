package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"log/slog"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// get task
	args := ExampleArgs{}
	reply := TaskMessage{}
	for {
		call("Master.DistributeTasks", &args, &reply)
		slog.Info("Got Task", "TaskMesage", reply)

		switch reply.TaskT {
		case MapTask:
			doMapTask(&reply, mapf)
		case ReduceTask:
			doReduceTask(&reply, reducef)
		// if the master has no task for us we check if the we only need to wait
		// or if all tasks are done
		case NoTask:
			var doneMsg DoneMessage
			call("Master.AllDone", &ExampleArgs{}, &doneMsg)
			if doneMsg.AllDone {
				return
			}
		}
		time.Sleep(1 * time.Second)
	}

}

func doReduceTask(taskMessage *TaskMessage, reducef func(string, []string) string) {
	panic("unimplemented")
}

func doMapTask(taskMessage *TaskMessage, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	content, err := os.ReadFile(taskMessage.InputFileName)
	if err != nil {
		slog.Error("cannot read", "inputFile", taskMessage.InputFileName)
	}
	kva := mapf(taskMessage.InputFileName, string(content))
	intermediate = append(intermediate, kva...)
	// split into buckets
	buckets := make([][]KeyValue, taskMessage.NReduce)
	for _, kv := range intermediate {
		bucketKey := ihash(kv.Key) % taskMessage.NReduce
		buckets[bucketKey] = append(buckets[bucketKey], kv)
	}
	var tmpFiles []*os.File
	for _, bucket := range buckets {
		f, err := os.CreateTemp(".", "intermediate-tmp-*")
		if err != nil {
			panic("Panic during CreateTmp")
		}
		tmpFiles = append(tmpFiles, f)
		enc := json.NewEncoder(f)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				panic("panic encoding json")
			}
		}
	}
	for reduceNum, tmpFile := range tmpFiles {
		tmpFile.Close()
		filename := fmt.Sprintf("inter-%d-%d", taskMessage.TaskNum, reduceNum)
		os.Rename(tmpFile.Name(), filename)
	}
	taskMessage.Done = true
	call("Master.ReportTaskComplete", &taskMessage, &ExampleReply{})
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
