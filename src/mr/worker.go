package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"log/slog"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

		switch reply.TaskT {
		case MapTask:
			slog.Info("Got Map Task", "TaskMesage", &reply)
			go doMapTask(&reply, mapf)
			call("Master.ReportTaskComplete", &reply, &ExampleReply{})
		case ReduceTask:
			slog.Info("Got Reduce Task", "TaskMesage", &reply)
			go doReduceTask(&reply, reducef)
			call("Master.ReportTaskComplete", &reply, &ExampleReply{})
		// if the master has no task for us we check if the we only need to wait
		// or if all tasks are done
		case NoTask:
			slog.Info("Got Done Task, Waiting")
			var doneMsg DoneMessage
			call("Master.AllDone", &ExampleArgs{}, &doneMsg)
			if doneMsg.AllDone {
				return
			}
		}
		time.Sleep(1 * time.Second)
	}

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
}
func doReduceTask(taskMessage *TaskMessage, reducef func(string, []string) string) {

	// 1. Read all json files with the bucket/reduce number into intermediate []KeyValue
	intermediate := readIntermediateFiles(taskMessage.TaskNum)

	// 2. Sort the []KeyValue using the Key
	// sort.Slice(intermediate, func(i, j int) bool {
	// 	return intermediate[i].Key < intermediate[j].Key
	// })
	sort.Sort(ByKey(intermediate))

	// loop from sequential
	tmpFile, err := os.CreateTemp(".", "tmp-out-*")
	if err != nil {
		panic("cannot create temp file")
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	err = os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", taskMessage.TaskNum))
	if err != nil {
		panic("Cannot rename file")
	}
}

func readIntermediateFiles(taskNum int) []KeyValue {
	var kva []KeyValue
	dir, err := os.ReadDir(".")
	if err != nil {
		panic("Cannot read dir")
	}
	pattern := fmt.Sprintf(`^inter-(\d+)-%d$`, taskNum)
	re := regexp.MustCompile(pattern)
	for _, dir_entry := range dir {
		if dir_entry.IsDir() {
			continue
		}
		if re.MatchString(dir_entry.Name()) {
			file, err := os.Open(dir_entry.Name())
			if err != nil {
				panic("cannot open dir file")
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
	}
	return kva
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
