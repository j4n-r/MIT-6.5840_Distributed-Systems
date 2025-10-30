package mr

import (
	"bufio"
	"cmp"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"slices"
	"strings"
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

	// Your worker implementation here.

	for {
		args := TaskArgs{}

		reply := TaskReply{}

		ok := call("Coordinator.DistributeTasks", &args, &reply)
		if !ok {
			panic("RPC call failed")
		}

		// doMapTask  and doReduceTask could be goroutines but for the sake
		// of running multiple workers we will not do that
		switch reply.Task {
		case TaskType(Map):
			log.Printf("Switch on task hit MapTask: %+v\n", reply)
			doMapTask(mapf, reply)
		case TaskType(Reduce):
			log.Printf("Switch on task hit ReduceTask: %+v\n", reply)
			 doReduceTask(reducef, reply)
		case TaskType(Quit):
			log.Println("got QUIT task")
			return
		}

	}

}

func doMapTask(mapf func(string, string) []KeyValue, reply TaskReply) {
	log.Printf("Entering doMapTask with: %+v\n", reply)
	data, err := os.ReadFile(reply.FileInputName)
	if err != nil {
		log.Println("Error reading file:", reply.FileInputName)
		panic("Error reading input file")
	}
	fileString := string(data)
	intermediate := mapf(reply.FileInputName, fileString)

	log.Println("Got intermediate")

	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range intermediate {
		bucket := ihash(kv.Key) % reply.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}
	// DEBUG doing this here  only for debugging
	for _, bucket := range buckets {
		slices.SortFunc(bucket, func(a, b KeyValue) int {
			return strings.Compare(a.Key, b.Key)
		})
	}
	for i, bucket := range buckets {
		writeBucketToFile(bucket, fmt.Sprintf("mr-map_%v-reduce_%v.txt", reply.TaskNum, i))
	}
	log.Println("Saved intermediate file")
	ok := call("Coordinator.TaskDone", &TaskArgs{
		TaskId: reply.TaskId,
	}, &reply)
	if !ok {
		panic("RPC map done call failed")
	}
}

func doReduceTask(reducef func(string, []string) string, reply TaskReply) {
	log.Printf("Entering doReduceTask with: %+v", reply)

	ofile, err := os.OpenFile(reply.FileOutputName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("Erro writing output file")
	}
	defer ofile.Close()
	intermediate := readFilesToIntermediate(reply)

	slices.SortFunc(intermediate, func(a, b KeyValue) int {
		return cmp.Compare(a.Key, b.Key)
	})

	i := 0
	for i < len(intermediate) {
		j := i + 1
		// Find all values for this key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ok := call("Coordinator.TaskDone", &TaskArgs{
		TaskId: reply.TaskId,
	}, &reply)
	if !ok {
		panic("RPC map done call failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args *TaskArgs, reply *TaskReply) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

func writeBucketToFile(bucket []KeyValue, fileName string) {
	file, err := os.OpenFile(fmt.Sprintf("rm-inter/%v", fileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("%v: %+v", err, fileName)
		panic("opening file")
	}
	defer file.Close()
	for _, kv := range bucket {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}

}

func readFilesToIntermediate(reply TaskReply) (intermediate []KeyValue) {
	files, err := os.ReadDir("rm-inter")
	if err != nil {
		panic("opening dir")
	}
	for _, file := range files {
		var map_task int
		var reduce_task int
		_, err = fmt.Sscanf(file.Name(), "mr-map_%d-reduce_%d.txt", &map_task, &reduce_task)
		if reduce_task == reply.TaskNum {
			file, err := os.OpenFile(fmt.Sprintf("rm-inter/%v", file.Name()), os.O_RDONLY, 0644)
			if err != nil {
				panic("Opening read intermediate file")
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				key := strings.Fields(line)[0]
				value := strings.Fields(line)[1]
				intermediate = append(intermediate, KeyValue{key, value})
			}

		}
	}
	return intermediate
}
