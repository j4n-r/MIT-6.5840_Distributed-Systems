package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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

		switch reply.Task {
		case TaskKind(Map):
			log.Printf("Switch on task hit MapTask: %+v\n", reply)
			doMapTask(mapf, reply)
		case TaskKind(Reduce):
			panic("reduce not implementd")
		}
	}

}

func doMapTask(mapf func(string, string) []KeyValue, reply TaskReply) {
	data, err := os.ReadFile(reply.FileInputName)
	if err != nil {
		log.Println("Error reading file:", reply.FileInputName)
		panic("Error reading input file")
	}
	fileString := string(data)
	intermediate := mapf(reply.FileInputName, fileString)

	log.Println("Got intermediate")

	f, err := os.Create(reply.FileOutputName)
	if err != nil {
		panic("Error creating file")
	}
	for i := range intermediate {
		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, intermediate[i].Value)
	}
	log.Println("Saved intermediate file")
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
