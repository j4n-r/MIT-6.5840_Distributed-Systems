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
			log.Printf("Switch on task hit ReduceTask: %+v\n", reply)
		case TaskKind(Done):
			panic("Done\n")
		}
	}

}

func doMapTask(mapf func(string, string) []KeyValue, reply TaskReply) {
	log.Println("Entering doMapTask with: %+v", reply)
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
	for i, bucket := range buckets {
		writeBucketToFile(bucket, fmt.Sprintf("%d-intermediate.txt", i))
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

func writeBucketToFile(bucket []KeyValue, fileName string) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("%v: %+v", err, fileName)
		panic("opening file")
	}
	for _, kv := range bucket {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}

}
