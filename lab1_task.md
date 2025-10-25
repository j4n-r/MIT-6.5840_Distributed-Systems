
Your job is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker. There will be just one coordinator process, and one or more worker processes executing in parallel. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. 

The workers will talk to the coordinator via RPC. 

Each worker process will
in a loop: 
 - ask the coordinator for a task 
 - read the task's input from one or more files 
 - execute the task, 
 - write the task's output to one or more files, 
and again ask the coordinator for a new task. 

The coordinator 
- should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), 
- and give the same task to a different worker.


We have given you a little code to start you off. The "main" routines for the coordinator and worker are in main/mrcoordinator.go and main/mrworker.go; don't change these files. You should put your implementation in mr/coordinator.go, mr/worker.go, and mr/rpc.go.

# Rules
The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the number of reduce tasks -- the argument that main/mrcoordinator.go passes to MakeCoordinator(). Each mapper should create nReduce intermediate files for consumption by the reduce tasks.

The worker implementation should put the output of the X'th reduce task in the file mr-out-X.
A mr-out-X file should contain one line per Reduce function output. The line should be generated with the Go "%v %v" format, called with the key and value. Have a look in main/mrsequential.go for the line commented "this is the correct format". The test script will fail if your implementation deviates too much from this format.

You can modify mr/worker.go, mr/coordinator.go, and mr/rpc.go. You can temporarily modify other files for testing, but make sure your code works with the original versions; we'll test with the original versions.

The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.

main/mrcoordinator.go expects mr/coordinator.go to implement a Done() method that returns true when the MapReduce job is completely finished; at that point, mrcoordinator.go will exit.

When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from call(): if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.# MapReduce Implementation Lab

## Overview

Implement a distributed MapReduce system consisting of a coordinator and worker processes. The system will have one coordinator and multiple workers running in parallel, communicating via RPC.

## Architecture

**Coordinator**: Assigns tasks to workers and tracks completion. If a worker fails to complete a task within 10 seconds, reassigns it to another worker.

**Workers**: Continuously request tasks from the coordinator, process input files, execute Map/Reduce functions, and write results.

## Getting Started

### File Structure
- `main/mrcoordinator.go` - Coordinator main routine (don't modify)
- `main/mrworker.go` - Worker main routine (don't modify)
- `mr/coordinator.go` - Your coordinator implementation
- `mr/worker.go` - Your worker implementation
- `mr/rpc.go` - Your RPC definitions

### Running Your Code

```bash
# Build the word-count plugin
go build -buildmode=plugin ../mrapps/wc.go

# Start the coordinator
rm mr-out*
go run mrcoordinator.go pg-*.txt

# Start workers (in separate terminals)
go run mrworker.go wc.so

# Verify output
cat mr-out-* | sort | more
```

### Testing

```bash
cd ~/6.5840/src/main
bash test-mr.sh
```

**Expected output when complete:**
```
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

## Requirements

### Core Functionality

1. **Map Phase**: Divide intermediate keys into `nReduce` buckets. Each mapper creates `nReduce` intermediate files.

2. **Output Format**: 
   - Reduce task X outputs to `mr-out-X`
   - Each line: `%v %v` format (key and value)
   - See `main/mrsequential.go` for reference

3. **Intermediate Files**: Store in current directory with naming convention `mr-X-Y` (X = map task number, Y = reduce task number)

4. **Completion**: Implement `Done()` method in coordinator that returns true when job finishes

5. **Worker Termination**: Workers should exit when job completes (detect via failed RPC calls to coordinator)

## Implementation Hints

### Getting Started
- Modify `Worker()` to send RPC requesting tasks
- Coordinator responds with unstarted map task filenames
- Worker reads files and calls Map function (similar to `mrsequential.go`)

### Key/Value Storage
Use JSON encoding for intermediate files:

```go
// Writing
enc := json.NewEncoder(file)
for _, kv := range kvs {
    err := enc.Encode(&kv)
}

// Reading
dec := json.NewDecoder(file)
for {
    var kv KeyValue
    if err := dec.Decode(&kv); err != nil {
        break
    }
    kva = append(kva, kv)
}
```

### Reduce Task Selection
Use `ihash(key)` function to determine which reduce task handles a given key.

### Concurrency
- Lock shared data in coordinator (it's an RPC server)
- Test with race detector: `go run -race`
- Reduces must wait until all maps complete

### Crash Recovery
- Use temporary files with atomic rename: `os.CreateTemp()` + `os.Rename()`
- Coordinator waits 10 seconds before reassigning tasks
- Test with `mrapps/crash.go` plugin

### RPC Guidelines
- Only struct fields with capital letters are sent
- Reply structs must have default values:
  ```go
  reply := SomeType{}
  call(..., &reply)
  ```

## Debugging

- Intermediate/output files are in `mr-tmp/` subdirectory
- Modify `test-mr.sh` to exit after failing test
- Use `test-mr-many.sh` to catch low-probability bugs
- Ignore "method Done has 1 input parameters" RPC messages
- Some "connection refused" errors are expected during shutdown

## Challenge Exercises (No Credit)

1. Implement your own MapReduce application (e.g., Distributed Grep from Section 2.3)
2. Run coordinator and workers on separate machines using TCP/IP and shared filesystem
