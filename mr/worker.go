package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		var outputFiles []string
		var requestTaskArgs RequestTaskArgs
		var requestReply RequestTaskReply
		if !call("Coordinator.RequestTask", &requestTaskArgs, &requestReply) {
			return // exit
		}
		if requestReply.TaskType == WAIT {
			time.Sleep(time.Second)
			continue
		} else if requestReply.TaskType == EXIT {
			return // exit
		} else {
			// process task
			if requestReply.TaskType == MAP {
				outputFiles = append(outputFiles, mapTask(mapf, requestReply.File[0], requestReply.NReduce, requestReply.TaskIndex)...)
			} else if requestReply.TaskType == REDUCE {
				outputFiles = append(outputFiles, reduceTask(reducef, requestReply.File, requestReply.NReduce, requestReply.TaskIndex))
			}
		}

		if !call("Coordinator.ReportTask", TaskManager{requestReply.TaskIndex, COMPLETED, requestReply.File, outputFiles}, nil) {
			return // exit
		}
	}
}

// process map task
func mapTask(mapf func(string, string) []KeyValue, filename string, nReduce int, taskIndex uint8) []string {
	// open the file and read the content
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("cannot read %v", filename)
	}
	file.Close()

	// call the map function
	kva := mapf(filename, string(content))

	// map the key value to the nReduce sequence
	var hash_kv [][]KeyValue = make([][]KeyValue, nReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		hash_kv[index] = append(hash_kv[index], kv)
	}

	// create nReduce intermediate files
	var tempFiles []os.File
	for i := 0; i < nReduce; i++ {
		tempFile, _ := os.CreateTemp("./", "mr-tmp-*")
		tempFiles = append(tempFiles, *tempFile)
		defer os.Remove(tempFile.Name())
	}

	// write the intermediate files
	for i, kv := range hash_kv {
		enc := json.NewEncoder(&tempFiles[i])
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("cannot encode %v", kv)
		}
	}
	// rename the temp file
	var outputFiles []string
	for i, tempFile := range tempFiles {
		tempFile.Close()
		os.Rename(tempFile.Name(), fmt.Sprintf("./mr-%d-%d", taskIndex, i))
		outputFiles = append(outputFiles, fmt.Sprintf("./mr-%d-%d", taskIndex, i))
	}

	return outputFiles
}

// process reduce task
func reduceTask(reducef func(string, []string) string, filenames []string, nReduce int, taskIndex uint8) string {
	// read the intermediate files
	var kva []KeyValue
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("cannot open %v", filename)
		}
		var kvt []KeyValue
		dec := json.NewDecoder(file)
		dec.Decode(&kvt)
		kva = append(kva, kvt...)
		file.Close()
	}

	// sort the intermediate keyValue
	sort.Sort(ByKey(kva))

	// output temp file
	tempFile, _ := os.CreateTemp("./", "mr-tmp-*")

	// call the reduce function
	for i := 0; i < len(kva); {
		j := i + 1
		values := []string{kva[i].Value}

		for j < len(kva) && kva[j].Key == kva[i].Key {
			values = append(values, kva[j].Value)
			j++
		}

		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	// rename the temp
	tempFile.Close()
	os.Rename(tempFile.Name(), fmt.Sprintf("./mr-out-%d", taskIndex))
	return fmt.Sprintf("./mr-out-%d", taskIndex)
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {
// 	// declare an argument structure.
// 	args := ExampleArgs{}
// 	// fill in the argument(s).
// 	args.X = 99
// 	// declare a reply structure.
// 	reply := ExampleReply{}
// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

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
