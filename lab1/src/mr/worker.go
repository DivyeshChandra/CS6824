package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// KeyValue is the output of map
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type byKey []KeyValue

// for sorting by key.
func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func runMap(task *Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	file.Close()
	fmt.Printf("Mapping words in %v \n", task.File)
	kva := mapf(task.File, string(content))
	for _, kv := range kva {
		fileName := "mr-map-" + strconv.Itoa(ihash(kv.Key)%10)
		fileOut, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(fileOut)
		err = enc.Encode(&kv)
		if err != nil {
			fmt.Printf("Error writing %v %v to file in map", kv.Key, kv.Value)
		}
		fileOut.Close()
	}
	go submitTaskResult(task)
}

func runReduce(task *Task, reducef func(string, []string) string) {
	kva := []KeyValue{}
	fileN := task.File
	file, _ := os.Open(fileN)
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	sort.Sort(byKey(kva))
	Sfile, _ := os.Create("sorted-" + fileN)
	enc := json.NewEncoder(Sfile)
	for _, kv := range kva {
		enc.Encode(kv)
	}
	Sfile.Close()
	oname := "mr-out-" + task.TaskID
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	go submitTaskResult(task)
}

func submitTaskResult(task *Task) {
	var submitTaskResult = &SubmitResponse{
		Reponse: 0,
	}
	call("Coordinator.SubmitTaskResult", task, submitTaskResult)
	//fmt.Printf("Finished %s task\n", task.TaskType)
	return
}

//Worker constructor main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	request := &RequestTask{
		WorkerID: rand.Intn(1000),
	}
	task := &Task{
		TaskType: "",
	}
	for call("Coordinator.GetTask", request, task) {
		switch {
		case task.TaskType == "":
			fmt.Println("No response from coordinator")
		case task.TaskType == "map":
			runMap(task, mapf)
		case task.TaskType == "reduce":
			runReduce(task, reducef)
		default:
			fmt.Println("Wtf is happening")
		}
		task = &Task{
			TaskType: "",
		}
		time.Sleep(300 * time.Millisecond)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
