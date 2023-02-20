package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
	"strconv"
	"encoding/json"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func lessFunc(kv1, kv2 *KeyValue) bool {
	return kv1.Key < kv2.Key
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	reducePhase bool
	reduceTask int
	NumReduce int
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := WorkerSt{
		mapf:    mapf,
		reducef: reducef,
		reducePhase: false,
		reduceTask: 1,
		NumReduce: 10,
	}

	w.RequestMapTask()
}

// Requests map task, tries to do it, and repeats
func (w *WorkerSt) RequestMapTask() {
	for {
			args := EmptyArs{}
			reply := MapTask{}
			call("Coordinator.RequestMapTask", &args, &reply)
			if reply.Filename == "Reduce phase started"{
				w.reducePhase = true
				w.reduceTask = reply.NumReduce
				w.RequestReduceTask()
				break
			}
			if reply.Filename == "Map task all assigned" {
				time.Sleep(1 * time.Second)
				continue
			} 
		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()
		w.NumReduce = reply.NumReduce
		kva := w.mapf(reply.Filename, string(content))

		// store kva in multiple files according to rules described in the README
		// ...
		var outputFiles []*json.Encoder

	  for i := 0; i < reply.NumReduce; i++ {
			oname := "mr-" + strconv.Itoa(reply.Id) + "-" + strconv.Itoa(i)
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(ofile)
			outputFiles = append(outputFiles, enc)
		}

		for i:=0; i < len(kva); i++{
			y := ihash(kva[i].Key) % reply.NumReduce
			outputFiles[y].Encode(&kva[i])
		}

		// fmt.Println("Map task for", reply.Filename, "completed")
		// fmt.Println(kva)

		emptyReply := EmptyReply{}
		call("Coordinator.TaskCompleted", &reply, &emptyReply)
	}
}

func (w *WorkerSt) RequestReduceTask(){
	for {
		args := EmptyArs{}
		reply := ReduceTaskReply{}
		call("Coordinator.RequestReduceTask", &args, &reply)
		if reply.TaskNum <0{
			time.Sleep(1 * time.Second)
				continue
		}
		kva := make([]KeyValue, 0)
		for i := 1; i <= reply.NumFiles; i++{
			oname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskNum)
			ofile, _ := os.Open(oname)
			dec := json.NewDecoder(ofile)
 			for {
    	var kv KeyValue
    	if err := dec.Decode(&kv); err != nil {
      	break
    	}
    	kva = append(kva, kv)
			}
		}
		sort.Slice(kva, func(i, j int) bool {
			return lessFunc(&kva[i], &kva[j])
		})
		oname := "mr-out-" + strconv.Itoa(reply.TaskNum)
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
			output := w.reducef(kva[i].Key, values)
	
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			i = j
		}
		emptyReply := EmptyReply{}
		call("Coordinator.ReduceTaskCompleted", &reply, &emptyReply)
	}
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

	// fmt.Println(err)
	return false
}
