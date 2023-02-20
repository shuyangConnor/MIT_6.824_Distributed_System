package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	NumReduce      int             // Number of reduce tasks
	Files          []string        // Files for map tasks, len(Files) is number of Map tasks
	MapTasks       chan MapTask    // Channel for uncompleted map tasks
	ReduceTasks    chan ReduceTaskReply
	CompletedTasks map[string]bool // Map to check if task is completed
	CompletedReduce map[int]bool
	Lock           sync.Mutex      // Lock for contolling shared variables
	reducePhase    bool
}

// Starting coordinator logic
func (c *Coordinator) Start() {
	// fmt.Println("Starting Coordinator, adding Map Tasks to channel")
	// Prepare initial MapTasks and add them to the queue
	for index, file := range c.Files {
		mapTask := MapTask{
			Filename:  file,
			NumReduce: c.NumReduce,
			Id: index + 1,
		}

		// fmt.Println("MapTask", mapTask, "added to channel")
		c.CompletedTasks["map_"+mapTask.Filename] = false
		c.MapTasks <- mapTask
		
	}
	c.CompletedReduce = make(map[int]bool)
	for i:=0;i<c.NumReduce;i++{
		reduceTask := ReduceTaskReply{
			ReducePhase: false,
			TaskNum: i,
			NumFiles: len(c.Files),
		}
		// fmt.Println("ReduceTask", reduceTask, "added to channel")
		c.CompletedReduce[i] = false
		c.ReduceTasks <- reduceTask
		
	}
	c.reducePhase = false
	c.server()
}

// RPC that worker calls when idle (worker requests a map task)
func (c *Coordinator) RequestMapTask(args *EmptyArs, reply *MapTask) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	// fmt.Println("Map task requested")
	if c.reducePhase == true{
		// fmt.Println("Reduce phase started")
		*reply = MapTask{
			Filename: "Reduce phase started",
			NumReduce: c.NumReduce,
		}
		return nil
	}
	if len(c.MapTasks) == 0{
		// fmt.Println("Map task all assigned")
	  *reply = MapTask{
			Filename: "Map task all assigned",
			NumReduce: c.NumReduce,
		}
		return nil
	}
	task, _ := <-c.MapTasks // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt

	// fmt.Println("Map task found,", task.Filename)
	*reply = task

	go c.WaitForWorker(task)

	return nil
}

func (c *Coordinator) RequestReduceTask(args *EmptyArs, reply *ReduceTaskReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	// fmt.Println("Reduce task requested")
	if len(c.ReduceTasks) == 0{
		// fmt.Println("Reduce task all assigned")
	  *reply = ReduceTaskReply{
			ReducePhase: true,
			TaskNum: -1,
			NumFiles: len(c.Files),
		}
		return nil
	}
	task, _ := <-c.ReduceTasks // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt

	// fmt.Println("Reduce task found,", task.TaskNum)
	*reply = task
	go c.WaitForReduceWorker(task)

	return nil
}

// Goroutine will wait 10 seconds and check if map task is completed or not
func (c *Coordinator) WaitForWorker(task MapTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedTasks["map_"+task.Filename] == false {
		// fmt.Println("Timer expired, task", task.Filename, "is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
}

func (c *Coordinator) WaitForReduceWorker(task ReduceTaskReply) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedReduce[task.TaskNum] == false {
		// fmt.Println("Timer expired, task ",task.TaskNum, "is not finished. Putting back in queue.")
		c.ReduceTasks <- task
	}
	c.Lock.Unlock()
}

// RPC for reporting a completion of a task
func (c *Coordinator) TaskCompleted(args *MapTask, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedTasks["map_"+args.Filename] = true

	// fmt.Println("Task", args, "completed")

	// If all of map tasks are completed, go to reduce phase
	// ...
	for _, value := range c.CompletedTasks{
		if (value == false){
			return nil
		}
	}
	c.reducePhase = true
	return nil
}

func (c *Coordinator) ReduceTaskCompleted(args *ReduceTaskReply, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.CompletedReduce[args.TaskNum] = true

	// fmt.Println("Reduce task", args, "completed")

	// If all of reduce tasks are completed, turn down the server
	// ...
	c.Done();
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.
	for i:= 0; i < len(c.CompletedReduce); i++{
		if (c.CompletedReduce[i] == false) {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:      nReduce,
		Files:          files,
		MapTasks:       make(chan MapTask, 100),
		ReduceTasks:		make(chan ReduceTaskReply, 100),
		CompletedTasks: make(map[string]bool),
		CompletedReduce: make(map[int]bool),
	}

	// fmt.Println("Starting coordinator")

	c.Start()

	return &c
}
