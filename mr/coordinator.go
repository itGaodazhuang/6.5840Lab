package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

var DEBUG bool = false

//
// RPC handlers for the worker to call.
//

// Error
func (c *Coordinator) Error() string {
	return "error"
}

// RPC handler for worker to request a task
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// a abstract task manager to change task info
	var curTask *TaskManager

	c.Mu.Lock()
	// set the task type and related info
	if len(c.TaskList) == 0 { //wait or exit
		if c.State != DONE {
			reply.TaskType = WAIT
			c.Mu.Unlock()
			return nil
		} else {
			reply.TaskType = EXIT
			c.Mu.Unlock()
			return nil
		}
		//
		// it's a so handsome way to do this, exhaust one day to fix it!!!!!!!!!!!!!!!!!!!
		// test-early exit
		//
		// if c.State == MAPPHASE {
		// 	reply.TaskType = WAIT
		// 	c.Mu.Unlock()
		// 	return nil
		// } else {
		// 	reply.TaskType = EXIT
		// 	c.Mu.Unlock()
		// 	return nil
		// }
	}
	//dispatch task
	if c.State == MAPPHASE {
		reply.TaskType = MAP
		curTask = &c.MapManager[c.TaskList[0]]
	} else if c.State == REDUCEPHASE {
		reply.TaskType = REDUCE
		curTask = &c.ReduceManager[c.TaskList[0]]
	}
	reply.TaskIndex = c.TaskList[0]
	reply.File = curTask.InputFile
	reply.NReduce = c.NReduce

	// change the task State
	c.TaskList = c.TaskList[1:]
	curTask.State = INPROGRESS

	// start timer
	c.Tim = append(c.Tim, *time.AfterFunc(10*time.Second, func() { c.ExceedTime(curTask) }))

	// debug
	if DEBUG {
		fmt.Println("===========================================")
		fmt.Println(curTask, "has been dispatched, the tasklist is: ", c.TaskList)
		fmt.Println("===========================================")
	}
	c.Mu.Unlock()

	return nil
}

// RPC handler for worker to set a timer
func (c *Coordinator) ExceedTime(taskManager *TaskManager) error {
	if taskManager.State == INPROGRESS {
		// change the task State to UNDISPATCHED
		c.Mu.Lock()
		taskManager.State = UNDISPATCHED
		c.TaskList = append(c.TaskList, taskManager.Index)

		if DEBUG {
			fmt.Println("===========================================")
			fmt.Println(taskManager, "has been reset current task list is: ", c.TaskList)
			fmt.Println("===========================================")
		}
		c.Mu.Unlock()

	}
	return nil
}

// RPC handler for worker to report a task
func (c *Coordinator) ReportTask(reportTask TaskManager, reply *uint8) error {
	// a abstract task manager to change task info
	var curTask *TaskManager
	c.Mu.Lock()
	if c.State == MAPPHASE {
		curTask = &c.MapManager[reportTask.Index]
	} else if c.State == REDUCEPHASE {
		curTask = &c.ReduceManager[reportTask.Index]
	}

	if curTask.State != INPROGRESS {
		return nil
	}
	// update the coordinator
	curTask.State = reportTask.State
	curTask.OutputFile = reportTask.OutputFile
	c.CompletedTaskCount--

	// process the State change
	if c.CompletedTaskCount == 0 {
		if c.State == MAPPHASE {
			c.initReduceTask()
			c.State = REDUCEPHASE
		} else if c.State == REDUCEPHASE {
			c.State = DONE
		}
	}

	// debug
	if DEBUG {
		fmt.Println("===========================================")
		fmt.Println(curTask, "has been reported, the tasklist is: ", c.TaskList)
		fmt.Println("the current phase is: ", c.State)
		fmt.Println("===========================================")
	}

	c.Mu.Unlock()

	return nil
}

// methods of the coordinator

// init the reduce task
func (c *Coordinator) initReduceTask() {

	for i := 0; i < c.NReduce; i++ {
		var files []string
		for _, v := range c.MapManager {
			files = append(files, v.OutputFile[i])
		}
		c.TaskList = append(c.TaskList, uint8(i))
		c.ReduceManager = append(c.ReduceManager, TaskManager{uint8(i), UNDISPATCHED, files, []string{}})
	}
	c.CompletedTaskCount = c.NReduce

	// debug
	if DEBUG {
		fmt.Println("the reduce task list is initiated: ", c.TaskList)
	}
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
	ret := false
	if c.State == DONE {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, NReduce int) *Coordinator {

	// instantiate a coordinator
	c := new(Coordinator)

	//init the coordinator
	c.NReduce = NReduce
	c.Files = files
	c.State = MAPPHASE
	for i, v := range files {
		c.TaskList = append(c.TaskList, uint8(i))
		c.MapManager = append(c.MapManager, TaskManager{uint8(i), UNDISPATCHED, []string{v}, []string{}})
	}
	c.CompletedTaskCount = len(c.MapManager)

	if DEBUG {
		fmt.Println("===========================================")
		fmt.Println("the map task list is initiated: ", c.TaskList)
		fmt.Println("===========================================")
	}

	// register RPC
	c.server()

	return c
}
