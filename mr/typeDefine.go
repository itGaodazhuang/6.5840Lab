package mr

import (
	"sync"
	"time"
)

// states of the task
type TaskState uint8

const (
	UNDISPATCHED TaskState = iota
	INPROGRESS
	COMPLETED
)

// phase of the coordinator
type Phase uint8

const (
	MAPPHASE Phase = iota
	REDUCEPHASE
	DONE
)

// type of the task
type TaskType uint8

const (
	MAP TaskType = iota
	REDUCE
	EXIT
	WAIT
)

// structure for task manager
type TaskManager struct {
	Index uint8
	//UNDISPATCHED, INPROGRESS, COMPLETED
	State      TaskState
	InputFile  []string
	OutputFile []string
}

// structure for coordinator
type Coordinator struct {
	// Your definitions here.
	NReduce            int
	State              Phase
	CompletedTaskCount int
	Files              []string
	Tim                []time.Timer
	TaskList           []uint8
	MapManager         []TaskManager
	ReduceManager      []TaskManager
	Mu                 sync.Mutex
}
