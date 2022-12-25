package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

type Reply struct {
	TaskDone bool
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.

	<-mr.registerChannel
	var wg sync.WaitGroup
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		dtArgs := DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nios,
		}
		var dtRply Reply
		go func(taskNum int) {
			mr.Lock()
			workerId := taskNum % len(mr.workers)
			ok := call(mr.workers[workerId], "Worker.DoTask", dtArgs, &dtRply)
			mr.Unlock()
			for {
				if ok == false {
					fmt.Printf("Schedule: Phase: %s TaskNumber: %v error\n", phase, taskNum)
					workerId = (workerId + 1) % len(mr.workers)
					ok = call(mr.workers[workerId], "Worker.DoTask", dtArgs, &dtRply)
				} else {
					time.Sleep(200 * time.Millisecond)
					if dtRply.TaskDone == true {
						wg.Done()
						break
					} else {
						mr.Lock()
						workerId = (workerId + 1) % len(mr.workers)
						ok = call(mr.workers[workerId], "Worker.DoTask", dtArgs, &dtRply)
						mr.Unlock()
					}
				}
			}
		}(i)
	}
	wg.Wait()
	debug("Schedule: %v phase done\n", phase)
}
