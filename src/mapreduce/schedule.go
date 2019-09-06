package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nother int // number of inputs (for reduce) or outputs (for map)
	var wg sync.WaitGroup

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nother = nReduce
	case reducePhase:
		ntasks = nReduce
		nother = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nother)

	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		rpcAddr := <-registerChan
		taskArgs := DoTaskArgs{jobName, mapFiles[i], phase, i, nother}
		go concurrentSched(rpcAddr, taskArgs, &wg, registerChan, ntasks)

	}
	wg.Wait()

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part 2, 2B).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}

func concurrentSched(rpcAddr string, taskArgs DoTaskArgs, wg *sync.WaitGroup, registerChan chan string, numtasks int) {
	success := call(rpcAddr, "Worker.DoTask", taskArgs, nil)
	if success {
		go func() {
			registerChan <- rpcAddr
		}()
		wg.Done()
	} else {
		rpcAddr2 := <-registerChan
		go concurrentSched(rpcAddr2, taskArgs, wg, registerChan, numtasks)
	}
}
