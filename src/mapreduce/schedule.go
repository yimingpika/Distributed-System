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
	fmt.Println("mapFiles => ", len(mapFiles))
	fmt.Println("nReduce => ", nReduce)
	fmt.Println("phase => ", phase)
	fmt.Println("registerChan => ", registerChan)
	// chan means channel
	// Ref to know more: https://colobu.com/2016/04/14/Golang-Channels/
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	var wg sync.WaitGroup
	for i:=0; i<ntasks; i++{
		wg.Add(1)
		go func(i int) {
			// Decrement the counter when the goroutine completes.
			dotaskargs := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}

			// workerAddress: /var/tmp/824-501/mr1638-worker0 / 1
			result := false
			var workerAddress string

			for result!=true {
				// To tell a worker to execute a task by sending RPC to the worker
				// result is either true or false
				workerAddress = <- registerChan
				result  = call(workerAddress, "Worker.DoTask", dotaskargs, nil)
			}

			// We need to first decrease the value by 1, then assign it to another goroutine
			wg.Done()
			// After that, re-assign the worker to the channel
			registerChan <- workerAddress
		}(i)
	}

	// Wait for all tasks to be complete
	wg.Wait()

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part 2, 2B).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
