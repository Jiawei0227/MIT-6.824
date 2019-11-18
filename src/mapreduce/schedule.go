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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	todoChannel := make(chan int)
	go func() {
		for i := 0; i < ntasks; i++ {
			todoChannel <- i
		}
	}()
	fmt.Printf("Adding %v tasks done \n", ntasks)

	var wg sync.WaitGroup
	wg.Add(ntasks)

	// need to run in a goroutine because fetching is blocked = =
	go func() {
		for i := range todoChannel {
			registerWorker := <-registerChan
			go func(task int) {
				args := DoTaskArgs{
					JobName:       jobName,
					File:          mapFiles[task],
					Phase:         phase,
					TaskNumber:    task,
					NumOtherPhase: n_other,
				}
				re := call(registerWorker, "Worker.DoTask", &args, nil)
				defer func() { registerChan <- registerWorker }()
				if re {
					fmt.Printf("Task %v done \n", task)
					wg.Done()
				} else {
					todoChannel <- task
				}
			}(i)
		}
	}()


	fmt.Printf("Waiting for all to finish?")

	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
