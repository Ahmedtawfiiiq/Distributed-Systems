package mapreduce

import "sync"

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1) // Increment the counter
		args := RunTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: numOtherPhase,
		} // Create a new RunTaskArgs object

		// Create a goroutine for each task (map or reduce)
		go func(args RunTaskArgs) {
			defer wg.Done() // Decrease the counter when the goroutine completes
			for {
				worker := <-mr.registerChannel
				ok := call(worker, "Worker.RunTask", args, nil)

				// If the RPC call was successful, add the worker back to the registerChannel
				if ok {
					go func() {
						mr.registerChannel <- worker
					}()
					break
				}
			}
		}(args)
	}
	wg.Wait() // Wait for all the goroutines to complete

	debug("Schedule: %v phase done\n", phase)
}
