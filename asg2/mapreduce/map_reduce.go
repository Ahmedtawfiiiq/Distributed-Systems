package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file assigned to this task
	nReduce int, // The number of reduce tasks that will be run
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	// Step 1: Read the input file
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}
	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}

	// Step 2: Call the user-defined map function
	kvs := mapFn(inputFile, string(contents))

	// Step 3: Create a slice to hold all the intermediate files
	intermediateFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce) // create a new JSON encoder for each intermediate file
	for i := 0; i < nReduce; i++ {
		fileName := getIntermediateName(jobName, mapTaskIndex, i) // get the name of the file corresponding to the i-th reduce task
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatal(err)
		}
		intermediateFiles[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	// Step 4: Write the intermediate key/value pairs to the appropriate intermediate files
	for _, kv := range kvs {
		reduceTaskIndex := int(hash32(kv.Key)) % nReduce
		err := encoders[reduceTaskIndex].Encode(kv)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Step 5: Close all the intermediate files
	for i := 0; i < nReduce; i++ {
		err := intermediateFiles[i].Close()
		if err != nil {
			log.Fatal(err)
		}
	}
}
func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks that were run
	reduceFn func(key string, values []string) string,
) {
	// Step 1: Create a slice to hold all the intermediate key/value pairs
	kvs := make([]KeyValue, 0)

	// Step 2: Read all the intermediate files
	for i := 0; i < nMap; i++ {
		// get the name of the file corresponding to the i-th map task
		fileName := getIntermediateName(jobName, i, reduceTaskIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		decoder := json.NewDecoder(file) // create a new JSON decoder
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break // there are no more key/value pairs in the file
			}
			kvs = append(kvs, kv) // group all the key/value pairs into a single slice
		}
		err = file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}

	// Step 3: Sort all the intermediate key/value pairs by key
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	// Step 4: Call the user-defined reduce function for each unique key
	kvsMap := make(map[string][]string)
	for _, kv := range kvs {
		kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
	}
	for key, values := range kvsMap {
		output := reduceFn(key, values)
		kvs = append(kvs, KeyValue{key, output})
	}

	// Step 5: Write the output of the reduce function to the output file
	outputFileName := getReduceOutName(jobName, reduceTaskIndex)
	file, err := os.Create(outputFileName)
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		if kv.Key != "" {
			output := reduceFn(kv.Key, []string{kv.Value})
			enc.Encode(KeyValue{kv.Key, output})
		}
	}

	// Step 6: Close the output file
	err = file.Close()
	if err != nil {
		log.Fatal(err)
	}
}
