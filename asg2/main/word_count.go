package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strconv"
	"strings"
)

func mapFn(docName string, value string) []mapreduce.KeyValue {
	kvs := make([]mapreduce.KeyValue, 0) // slice of KeyValue
	words := strings.Fields(value)       // split the value into words
	for _, word := range words {
		if len(word) >= 8 {
			// increment the count of the word by 1 if it already exists
			found := false
			for i, kv := range kvs {
				if kv.Key == word {
					curr, _ := strconv.Atoi(kvs[i].Value)
					kvs[i].Value = strconv.Itoa(curr + 1)
					found = true
					break
				}
			}
			// add the word to the slice if it doesn't exist
			if !found {
				kvs = append(kvs, mapreduce.KeyValue{Key: word, Value: "1"})
			}
		}
	}
	return kvs
}

func reduceFn(key string, values []string) string {
	count := 0
	for _, value := range values {
		curr, _ := strconv.Atoi(value)
		count += curr
	}
	return strconv.Itoa(count)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run word_count.go master sequential papers)
// 2) Master (e.g., go run word_count.go master localhost_7777 papers &)
// 3) Worker (e.g., go run word_count.go worker localhost_7777 localhost_7778 &) // change 7778 when running other workers
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcnt_seq", os.Args[3], 3, mapFn, reduceFn)
		} else {
			mr = mapreduce.Distributed("wcnt_dist", os.Args[3], 3, os.Args[2])
		}
		mr.Wait()
	} else if os.Args[1] == "worker" {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapFn, reduceFn, 100, true)
	} else {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	}
}
