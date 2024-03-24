package asg1

import (
	"math"
	"strings"
)

// Task 1
// This function should output 0^2 + 1^2 + 2^2 + ... + (|n|)^2
func getSumSquares(n int) int {
	// To Do
	sum := 0
	n = int(math.Abs(float64(n)))
	for i := 0; i <= n; i++ {
		sum += i * i
	}
	return sum
}

// Task 2
// This function extracts all words ending with endLetter from the string text
// Hints:
// - You may find the strings.Fields method useful.
// - Read about the difference between the types "rune" and "byte", and see how the function is tested.
func getWords(text string, endLetter rune) []string {
	// To Do
	words := strings.Fields(text)
	var result []string
	for _, word := range words {
		if strings.HasSuffix(word, string(endLetter)) {
			result = append(result, word)
		}
	}
	return result
}

// Task 3
type RegRecord struct {
	studentId  int
	courseName string
}

// This method receives a list of student registration records and should return
// a map that shows the number of students registered per course.
// Note that if a duplicate record appears in the input list, it should not be considered in the count.
func getCourseInfo(records []RegRecord) map[string]int {
	// To Do
	courseInfo := make(map[string]int)
	for _, record := range records {
		courseInfo[record.courseName]++
	}
	return courseInfo
}

// Task 4
// This method is required to count the occurrences of an input key in a list of integers.
// This should be done in parallel. Each invoked go routine should run the countWorker method on part of the list.
// The communication between the main thread and the workers should be done via channels.
// You can use any way to divide the input list across your workers.
// numThreads will not exceed the length of the array
func count(list []int, key int, numThreads int) int {
	// To Do
	inputChan := make(chan int)
	outChan := make(chan int)
	for i := 0; i < numThreads; i++ {
		go countWorker(key, inputChan, outChan)
	}
	for _, v := range list {
		inputChan <- v
	}
	close(inputChan)
	count := 0
	for i := 0; i < numThreads; i++ {
		count += <-outChan
	}
	close(outChan)
	return count
}

// This worker method receives inputs via inputChan, and outputs the number of occurrences to outChan
// Note: The worker does not have any information about the number of inputs it will process, i.e.,
// the method should keep working as long as inputChan is open
func countWorker(key int, inputChan chan int, outChan chan int) {
	// To Do
	count := 0
	for input := range inputChan {
		if input == key {
			count++
		}
	}
	outChan <- count
}
