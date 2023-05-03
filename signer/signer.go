package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const hashNumber = 6

func ExecutePipeline(jobs ...job) {
	in := make(chan interface{})
	var wg sync.WaitGroup
	for _, currentJob := range jobs {
		wg.Add(1)
		out := make(chan interface{})
		go pipelineWorker(currentJob, in, out, &wg)
		in = out
	}
	wg.Wait()
}

func pipelineWorker(job job, in, out chan interface{}, wg *sync.WaitGroup) {
	job(in, out)
	defer close(out)
	defer wg.Done()
}

func SingleHash(in, out chan interface{}) {
	mutex := sync.Mutex{}
	var wg sync.WaitGroup
	for i := range in {
		data := i.(int)
		wg.Add(1)
		go getSingleHash(data, out, &mutex, &wg)
	}
	wg.Wait()
}

func getSingleHash(intData int, out chan interface{}, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	data := strconv.Itoa(intData)
	dataMd5 := getMd5(data)
	mutex.Unlock()
	result := getCrc32(data, dataMd5)
	out <- result
	defer wg.Done()
}

func getMd5(data string) string {
	result := DataSignerMd5(data)
	return result
}

func getCrc32(data, dataMd5 string) string {
	result := [2]string{}
	source := [2]string{data, dataMd5}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go getCrc32Worker(source[i], &result[i], &wg)
	}
	wg.Wait()
	return result[0] + "~" + result[1]
}

func getCrc32Worker(data string, res *string, wg *sync.WaitGroup) {
	*res = DataSignerCrc32(data)
	defer wg.Done()
}

func MultiHash(in chan interface{}, out chan interface{}) {
	var wg sync.WaitGroup
	for i := range in {
		wg.Add(1)
		go getMultiHash(i, out, &wg)
	}
	wg.Wait()
}

func getMultiHash(data interface{}, out chan interface{}, wg *sync.WaitGroup) {
	result := make([]string, hashNumber)
	var wgMulti sync.WaitGroup
	for i := 0; i < hashNumber; i++ {
		wgMulti.Add(1)
		go getCrc32Worker(strconv.Itoa(i)+data.(string), &result[i], &wgMulti)
	}
	wgMulti.Wait()
	out <- strings.Join(result, "")
	defer wg.Done()
}

func CombineResults(in, out chan interface{}) {
	result := []string{}
	for i := range in {
		result = append(result, i.(string))
	}
	sort.Strings(result)
	out <- strings.Join(result, "_")
}
