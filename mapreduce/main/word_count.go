package main

import (
	"fmt"
	"lab1/mapreduce"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	for _, w := range words {
		kv := mapreduce.KeyValue{Key: w, Value: "1"}
		res = append(res, kv)
	}
	return
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	tc := 0
	for _, e := range values {
		count, err := strconv.Atoi(e)
		if err != nil {
			log.Fatal(err)
		}
		tc += count
	}
	return strconv.Itoa(tc)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
