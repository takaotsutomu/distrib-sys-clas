package mapreduce

import (
	"bufio"
	"encoding/json"
	"hash/fnv"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	file, err := os.Open(inFile)
	checkError(err)
	scanner := bufio.NewScanner(file)

	var encs []*json.Encoder
	for i := 0; i < nReduce; i++ {
		fname := reduceName(jobName, mapTaskNumber, i)
		file, err := os.Create(fname)
		checkError(err)
		encs = append(encs, json.NewEncoder(file))
		defer file.Close()
	}

	for scanner.Scan() {
		line := scanner.Text()
		kvList := mapF(inFile, line)
		for _, kv := range kvList {
			hashcode := ihash(kv.Key)
			err := encs[hashcode%uint32(nReduce)].Encode(&kv)
			checkError(err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
