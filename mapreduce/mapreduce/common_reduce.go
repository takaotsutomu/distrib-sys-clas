package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	var decs []*json.Decoder
	for i := 0; i < nMap; i++ {
		fname := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(fname)
		checkError(err)
		decs = append(decs, json.NewDecoder(file))
	}

	var kvList []KeyValue
	for i := 0; i < nMap; i++ {
		for decs[i].More() {
			var kv KeyValue
			err := decs[i].Decode(&kv)
			checkError(err)
			kvList = append(kvList, kv)
		}
	}
	sort.Slice(kvList, func(i, j int) bool {
		kv1 := kvList[i]
		kv2 := kvList[j]
		if kv1.Key == kv2.Key {
			return true
		}
		return kv1.Key < kv2.Key
	})

	fname := mergeName(jobName, reduceTaskNumber)
	file, err := os.Create(fname)
	checkError(err)
	enc := json.NewEncoder(file)

	key := kvList[0].Key
	values := []string{kvList[0].Value}
	for i := 1; i < len(kvList); i++ {
		if i == len(kvList)-1 {
			if kvList[i].Key != key {
				enc.Encode(KeyValue{key, reduceF(key, values)})
				key = kvList[i].Key
				values = []string{kvList[i].Value}
			}
			enc.Encode(KeyValue{key, reduceF(key, values)})
		} else {
			if kvList[i].Key != key {
				enc.Encode(KeyValue{key, reduceF(key, values)})
				key = kvList[i].Key
				values = []string{kvList[i].Value}
			} else {
				values = append(values, kvList[i].Value)
			}
		}
	}
}
