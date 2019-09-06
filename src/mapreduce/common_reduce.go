package mapreduce

import (
	"encoding/json"
	"os"
	//"sort"
	"strconv"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	var kv KeyValue
	var keyValues []KeyValue
	dict := make(map[string]int)

	// Create/open a file based on the given name and make a JSON encoder
	output, _ := os.OpenFile(outFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0660)
	enc := json.NewEncoder(output)

	// For each map task
	for m := 0; m < nMap; m++ {
		// Calculate the filename that was used and open it
		filename := reduceName(jobName, m, reduceTask)
		file, _ := os.OpenFile(filename, os.O_RDONLY, 0660)

		// Decode one KeyValue at a time until there's nothing left
		dec := json.NewDecoder(file)
		for err := dec.Decode(&kv); err == nil; {
			// Accumulate duplicate keys and values in map
			val, _ := strconv.Atoi(kv.Value)
			dict[kv.Key] += val
			err = dec.Decode(&kv)
		}
		file.Close()
	}

	// Convert from map to KeyValue slice
	for key, val := range dict {
		keyValues = append(keyValues, KeyValue{key, strconv.Itoa(val)})
	}

	//Sort by alphabetical order
	//sort.Slice(keyValues, func(i, j int) bool {
	//	return keyValues[i].Key < keyValues[j].Key
	//})

	// Encode as JSON into the provided outFile
	for _, kvpair := range keyValues {
		_ = enc.Encode(KeyValue{kvpair.Key, reduceF(kvpair.Key, []string{kvpair.Value})})
	}
	output.Close()
}
