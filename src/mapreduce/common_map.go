package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"math"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	infile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	// Create a (hash)-map to hold indices in keyvals that share a common filename
	// keyTofile[filename] = [slice of indices in that should be in filename]
	var keyTofile = make(map[string][]int)

	// Load the given infile, convert to string
	contents, _ := ioutil.ReadFile(infile)
	str := string(contents)

	// Call the given map function on the contents of the file to generate a list of keys and values
	keyvals := mapF(infile, str)

	// For each index i and KeyValue kv in keyvals
	for i, kv := range keyvals {
		// Use the key to calculate the filename it should go in
		r := int(math.Mod(float64(ihash(kv.Key)), float64(nReduce)))
		newFile := reduceName(jobName, mapTask, r)

		// Add the index of that kv to the map, with map key newfile
		keyTofile[newFile] = append(keyTofile[newFile], i)
	}
	// For file name fileName and slice indexSlice in keyTofile
	for fileName, indexSlice := range keyTofile {
		// Open a file with that name, creating it if it doesn't exist
		file, _ := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0660)
		enc := json.NewEncoder(file)

		for _, keyIndex := range indexSlice {
			_ = enc.Encode(&keyvals[keyIndex])
		}
		file.Close()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
