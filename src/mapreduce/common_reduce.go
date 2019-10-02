package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	//fmt.Println("reduce Job name => ", jobName)
	//fmt.Println("reduce output file => ", outFile)
	//fmt.Println("nMap => ", nMap)

	// To store the keys in slice in sorted order
	var keys []string

	kvpairs := make(map[string][]string)
	// create reduceName for each Map task (I think using for loop to iterate?)
	for i:=0; i<nMap; i++ {
		// filename =>  mrtmp.test-0-0
		filename := reduceName(jobName, i, reduceTask)
		// decoded the intermediate file from doMap()
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := kvpairs[kv.Key]
			if !ok {
				keys = append(keys, kv.Key)
			}
			//fmt.Println("key => ", kv.Key, " value => ", kv.Value)
			/*f := func(c rune) bool {
				return !unicode.IsLetter(c)
			}*/
			kvpairs[kv.Key] = append(kvpairs[kv.Key], kv.Value)
			//fmt.Println("key => ", kv.Key)
		}
		file.Close()
	}

	sort.Strings(keys)
 	//fmt.Println("key length => ", len(keys))

	//for _, k := range keys {
	//	reduceVal := reduceF(k, kvpairs[k])
	//	fmt.Println("Key:", k, "Value:", kvpairs[k], " reduceResult => ", reduceVal)
	//}

	file, _ := os.Create(outFile)
	enc := json.NewEncoder(file)

	for _, k := range keys {
		reduceVal := reduceF(k, kvpairs[k])
		//fmt.Println("Key:", k, "Value:", kvpairs[k], " reduceResult => ", reduceVal)
		err := enc.Encode(KeyValue{k, reduceVal})
		if err != nil {
			break
		}
	}

	file.Close()
}
