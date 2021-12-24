package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapFile(filename string, mapf func(string, string) []KeyValue, nReduce int, mapId int) error {
	//
	// read the input file, pass it to Map, and output to intermediate file
	//
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}

	if file.Close() != nil {
		log.Fatal(err)
		return err
	}
	intermediate := mapf(filename, string(content))

	sort.Sort(ByKey(intermediate))

	// Open nReduce number of files to write intermediate Map results.
	var fileHandlers []*os.File
	for i := 0; i < nReduce; i++ {
		err = func() error {
			tempf, err := os.Create(fmt.Sprintf("mr-%d-%d", mapId, i))
			if err != nil {
				return err
			}
			fileHandlers = append(fileHandlers, tempf)
			return nil
		}()
		if err != nil {
			return err
		}
	}

	for i := 0; i < len(intermediate); i++ {
		reduceId := ihash(intermediate[i].Key) % nReduce
		_, err := fmt.Fprintf(fileHandlers[reduceId], "%v %v\n", intermediate[i].Key, intermediate[i].Value)
		if err != nil {
			return err
		}
	}

	for _, f := range fileHandlers {
		f.Close()
	}
	return nil
}

func reduceFile(reducef func(string, []string) string, reduceId int) error {
	filenameRegex, _ := regexp.Compile(fmt.Sprintf("^mr-[0-9]*-%d$", reduceId))

	var filenames []string
	e := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err == nil && filenameRegex.MatchString(info.Name()) {
			filenames = append(filenames, path)
		}
		return nil
	})
	if e != nil {
		log.Fatal(e)
		return e
	}

	var intermediate []KeyValue
	for _, filename := range filenames {
		err := func() error {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				return err
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				kvp := strings.Split(scanner.Text(), " ")
				intermediate = append(intermediate, KeyValue{Key: kvp[0], Value: kvp[1]})
			}
			return nil
		}()

		if err != nil {
			log.Fatal(e)
			return err
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceId)
	ofile, err := os.Create(oname)
	defer ofile.Close()

	if err != nil {
		log.Fatal(e)
		return err
	}
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// fmt.Println("Getting Job from coordinator...")
		status, reply := GetJob()
		if status && reply.Complete {
			return
		} else if !status || reply.JobType == 0 {
			// fmt.Println("No jobs available, sleeping")
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.JobType == 1 {
			// fmt.Printf("Working on Map Job %d: %s\n", reply.MapId, reply.Filename)
			// Map task
			err := mapFile(reply.Filename, mapf, reply.NReduce, reply.MapId)
			done := true
			if err != nil {
				// fmt.Printf("Map Job %d: %s failed: %s\n", reply.MapId, reply.Filename, err)
				done = false
			}
			// fmt.Printf("Map Job %d: %s run successfully\n", reply.MapId, reply.Filename)
			if UpdateStatus(1, done, reply.MapId, -1) {
				return
			}
		} else {
			// Reduce task
			// fmt.Printf("Working on Reduce Job %d\n", reply.ReduceId)
			err := reduceFile(reducef, reply.ReduceId)
			done := true
			if err != nil {
				// fmt.Printf("Reduce Job %d failed\n", reply.ReduceId)
				done = false
			}
			// fmt.Printf("Reduce Job %d run successfully\n", reply.ReduceId)
			if UpdateStatus(2, done, -1, reply.ReduceId) {
				return
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func UpdateStatus(jobType int, done bool, mapId int, reduceId int) bool {
	args := StatusArgs{JobType: jobType, Done: done, MapId: mapId, ReduceId: reduceId}
	reply := StatusReply{Complete: false}
	call("Coordinator.UpdateStatus", &args, &reply)
	return reply.Complete
}

func GetJob() (bool, *JobReply) {
	args := JobArgs{}
	reply := JobReply{}
	status := call("Coordinator.AssignJob", &args, &reply)
	return status, &reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
