package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"os"
	"io/ioutil"
	"sort"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for t, ok := CallGetTask(); ok; t, ok = CallGetTask() {
		log.Printf("worker: Get Task %v", t)
		switch t.Type {
		case Map:
			if err := doMap(t.Filename, t.NReduce, t.MapTaskNum, mapf); err == nil {
				CallComplete(t.ID)
			}
		case Reduce:
			if err := doReduce(t.NMap, t.ReduceTaskNum, reducef); err == nil {
				CallComplete(t.ID)
			}
		case Kill:
			return
		default:
			return
		}
	}

}

func doMap(filename string, nReduce int, mapTaskNum int, mapf func(string, string) []KeyValue) error {
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
	file.Close()
	kva := mapf(filename, string(content))

	kvaReduce := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		nr := ihash(kv.Key) % nReduce
		kvaReduce[nr] = append(kvaReduce[nr], kv)
	}

	for reduceTaskNum, kvr := range kvaReduce {
		intermediate := fmt.Sprintf("mr-%v-%v", mapTaskNum, reduceTaskNum)
		ofile, _ := ioutil.TempFile(os.TempDir(), "")
		enc := json.NewEncoder(ofile)
		for _, kv := range kvr {
			enc.Encode(&kv)
		}
		_ = os.Rename(ofile.Name(), intermediate)
	}
	return nil
}

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(nMap int, reduceTaskNum int, reducef func(string, []string) string) error {
	var intermediate []KeyValue
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, reduceTaskNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return err
		}
		dec := json.NewDecoder(file)
		
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	out := fmt.Sprintf("mr-out-%v", reduceTaskNum)
	ofile, _ := ioutil.TempFile(os.TempDir(), "")

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
	_ = os.Rename(ofile.Name(), out)
	return nil
}

// CallGetTask ...
func CallGetTask() (*Task, bool) {
	task := Task{}
	if ok := call("Master.GetTask", &struct{}{}, &task); ok {
		return &task, true
	}
	return nil, false
}

// CallComplete ...
func CallComplete(id int) {
	log.Printf("worker: task %v done, call complete\n", id)
	call("Master.Complete", &CompleteArgs{ID: id}, &struct{}{})
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
