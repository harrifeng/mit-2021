package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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

	for {
		hargs := HandlerArgs{}
		hreply := HandlerReply{}

		call("Coordinator.Handler", &hargs, &hreply)
		//log.Println("hreply", hreply)
		if hreply.JobType == "map" {

			file, err := os.Open(hreply.MapFile)
			if err != nil {
				log.Fatalf("cannot open %v", hreply.MapFile)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", hreply.MapFile)
			}
			file.Close()
			kva := mapf(hreply.MapFile, string(content))

			total := []*json.Encoder{}

			for i := 0; i < hreply.ReduceNum; i++ {
				tmp, err := os.Create(fmt.Sprintf("mr-%v-%v.json", hreply.MapIndex, i))
				defer tmp.Close()
				if err != nil {
					panic(err)
				}
				enc := json.NewEncoder(tmp)
				total = append(total, enc)
			}

			for _, onekva := range kva {
				curr := total[ihash(onekva.Key)%10]
				curr.Encode(&onekva)
			}
			log.Printf("map job mr-%v finished", hreply.MapIndex)

			nargs := NotifyArgs{}
			nreply := NotifyReply{}
			nargs.NotifyType = "map"
			nargs.NotifyIndex = hreply.MapIndex

			call("Coordinator.Notify", &nargs, &nreply)

		} else if hreply.JobType == "reduce" {

			kva := []KeyValue{}
			for i := 0; i < hreply.MapNum; i++ {
				tmp, err := os.Open(fmt.Sprintf("mr-%v-%v.json", i, hreply.ReduceIndex))
				defer tmp.Close()
				if err != nil {
					panic(err)
				}

				dec := json.NewDecoder(tmp)

				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			oname := fmt.Sprintf("mr-out-%v", hreply.ReduceIndex)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			log.Printf("reduce job mr-%v finished", hreply.ReduceIndex)

			nargs := NotifyArgs{}
			nreply := NotifyReply{}
			nargs.NotifyType = "reduce"
			nargs.NotifyIndex = hreply.ReduceIndex

			call("Coordinator.Notify", &nargs, &nreply)

		} else if hreply.JobType == " retry" {
			//log.Println("retry--------------")
		} else if hreply.JobType == "alldone" {
			os.Exit(0)
		} else {
			//log.Println("sleeping 1 second")
			time.Sleep(100 * time.Microsecond)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	fmt.Println(err)
	return false
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
