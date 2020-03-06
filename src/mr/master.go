package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"

type Master struct {
	// Your definitions here.
	mu sync.RWMutex
	done map[int]chan struct{}

	mapChan chan *Task
	reduceChan chan *Task

	nDone int
	nMap int
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) popTask() *Task {
	t := <-m.mapChan
	if t != nil {
		return t
	}
	t = <-m.reduceChan
	if t != nil {
		return t
	}
	return &Task{Type:Kill}	
}

// GetTask ...
func (m *Master) GetTask(args *struct{}, reply *Task) error {
	t := m.popTask()
	log.Printf("master: Get Task: %v\n", t)
	*reply = *t
	if t.Type != Kill {
		go func() {
			select {
			case <-time.After(10 * time.Second):
				log.Printf("master: time out, resend %v\n", t)
				if t.Type == Map {
					m.mapChan <- t
				} else {
					m.reduceChan <- t
				}
			case <-m.done[t.ID]:
				return
			}
		}()
	}
	return nil
}

// Complete ...
func (m *Master) Complete(args *CompleteArgs, reply *struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nDone++
	l := fmt.Sprintf("master: id %v done, ", args.ID)
	if m.nDone == m.nMap {
		l += "close map, "
		close(m.mapChan)
	}
	if m.nDone == m.nMap + m.nReduce {
		l += "close reduce, "
		close(m.reduceChan)
	}
	l += fmt.Sprintf("close done %v\n", args.ID)
	log.Print(l)
	close(m.done[args.ID])
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.nDone == m.nMap + m.nReduce {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)
	m := Master{
		done: make(map[int]chan struct{}),
		mapChan: make(chan *Task, nMap),
		reduceChan: make(chan *Task, nReduce),
		nMap: nMap,
		nReduce: nReduce,
	}

	i := 0
	// Your code here.
	for ; i < nMap; i++ {
		t := &Task{
			ID: i,
			Type: Map,
			Filename: files[i],
			MapTaskNum: i,
			NReduce: nReduce,
		}
		m.mapChan <- t
		m.done[i] = make(chan struct{})
	}

	for ; i < nMap + nReduce; i++ {
		t := &Task{
			ID: i,
			Type: Reduce,
			NMap: nMap,
			ReduceTaskNum: i - nMap,
		}
		m.reduceChan <- t
		m.done[i] = make(chan struct{})
	}

	m.server()
	return &m
}
