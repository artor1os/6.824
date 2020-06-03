package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
}

const (
	GetOp = "Get"
	PutOp = "Put"
	AppendOp = "Append"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string

	indexCh map[int]chan struct{}
}

func (kv *KVServer) apply() {
	for am := range kv.applyCh {
		op := am.Command.(Op)
		DPrintf("apply: op: %v, index %v", op, am.CommandIndex)
		if ch, ok := kv.indexCh[am.CommandIndex]; ok {
			select {
			case <-ch:
			default:
			}
		} else {
			kv.indexCh[am.CommandIndex] = make(chan struct{}, 1)
		}
		kv.indexCh[am.CommandIndex] <- struct{}{}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{Type: GetOp, Key: args.Key})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// block on index
	if ch, ok := kv.indexCh[index]; ok {
		<- ch
	} else {
		kv.indexCh[index] = make(chan struct{})
	}
	<-kv.indexCh[index]

	var ok bool
	kv.mu.Lock()
	reply.Value, ok = kv.store[args.Key]
	kv.mu.Unlock()
	if !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{Type: args.Op, Key: args.Key, Value: args.Value})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// block on index
	if ch, ok := kv.indexCh[index]; ok {
		<- ch
	} else {
		kv.indexCh[index] = make(chan struct{})
	}
	<-kv.indexCh[index]

	kv.mu.Lock()
	switch args.Op {
	case PutOp:
		kv.store[args.Key] = args.Value
	case AppendOp:
		kv.store[args.Key] = kv.store[args.Key] + args.Value
	}
	kv.mu.Unlock()
	
	reply.Err = OK
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = make(map[string]string)
	kv.indexCh = make(map[int]chan struct{})
	// You may need initialization code here.
	go kv.apply()

	return kv
}
