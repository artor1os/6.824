package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
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
	KeyNotExist bool
	RID int
	CID int64

	WrongLeader bool
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

	indexCh map[int]chan *Op
	lastCommited map[int64]int
}

func (kv *KVServer) snapshot(index int) {
	DPrintf("make snapshot")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(kv.store)
	_ = e.Encode(kv.lastCommited)

	data := w.Bytes()
	go kv.rf.DiscardOldLog(index, data)
}

func (kv *KVServer) recover(snapshot []byte) {
	DPrintf("recover from snapshot, size %v", len(snapshot))
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var store map[string]string
	var lastCommited map[int64]int

	if d.Decode(&store) != nil || d.Decode(&lastCommited) != nil {
		DPrintf("failed to read snapshot")
	} else {
		kv.store = store
		kv.lastCommited = lastCommited
	}
}

func (kv *KVServer) apply() {
	for am := range kv.applyCh {
		kv.mu.Lock()
		if !am.CommandValid { // snapshot
			kv.recover(am.Snapshot)
		} else {
			op := am.Command.(Op)
			var ch chan *Op
			var ok bool
			if ch, ok = kv.indexCh[am.CommandIndex]; ok {
				select {
				case <-ch:
				default:
				}
			} else {
				ch = make(chan *Op, 1)
				kv.indexCh[am.CommandIndex] = ch
			}
			kv.applyOp(&op)
			if kv.maxraftstate > -1 && am.RaftStateSize >= kv.maxraftstate {
				kv.snapshot(am.CommandIndex)
			}
			ch <- &op
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) isDup(op *Op) bool {
	lastCommited, ok := kv.lastCommited[op.CID]
	if !ok {
		return false
	}
	return op.RID <= lastCommited
}

func (kv *KVServer) applyOp(op *Op) {
	switch op.Type {
	case PutOp:
		if !kv.isDup(op) {
			kv.store[op.Key] = op.Value
		}
	case AppendOp:
		if !kv.isDup(op) {
			kv.store[op.Key] += op.Value
		}
	case GetOp:
		v, ok := kv.store[op.Key]
		if !ok {
			op.KeyNotExist = true
		} else {
			op.KeyNotExist = false
			op.Value = v
		}
	}
	if !kv.isDup(op) {
		kv.lastCommited[op.CID] = op.RID
	}
}

func (kv *KVServer) waitIndexCommit(index int, cid int64, rid int) *Op {
	kv.mu.Lock()
	var ch chan *Op
	var ok bool
	if ch, ok = kv.indexCh[index]; !ok {
		ch = make(chan *Op, 1)
		kv.indexCh[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		if op.CID != cid || op.RID != rid {
			return &Op{WrongLeader: true}
		}
		return op
	case <-time.After(time.Millisecond * 300):
		DPrintf("timeout")
		return &Op{WrongLeader: true}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	newOp := Op{Type: GetOp, RID: args.RID, CID: args.CID, Key: args.Key}
	index, _, isLeader := kv.rf.Start(newOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// block on index
	op := kv.waitIndexCommit(index, args.CID, args.RID)

	if op.WrongLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if op.KeyNotExist {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = op.Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	newOp := Op{Type: args.Op, RID: args.RID, CID: args.CID, Key: args.Key, Value: args.Value}
	index, _, isLeader := kv.rf.Start(newOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// block on index
	op := kv.waitIndexCommit(index, args.CID, args.RID)

	if op.WrongLeader {
		reply.Err = ErrWrongLeader
		return
	}

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
	kv.indexCh = make(map[int]chan *Op)
	kv.lastCommited = make(map[int64]int)
	// You may need initialization code here.
	go kv.apply()

	return kv
}
