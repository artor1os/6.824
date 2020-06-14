package shardkv


// import "../shardmaster"
import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)


const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) say(format string, a ...interface{}) {
	DPrintf(fmt.Sprintf("gid %v, me %v:\t", kv.gid, kv.me)+format, a...)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	Data map[string]string
	Shard int

	RID int
	CID int64

	Err Err
}

const (
	GetOp = "Get"
	PutOp = "Put"
	AppendOp = "Append"
	MigrateOp = "Migrate"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string

	indexCh map[int]chan *Op
	lastCommited map[int64]int

	mck *shardmaster.Clerk
	config shardmaster.Config
	configChanging bool
	shardMigrationCompleted map[int]bool
}

func (kv *ShardKV) shouldServe(key string) bool {
	shard := key2shard(key)
	return kv.config.Shards[shard] == kv.gid
}

const pollInterval = time.Millisecond * 100

func (kv *ShardKV) pollConfig() {
	ticker := time.NewTicker(pollInterval)
	for range ticker.C {
		config := kv.mck.Query(-1)
		kv.mu.Lock()
		var wg sync.WaitGroup
		if !kv.configChanging && config.Num > kv.config.Num {
			kv.say("pollConfig: new config found, config %v", config)
			kv.configChanging = true
			kv.shardMigrationCompleted = make(map[int]bool)
			_, isLeader := kv.rf.GetState()
			kv.migrate(config, isLeader, &wg)
			kv.config = config
			if kv.isMigrationCompleted() {
				kv.say("pollConfig: no new shard to wait")
				kv.configChanging = false
			}
		}
		kv.mu.Unlock()
		wg.Wait()
	}
}

func (kv *ShardKV) dataOfShard(shard int) map[string]string {
	data := make(map[string]string)
	for k, v := range kv.store {
		if key2shard(k) == shard {
			data[k] = v
		}
	}
	return data
}

func (kv *ShardKV) isMigrationCompleted() bool {
	allCompleted := true
	for _, c := range kv.shardMigrationCompleted {
		allCompleted = allCompleted && c
	}
	return allCompleted
}

func (kv *ShardKV) migrate(newConf shardmaster.Config, isLeader bool, wg *sync.WaitGroup) {
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if newGroup := newConf.Shards[shard]; kv.config.Shards[shard] == kv.gid && newGroup != kv.gid && isLeader {
			servers := newConf.Groups[newGroup]
			args := MigrateArgs{Shard: shard, Data: kv.dataOfShard(shard)}
			kv.say("migrate: shard %v, from %v, to %v", shard, kv.gid, newGroup)
			wg.Add(1)
			go func() {
				si := 0
				for {
					reply := MigrateReply{}
					server := kv.make_end(servers[si])
					server.Call("ShardKV.Migrate", &args, &reply)
					if reply.Err == OK {
						break
					}
					si++
					si %= len(servers)
				}
				wg.Done()
			}()
		} else if kv.config.Shards[shard] != kv.gid && newGroup == kv.gid && kv.config.Shards[shard] != 0 {
			kv.say("migrate: group %v wait for new shard %v", kv.gid, shard)
			kv.shardMigrationCompleted[shard] = false
		}
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	if !kv.configChanging {
		kv.say("Migrate: configChanging not started yet")
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	kv.say("Migrate: start migration shard %v, group %v", args.Shard, kv.gid)
	data := make(map[string]string)
	for k, v := range args.Data {
		data[k] = v
	}
	index, _, isLeader := kv.rf.Start(Op{Type: MigrateOp, Data: data, Shard: args.Shard})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := kv.waitIndexCommit(index, 0, 0)
	
	reply.Err = op.Err
	kv.say("Migrate: migration Err %v", reply.Err)
}

func (kv *ShardKV) snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(kv.store)
	_ = e.Encode(kv.lastCommited)

	data := w.Bytes()
	go kv.rf.DiscardOldLog(index, data)
}

func (kv *ShardKV) recover(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var store map[string]string
	var lastCommited map[int64]int

	if d.Decode(&store) != nil || d.Decode(&lastCommited) != nil {
	} else {
		kv.store = store
		kv.lastCommited = lastCommited
	}
}

func (kv *ShardKV) apply() {
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

func (kv *ShardKV) isDup(op *Op) bool {
	lastCommited, ok := kv.lastCommited[op.CID]
	if !ok {
		return false
	}
	return op.RID <= lastCommited
}

func (kv *ShardKV) commit(op *Op) {
	kv.lastCommited[op.CID] = op.RID
}

func (kv *ShardKV) applyOp(op *Op) {
	op.Err = OK

	if op.Type == MigrateOp {
		kv.say("applyOp: MigrateOp, shard %v", op.Shard)
		for k, v := range op.Data {
			kv.store[k] = v
		}
		kv.shardMigrationCompleted[op.Shard] = true
		if kv.isMigrationCompleted() {
			kv.say("applyOp: MigrateOp shard %v, migration completed", op.Shard)
			kv.configChanging = false
		}
		return
	}

	if !kv.shouldServe(op.Key) {
		kv.say("applyOp: key is migrating, not apply this op %#v", op)
		op.Err = ErrWrongGroup
		return
	}

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
			op.Err = ErrNoKey
		} else {
			op.Value = v
		}
	}
	if !kv.isDup(op) {
		kv.commit(op)
	}
}

func (kv *ShardKV) waitIndexCommit(index int, cid int64, rid int) *Op {
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
			return &Op{Err: ErrWrongLeader}
		}
		return op
	case <-time.After(time.Millisecond * 300):
		return &Op{Err: ErrWrongLeader}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.configChanging || !kv.shouldServe(args.Key) {
		kv.say("Get: reject key %v, config %v", args.Key, kv.config)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	newOp := Op{Type: GetOp, RID: args.RID, CID: args.CID, Key: args.Key}
	index, _, isLeader := kv.rf.Start(newOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// block on index
	op := kv.waitIndexCommit(index, args.CID, args.RID)

	reply.Err = op.Err
	reply.Value = op.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.configChanging || !kv.shouldServe(args.Key) {
		kv.say("Get: reject key %v, config %v", args.Key, kv.config)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	newOp := Op{Type: args.Op, RID: args.RID, CID: args.CID, Key: args.Key, Value: args.Value}
	index, _, isLeader := kv.rf.Start(newOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// block on index
	op := kv.waitIndexCommit(index, args.CID, args.RID)

	reply.Err = op.Err
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = kv.mck.Query(-1)

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = make(map[string]string)
	kv.indexCh = make(map[int]chan *Op)
	kv.lastCommited = make(map[int64]int)

	kv.shardMigrationCompleted = make(map[int]bool)

	go kv.apply()
	go kv.pollConfig()

	return kv
}
