package shardkv

// import "../shardmaster"
import (
	"bytes"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string

	RID int
	CID int64

	Err     Err
	Migrate *MigrateInfo
	Config  *ConfigInfo
}

const (
	GetOp     = "Get"
	PutOp     = "Put"
	AppendOp  = "Append"
	MigrateOp = "Migrate"
	ConfigOp  = "Config"
)

type MigrateInfo struct {
	Data         map[string]string
	Shard        int
	Num          int
	LastCommited map[int64]int
}

type ConfigInfo struct {
	Config *shardmaster.Config
}

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

	indexCh      map[int]chan *Op
	lastCommited map[int64]int

	mck *shardmaster.Clerk

	sm *shardManager
}

type shardManager struct {
	GID           int
	ConfigNum     int
	ServedShards  set
	WaitingShards set
	// configNum -> shard -> servers
	MigratingShards map[int]map[int][]string
}

func newShardManager(gid int) *shardManager {
	sm := &shardManager{GID: gid, ServedShards: newSet(), WaitingShards: newSet(), MigratingShards: make(map[int]map[int][]string)}
	return sm
}

func (sm *shardManager) Update(config *shardmaster.Config) {
	sm.MigratingShards[config.Num] = make(map[int][]string)
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if sm.ConfigNum > 0 && !sm.ServedShards.Contain(shard) && config.Shards[shard] == sm.GID {
			sm.WaitingShards.Add(shard)
		}

		if sm.ConfigNum > 0 && sm.ServedShards.Contain(shard) && config.Shards[shard] != sm.GID {
			sm.MigratingShards[config.Num][shard] = config.Groups[config.Shards[shard]]
		}

		if config.Shards[shard] != sm.GID {
			sm.ServedShards.Delete(shard)
		} else {
			sm.ServedShards.Add(shard)
		}
	}

	sm.ConfigNum = config.Num
}

func (sm *shardManager) Waiting() bool {
	return !sm.WaitingShards.Empty()
}

func (sm *shardManager) ShouldServe(shard int) bool {
	return sm.ServedShards.Contain(shard) && !sm.WaitingShards.Contain(shard)
}

func (sm *shardManager) Serve(shard int) {
	sm.ServedShards.Add(shard)
	sm.WaitingShards.Delete(shard)
}

func (sm *shardManager) Migrate(configNum int, shard int) {
	delete(sm.MigratingShards[configNum], shard)
}

type set map[int]bool

func (s set) Contain(i int) bool {
	_, ok := s[i]
	return ok
}

func (s set) Add(i int) bool {
	if s.Contain(i) {
		return false
	}
	s[i] = true
	return true
}

func (s set) Delete(i int) {
	delete(s, i)
}

func (s set) Empty() bool {
	r := false
	for _, b := range s {
		r = r || b
	}
	return !r
}

func newSet() set {
	return make(map[int]bool)
}

func (kv *ShardKV) shouldServe(key string) bool {
	shard := key2shard(key)
	return kv.sm.ShouldServe(shard)
}

const pollInterval = time.Millisecond * 100

func (kv *ShardKV) pollConfig() {
	ticker := time.NewTicker(pollInterval)
	for range ticker.C {
		// No lock is needed here, data race is acceptable
		config := kv.mck.Query(kv.sm.ConfigNum + 1)
		if !kv.sm.Waiting() && config.Num > kv.sm.ConfigNum {
			kv.rf.Start(Op{Type: ConfigOp, Config: &ConfigInfo{Config: &config}})
		}
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

func (kv *ShardKV) copyLastCommited() map[int64]int {
	r := make(map[int64]int)
	for k, v := range kv.lastCommited {
		r[k] = v
	}
	return r
}

func (kv *ShardKV) deleteShard(shard int) {
	for k := range kv.store {
		if key2shard(k) == shard {
			delete(kv.store, k)
		}
	}
}

func (kv *ShardKV) migrate(configNum int) {
	for shard, servers := range kv.sm.MigratingShards[configNum] {
		args := MigrateArgs{Shard: shard, Data: kv.dataOfShard(shard), Num: configNum, LastCommited: kv.copyLastCommited()}
		go func(shard int, servers []string) {
			si := 0
			for {
				reply := MigrateReply{}
				server := kv.make_end(servers[si])
				ok := server.Call("ShardKV.Migrate", &args, &reply)
				if ok && reply.Err == OK {
					break
				}
				si++
				si %= len(servers)
			}
			kv.mu.Lock()
			kv.sm.Migrate(configNum, shard)
			if !kv.sm.ServedShards.Contain(shard) {
				kv.deleteShard(shard)
			}
			kv.mu.Unlock()
		}(shard, servers)
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	if args.Num > kv.sm.ConfigNum+1 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	data := make(map[string]string)
	for k, v := range args.Data {
		data[k] = v
	}
	lastCommited := make(map[int64]int)
	for k, v := range args.LastCommited {
		lastCommited[k] = v
	}
	index, _, isLeader := kv.rf.Start(Op{Type: MigrateOp, Migrate: &MigrateInfo{Data: data, Shard: args.Shard, Num: args.Num, LastCommited: lastCommited}})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// No need to check whether applied command is this command
	op := kv.waitIndexCommit(index, 0, 0)

	reply.Err = op.Err
}

func (kv *ShardKV) snapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(kv.store)
	_ = e.Encode(kv.lastCommited)
	_ = e.Encode(kv.sm)

	data := w.Bytes()
	go kv.rf.DiscardOldLog(index, data)
}

func (kv *ShardKV) recover(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var store map[string]string
	var lastCommited map[int64]int
	var sm *shardManager

	if d.Decode(&store) != nil || d.Decode(&lastCommited) != nil || d.Decode(&sm) != nil {
		panic("failed to recover")
	} else {
		kv.store = store
		kv.lastCommited = lastCommited
		kv.sm = sm
		for configNum := range kv.sm.MigratingShards {
			kv.migrate(configNum)
		}
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

	if op.Type == ConfigOp {
		// Drop config if waiting process of last re-configuration does not end
		if kv.sm.Waiting() {
			return
		}
		// Detect stale or duplicate config
		if op.Config.Config.Num <= kv.sm.ConfigNum {
			return
		}
		kv.sm.Update(op.Config.Config)
		kv.migrate(op.Config.Config.Num)
		return
	}

	if op.Type == MigrateOp {
		// Duplicate migrate
		if op.Migrate.Num < kv.sm.ConfigNum || kv.sm.ShouldServe(op.Migrate.Shard) {
			return
		}
		for k, v := range op.Migrate.Data {
			kv.store[k] = v
		}
		kv.sm.Serve(op.Migrate.Shard)

		for cid, rid := range op.Migrate.LastCommited {
			if localRID, ok := kv.lastCommited[cid]; !ok || rid > localRID {
				kv.lastCommited[cid] = rid
			}
		}

		return
	}

	if !kv.shouldServe(op.Key) {
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
	if !kv.shouldServe(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		// Optimization, maybe useless?
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
		}
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
	kv.mu.Lock()
	if !kv.shouldServe(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.Err = ErrWrongLeader
		}
		return
	}
	kv.mu.Unlock()
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
	kv.sm = newShardManager(kv.gid)

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = make(map[string]string)
	kv.indexCh = make(map[int]chan *Op)
	kv.lastCommited = make(map[int64]int)

	go kv.apply()
	go kv.pollConfig()

	return kv
}
