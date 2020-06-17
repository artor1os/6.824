package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader int
	rid    int
	cid    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.cid = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.RID = ck.rid
	ck.rid++
	args.CID = ck.cid
	i := 0
	for {
		reply := QueryReply{}
		ok := ck.servers[ck.leader].Call("ShardMaster.Query", args, &reply)
		if ok && !reply.WrongLeader {
			return reply.Config
		}
		ck.leader++
		ck.leader %= len(ck.servers)
		i++
		if i%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.RID = ck.rid
	ck.rid++
	args.CID = ck.cid
	i := 0
	for {
		reply := JoinReply{}
		ok := ck.servers[ck.leader].Call("ShardMaster.Join", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.leader++
		ck.leader %= len(ck.servers)
		i++
		if i%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.RID = ck.rid
	ck.rid++
	args.CID = ck.cid
	i := 0
	for {
		reply := LeaveReply{}
		ok := ck.servers[ck.leader].Call("ShardMaster.Leave", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.leader++
		ck.leader %= len(ck.servers)
		i++
		if i%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.RID = ck.rid
	ck.rid++
	args.CID = ck.cid
	i := 0
	for {
		reply := MoveReply{}
		ok := ck.servers[ck.leader].Call("ShardMaster.Move", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.leader++
		ck.leader %= len(ck.servers)
		i++
		if i%len(ck.servers) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}
