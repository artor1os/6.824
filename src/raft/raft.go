package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "time"
import "fmt"
import "sort"
import "strconv"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Logs []*LogEntry

func (log Logs) String() string {
	ret := "["
	for _, l := range log {
		ret += strconv.Itoa(l.Term) + ", "
	}
	ret += "]"
	return ret
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Lab2A: leader election and heartbeats
	currentTerm int
	votedFor    int

	role role

	resetElectionTimeout chan struct{}
	stopHeartbeat        chan struct{}
	stopWaitForElect     chan struct{}

	// Lab2B: log replication
	log Logs

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

type role string

const (
	follower  role = "fol"
	candidate role = "can"
	leader    role = "lea"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) say(something ...interface{}) {
	l := fmt.Sprint("peer ", rf.me, " term ", rf.currentTerm, " ", rf.role, "\t: ")
	l += fmt.Sprint(something...)
	DPrintln(l)
}

func (rf *Raft) majority() int {
	return (len(rf.peers) + 1) / 2
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = leader
	rf.say("become leader")

	rf.stopWaitForElect <- struct{}{}
	go rf.heartbeat()

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
}

// apply goroutine
func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	rf.say("apply: lastApplied ", rf.lastApplied, ", commitIndex ", rf.commitIndex)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandValid = true
		msg.CommandIndex = i
		msg.Command = rf.log[i].Command
		rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
}

// lock should be held by caller
func (rf *Raft) updateCommitIndex(index int) {
	rf.say("commitIndex from ", rf.commitIndex, " to ", index)
	rf.commitIndex = index
	go rf.apply()
}

const heartbeatInterval = 200 * time.Millisecond
const eTimeoutLeft = 400
const eTimeoutRight = 600

// heartbeat goroutine
func (rf *Raft) heartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	for {
		select {
		case <-ticker.C:
			if rf.killed() {
				ticker.Stop()
				return
			}
			rf.mu.Lock()
			if rf.role == leader { // NOTE: error-prone
				rf.say("send heartbeat")
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.appendEntries(i, rf.nextIndex[i], true)
				}
			}
			rf.mu.Unlock()
		case <-rf.stopHeartbeat:
			ticker.Stop()
			rf.say("stop heartbeat")
			return
		}
	}
}

// elcet goroutine
func (rf *Raft) elect(timeout time.Duration) {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me

	voteCh := make(chan struct{})
	elected := make(chan struct{})
	go func() {
		votes := 1
		for range voteCh { // NOTE: will be infinite loop?
			votes++
			if votes == rf.majority() {
				close(elected)
			}
		}
	}()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[args.LastLogIndex].Term
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok && reply.VoteGranted {
				voteCh <- struct{}{}
			}
		}(i)
	}
	rf.mu.Unlock()
	timer := time.NewTimer(timeout)
	select {
	case <-elected:
		timer.Stop()
		rf.becomeLeader()
	case <-timer.C:

	}
}

// waitForElect goroutine
func (rf *Raft) waitForElect() {
	for {
		eTimeout := time.Duration(RandIntRange(eTimeoutLeft, eTimeoutRight)) * time.Millisecond
		timer := time.NewTimer(eTimeout)
		select {
		case <-timer.C:
			if rf.killed() {
				timer.Stop()
				return
			}
			rf.mu.Lock()
			if rf.role == follower {
				rf.role = candidate
			}
			if rf.role == candidate {
				rf.say("election timeout, start a new election")
				go rf.elect(eTimeout)
			}
			rf.mu.Unlock()
		case <-rf.resetElectionTimeout:
			// rf.say("reset election timeout")
			timer.Stop()
			continue
		case <-rf.stopWaitForElect:
			// rf.say("stop wait for elect")
			timer.Stop()
			return
		}
	}
}

// lock should already held by caller
func (rf *Raft) rpcTermCheck(inTerm int, vote int) {
	if inTerm > rf.currentTerm {
		rf.say("currentTerm ", rf.currentTerm, " less than incoming term ", inTerm, " become follower")
		if rf.role == leader { // NOTE: error-prone
			rf.stopHeartbeat <- struct{}{}
			go rf.waitForElect()
		}
		rf.currentTerm = inTerm
		rf.role = follower
		rf.votedFor = vote // NOTE: very important
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term        int
	CandidateId int

	// 2B
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) isUptodate(lastLogIndex int, lastLogTerm int) bool {
	if len(rf.log) == 0 {
		return true
	}
	myLastLog := rf.log[len(rf.log)-1]
	if myLastLog.Term < lastLogTerm {
		return true
	}
	if myLastLog.Term == lastLogTerm {
		return len(rf.log)-1 <= lastLogIndex
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.rpcTermCheck(args.Term, args.CandidateId)
	reply.Term = rf.currentTerm // NOTE: error-prone
	if args.Term < rf.currentTerm {
		rf.say("get a stale RequestVote from ", args.CandidateId)
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUptodate(args.LastLogIndex, args.LastLogTerm)

	rf.say("reply voteGranted? ", reply.VoteGranted, " votedFor: ", rf.votedFor)
	if rf.role == follower {
		rf.resetElectionTimeout <- struct{}{}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// lock should be held by caller
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok && args.Term == rf.currentTerm {
		rf.rpcTermCheck(reply.Term, server)
	}
	return ok
}

type AppendEntriesArgs struct {
	// 2A
	Term     int
	LeaderId int

	// 2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      Logs
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.rpcTermCheck(args.Term, args.LeaderId)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// rf.say("get a stale AppendEntries from ", args.LeaderId)
		reply.Success = false
		return
	}

	// this rpc call is from valid leader
	// NOTE: error-prone
	if rf.role == candidate {
		rf.say("get a AppendEntries, become follower")
		rf.role = follower
	}
	if rf.role == follower {
		// rf.say("get a AppendEntries")
		rf.resetElectionTimeout <- struct{}{}
	}

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // NOTE: error-prone
		rf.say("mismatch entries")
		reply.Success = false
		return
	}

	if args.Entries != nil { // not a heartbeat
		rf.say("append entries: ", args.Entries)
		l := len(rf.log)
		start := args.PrevLogIndex + 1
		i := start
		for ; i < l && i-start < len(args.Entries); i++ {
			if rf.log[i].Term != args.Entries[i-start].Term {
				rf.log = rf.log[:i]
				break
			}
		}
		// rf.say("[", start, ":", i, ") match, copy from ", i)
		for ; i-start < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i-start])
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.say("update commit index from leader")
		rf.updateCommitIndex(Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))) // NOTE: error-prone
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// lock should be held by caller
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok && args.Term == rf.currentTerm {
		rf.rpcTermCheck(reply.Term, server)
	}
	return ok
}

// updateMatchIndex goroutine
func (rf *Raft) updateMatchIndex(server int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.matchIndex[server] == index {
		return
	}
	rf.say("update match index of ", server, " from ", rf.matchIndex[server], " to ", index)
	rf.matchIndex[server] = index

	mi := make([]int, len(rf.matchIndex))
	copy(mi, rf.matchIndex)
	sort.Ints(mi)
	maxN := -1
	for i := rf.majority() - 1; i >= 0; i-- {
		n := mi[i]
		if rf.log[n].Term == rf.currentTerm {
			maxN = n
			break
		}
	}
	if maxN > rf.commitIndex {
		rf.updateCommitIndex(maxN)
	}
}

// appendEntries goroutine
func (rf *Raft) appendEntries(server int, firstNext int, heartbeat bool) {
	n := firstNext
	args := AppendEntriesArgs{}
	args.LeaderId = rf.me
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	for {
		args.PrevLogIndex = n - 1
		rf.mu.Lock()

		// no longer valid AppendEntries
		if rf.killed() || rf.role != leader || n == 0 {
			rf.mu.Unlock()
			return
		}

		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		if !heartbeat {
			args.Entries = rf.log[n:]
		}
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, &args, &reply)

		rf.mu.Lock()

		// valid rpc call and not stale
		if ok && args.Term == rf.currentTerm {
			// match log
			if reply.Success {
				match := args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = match + 1
				go rf.updateMatchIndex(server, match)
				rf.mu.Unlock()
				return
			} else {
				// not match, decrement n and retry
				n--
			}
		} else if heartbeat {
			// heartbeat get error, but not log inconsistency, do not retry
			rf.mu.Unlock()
			return
		}
		// (rpc error or stale), not heartbeat, retry with the same n
		rf.mu.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != leader {
		isLeader = false
		return
	}

	index = len(rf.log)
	term = rf.currentTerm
	isLeader = true
	rf.say("log index ", index, " term ", term)
	rf.log = append(rf.log, &LogEntry{Command: command, Term: rf.currentTerm})
	rf.matchIndex[rf.me] = index // NOTE: important

	for server, next := range rf.nextIndex {
		if server == rf.me {
			continue
		}
		if index >= next {
			go rf.appendEntries(server, next, false)
		}
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = follower

	rf.stopHeartbeat = make(chan struct{})
	rf.stopWaitForElect = make(chan struct{})
	rf.resetElectionTimeout = make(chan struct{})

	rf.log = Logs{&LogEntry{Command: "dummy", Term: -1}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	go rf.waitForElect()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
