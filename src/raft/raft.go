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

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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
	var sb strings.Builder
	sb.WriteString("[")
	for i, l := range log {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(strconv.Itoa(l.Term))
	}
	return sb.String()
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
	// Persistent state on all servers
	currentTerm int // latest term server has seen(initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term(or null if none)

	// Lab2B: log replication
	// Persistent state on all servers
	log Logs // log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	applyCh chan ApplyMsg

	role role

	resetCh  chan struct{}
	resignCh chan struct{}
	notifyCh chan struct{}
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log Logs

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		//rf.say("readPersist: fail")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func (rf *Raft) say(format string, something ...interface{}) {
	l := fmt.Sprintf("peer %v term %v %v\t: ", rf.me, rf.currentTerm, rf.role)
	l += format
	l += "\n"
	DPrintf(l, something...)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term        int // candidate's term
	CandidateId int // candidate requesting vote

	// 2B
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) acceptNewerTerm(inTerm int, vote int) {
	rf.mu.Lock()
	isNolongerLeader := false
	// [raft-paper, all-servers]
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if inTerm > rf.currentTerm {
		//rf.say("acceptNewerTerm: inTerm %v is newer than currentTerm %v, become follower", inTerm, rf.currentTerm)
		if rf.role == leader {
			isNolongerLeader = true
		}
		rf.currentTerm = inTerm
		rf.role = follower
		rf.votedFor = vote // NOTE: very important
		// rf.persist()
	}
	rf.mu.Unlock()
	if isNolongerLeader {
		//rf.say("acceptNewerTerm: no longer leader, send resign to channel")
		rf.resignCh <- struct{}{}
		//rf.say("not block")
	}
}

func (rf *Raft) isCandidateUptodate(lastLogIndex int, lastLogTerm int) bool {
	// [raft-paper, up-to-date]
	// Raft determines which of two logs is more up-to-date by comparing the index and temr of
	// the last entries in the logs. If the logs have last entries with different terms, then the log
	// with the later term is more up-to-date. If the logs end with the same term, then whichever log is longer
	// is more up-to-date
	var ret bool
	myLastLog := rf.log[len(rf.log)-1]
	if myLastLog.Term < lastLogTerm {
		ret = true
	}
	if myLastLog.Term == lastLogTerm {
		ret = len(rf.log)-1 <= lastLogIndex
	}
	if myLastLog.Term > lastLogTerm {
		ret = false
	}
	//rf.say("isCandidateUptodate: %v, lastLogIndex %v, lastLogTerm %v, myLastLogIndex %v, myLasterLogTerm %v",
		//ret, lastLogIndex, lastLogTerm, len(rf.log)-1, myLastLog.Term)
	return ret
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.acceptNewerTerm(args.Term, args.CandidateId)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// [raft-paper, RequestVoteRPC receiver]
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		//rf.say("RequestVote: stale from %v", args.CandidateId)
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	// [raft-paper, RequestVoteRPC receiver]
	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.isCandidateUptodate(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
			rf.votedFor = -1
		}
		rf.persist()
	}

	//rf.say("RequestVote: voteGranted? %v, votedFor %v", reply.VoteGranted, rf.votedFor)
	isVoted := rf.role == follower && reply.VoteGranted
	rf.mu.Unlock()
	if isVoted {
		// [raft-paper, follower]
		// If eleciton timeout elapses without 
		// receiving AppendEntries RPC from current leader 
		// or granting vote to candidate: convert to candidate
		//rf.say("RequestVote: send reset to channel")
		rf.resetCh <- struct{}{}
		//rf.say("not block")
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
	// lock should be unheld
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.acceptNewerTerm(reply.Term, server)
	}
	return ok
}

type AppendEntriesArgs struct {
	// 2A
	Term     int // leader's term
	LeaderId int // so follower can redirect clients

	// 2B
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      Logs // log entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	FirstConflictIndex int
	ConflictTerm       int
	LastIndex          int
}

// apply goroutine
func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	//rf.say("apply: lastApplied %v, commitIndex %v", rf.lastApplied, rf.commitIndex)
	// [raft-paper, all-servers]
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
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
	//rf.say("updateCommitIndex: from %v to %v", rf.commitIndex, index)
	rf.commitIndex = index
	go rf.apply()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.acceptNewerTerm(args.Term, args.LeaderId)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// [raft-paper, AppendEntriesRPC receiver]
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		//rf.say("AppendEntries: stale from %v", args.LeaderId)
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// this rpc call is from valid leader
	if rf.role == candidate {
		// [raft-paper, candidate]
		// If AppendEntries RPC received from new leader: convert to follower
		//rf.say("AppendEntries: candidate become follower")
		rf.role = follower
	}
	if rf.role == follower {
		// [raft-paper, follower]
		// If eleciton timeout elapses without 
		// receiving AppendEntries RPC from current leader 
		// or granting vote to candidate: convert to candidate
		rf.mu.Unlock()
		//rf.say("AppendEntries: send reset to channel")
		rf.resetCh <- struct{}{}
		//rf.say("not block")
		rf.mu.Lock()
	}

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// [raft-paper, AppendEntriesRPC receiver]
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		//rf.say("AppendEntries: mismatch, prevLogIndex %v, prevLogTerm %v\nmy log %s", args.PrevLogIndex, args.PrevLogTerm, rf.log)
		reply.Success = false

		last := Min(args.PrevLogIndex, len(rf.log)-1)
		term := rf.log[last].Term
		reply.ConflictTerm = term
		reply.LastIndex = len(rf.log) - 1

		if last == 0 {
			reply.FirstConflictIndex = last
		} else {
			for ; last > 0 && rf.log[last].Term == term; last-- {
			}
			last++

			reply.FirstConflictIndex = last
		}
		//rf.say("AppendEntries: conflictTerm %v, firstConflictIndex %v", reply.ConflictTerm, reply.FirstConflictIndex)
		rf.mu.Unlock()
		return
	}

	if args.Entries != nil { // not a heartbeat
		//rf.say("AppendEntries: entries %v", args.Entries)
		l := len(rf.log)
		start := args.PrevLogIndex + 1
		i := start
		dirty := false
		for ; i < l && i-start < len(args.Entries); i++ {
			// [raft-paper, AppendEntriesRPC receiver]
			// If an existing entry conflicts with a new one(same index but different term),
			// delete the existing entry and all that follow it
			if rf.log[i].Term != args.Entries[i-start].Term {
				rf.log = rf.log[:i]
				dirty = true
				break
			}
		}
		for ; i-start < len(args.Entries); i++ {
			dirty = true
			// [raft-paper, AppendEntriesRPC receiver]
			// Append any new entries not already in the log
			rf.log = append(rf.log, args.Entries[i-start])
		}
		if dirty {
			rf.persist()
		}
	}

	// [raft-paper, AppendEntriesRPC receiver]
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		//rf.say("AppendEntries: leaderCommit %v is newer than commitIndex %v", args.LeaderCommit, rf.commitIndex)
		rf.updateCommitIndex(Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
	}

	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// lock should be unheld
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.acceptNewerTerm(reply.Term, server)
	}
	return ok
}

func (rf *Raft) majority() int {
	return (len(rf.peers) + 1) / 2
}

// updateMatchIndex goroutine will set matchIndex and may set commitIndex
func (rf *Raft) updateMatchIndex(server int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.matchIndex[server] >= index {
		return
	}
	//rf.say("updateMatchIndex: of server %v, from %v to %v", server, rf.matchIndex[server], index)
	rf.matchIndex[server] = index

	// [raft-paper]If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, 
	// and log[N].term == currentTerm: set commitIndex = N
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
// [raft-paper]If last log index >= nextIndex for a follower:
// send AppendEntries RPC with log entries starting at nextIndex
// - If successful: update nextIndex and matchIndex for follower
// - If AppendEntries fails because of log inconsistency:
//   decrement nextIndex and retry
func (rf *Raft) appendEntries(server int) {
	for {
		rf.mu.Lock()
		// No longer valid AppendEntries
		if rf.killed() || rf.role != leader {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{}
		args.LeaderId = rf.me
		args.Term = rf.currentTerm
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		// If last log index >= nextIndex, then args.Entries will not be nil
		// Otherwise, args.Entries == nil, and AppendEnties will be a heartbeat
		args.Entries = rf.log[rf.nextIndex[server]:]
		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, &args, &reply)

		rf.mu.Lock()

		// Valid rpc call and not stale
		if ok && args.Term == rf.currentTerm {
			if reply.Success {
				// If successful: update nextIndex and matchIndex for follower
				match := args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = match + 1
				go rf.updateMatchIndex(server, match)
			} else {
				// If AppendEntries fails because of log inconsistency:
				//   decrement nextIndex and retry
				// use fast decrement rather than decrement by 1
				index := args.PrevLogIndex
				if index > reply.LastIndex {
					index = reply.LastIndex
				}
				for ; index >= reply.FirstConflictIndex && rf.log[index].Term != reply.ConflictTerm; index-- {
				}

				rf.nextIndex[server] = index + 1

				//rf.say("appendEntries: mismatch, prevLogIndex %v, prevLogTerm %v, conflictTerm %v, firstConflictIndex %v\nmy log %s\ndecrement next to %v",
					//args.PrevLogIndex, args.PrevLogTerm, reply.ConflictTerm, reply.FirstConflictIndex, rf.log, index+1)

				rf.mu.Unlock()
				continue
			}
		}
		rf.mu.Unlock()
		return
	}
}

const heartbeatInterval = 150 * time.Millisecond
const eTimeoutLeft = 400
const eTimeoutRight = 600

// notify goroutine 
func (rf *Raft) notify() {
	ticker := time.NewTicker(heartbeatInterval)
	for {
		select {
		case <-rf.notifyCh:
		case <-ticker.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.role == leader {
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					//rf.say("notify: to %v", i)
					go rf.appendEntries(i)
				}
			}
			rf.mu.Unlock()
		case <-rf.resignCh:
			//rf.say("notify: resign")
			go rf.wait()
			return
		}
	}
}

// campaign is for Candidate to campaign for Leader,
// returns a channel indicating whether elected
// goroutines created will not be halted when start another round of election
// but this will be correct, because whenever another round of election is started
// elected channel of this round will be abandoned.
func (rf *Raft) campaign() (elected chan struct{}) {
	// [raft-paper]On conversion to candidate, start election:
	// - Increment currentTerm
	// - Vote for self
	// - Reset election timer * in wait()
	// - Send RequestVote RPCs to all other servers
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	voteCh := make(chan struct{})
	elected = make(chan struct{})
	go func() {
		votes := 1
		for range voteCh {
			votes++
			// [raft-paper]If votes received from majority of servers: become leader
			if votes == rf.majority() {
				close(elected)
			}
		}
	}()
	go func() {
		var wg sync.WaitGroup
		rf.mu.Lock()
		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && args.Term == rf.currentTerm && reply.VoteGranted {
					voteCh <- struct{}{}
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
		close(voteCh)
	}()
	return
}

// wait goroutine is for Follower to wait for campaign
func (rf *Raft) wait() {
	elected := make(chan struct{})
	for {
		eTimeout := time.Duration(RandIntRange(eTimeoutLeft, eTimeoutRight)) * time.Millisecond
		timer := time.NewTimer(eTimeout)
		select {
		case <-timer.C:
			rf.mu.Lock()
			if rf.killed() || rf.role == leader {
				return
			}
			if rf.role == follower {
				// [raft-paper]If eleciton timeout elapses without 
				// receiving AppendEntries RPC from current leader 
				// or granting vote to candidate: convert to candidate
				rf.role = candidate
			}
			if rf.role == candidate {
				// [raft-paper]If election timeout elapses: start new election
				//rf.say("wait: election timeout, start a new election")
				elected = rf.campaign()
			}
			rf.mu.Unlock()
		case <-rf.resetCh:
			//rf.say("wait: reset election timeout")
			elected = make(chan struct{})
			continue
		case <-elected:
			rf.mu.Lock()
			if rf.role != candidate {
				rf.mu.Unlock()
				continue
			}
			rf.role = leader
			//rf.say("wait: become leader")

			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}
			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
			}
			// [raft-paper]Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server;
			// repeat during idle periods to prevent election timeout
			go rf.notify()
			rf.mu.Unlock()
			//rf.say("wait: trigger initial heartbeat")
			rf.notifyCh <- struct{}{}
			//rf.say("not block")
			return
		}
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
	if rf.role != leader {
		isLeader = false
		rf.mu.Unlock()
		return
	}

	index = len(rf.log)
	term = rf.currentTerm
	isLeader = true
	//rf.say("Start: index %v, term %v", index, term)
	// [raft-paper]If command received from client: append entry to local log,
	// respond after entry applied to state machine
	rf.log = append(rf.log, &LogEntry{Command: command, Term: rf.currentTerm})

	rf.persist()

	rf.matchIndex[rf.me] = index // NOTE: important

	rf.mu.Unlock()

	rf.notifyCh <- struct{}{}

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

	rf.resignCh = make(chan struct{})
	rf.resetCh = make(chan struct{})

	rf.log = Logs{&LogEntry{Command: "dummy", Term: -1}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.notifyCh = make(chan struct{})

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.wait()

	return rf
}
