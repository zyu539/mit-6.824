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
	"math"
	"math/rand"
	"mit-6.824/labgob"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"mit-6.824/labgob"
	"mit-6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Cmd  string
	Term int64
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	heartBeatPeriod int                 // time interval between heart beats

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all server
	currentTerm int64
	votedFor    int32
	log         []*Log

	// Volatile state on all servers
	status      int32 // 0 for follower, 1 for candidate, 2 for leader
	commitIndex int64 // index of the highest log entry known to be committed.
	lastApplied int64 // index of the highest log entry applied to state machine
	receivedRPC bool  // whether the current server has received heart beat RPC

	// Volatile state on leaders
	nextIndex  []int64 // for each server, index of the next log entry to send.
	matchIndex []int64 // for each server, index of the highest log entry known to be replicated on server

	// Volatile state on candidate
	votes int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.status == 2

	return int(term), isLeader
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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int32
	LastLogIndex int64
	LastLogTerm  int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64 // currentTerm, for candidate to update itself
	VoteGranted bool  // true if candidate received vote
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []Log
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Reply false if term < currentTerm or already voted
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// fmt.Printf("%d, %d, %d", rf.currentTerm, args.Term, rf.votedFor)
	if rf.currentTerm > args.Term {
		return
	}

	// If RPC request or response contains term T > currentTerm,
	// set currentTerm = T, convert to follower, and reset vote status
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.status = 0
		rf.votedFor = -1
	}

	// If receiver has not voted in this term yet and candidate’s log is at least
	// as up-to-date as receiver’s log, grant vote
	var lastLogTerm int64
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		lastLogTerm = 1
	}

	if rf.votedFor < 0 && (args.LastLogTerm > lastLogTerm ||
		args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastApplied) {

		fmt.Printf("Follower %d voted for Candidate %d\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	// Upon receiving an AppendEntries from update-to-date leader request
	rf.receivedRPC = true

	// if RPC request or response contains term T > currentTerm,
	// set currentTerm = T, convert to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = reply.Term
		rf.status = 0
	}

	if int64(len(rf.log)) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}
	// TODO: append tries to log
	var lastNewEntryIndex int

	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int64(math.Min(float64(args.LeaderCommit), float64(lastNewEntryIndex)))
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) countVotes(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	requestTerm := args.Term
	if rf.sendRequestVote(server, args, reply) {
		rf.mu.Lock()

		// Check if the vote is for current term or the previous term before count up
		if requestTerm == rf.currentTerm && reply.VoteGranted {
			rf.votes++
			fmt.Printf("Candidate %d get vote from %d, current votes %d\n", rf.me, server, rf.votes)
		}

		// if RPC request or response contains term T > currentTerm,
		// set currentTerm = T, convert to follower
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.status = 0
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// if RPC request or response contains term T > currentTerm,
		// set currentTerm = T, convert to follower
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.status = 0
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) candidateBehavior() {
	// Election timeout is set to 400ms - 800ms
	timeout := int64(rand.Intn(400) + 400)
	start := time.Now().UnixMilli()

	// Set receiveRPC to false, vote for itself and increase current term by one
	// at the beginning of election cycle
	rf.mu.Lock()
	rf.receivedRPC = false
	rf.votedFor = int32(rf.me)
	rf.currentTerm += 1
	rf.votes = 1
	rf.mu.Unlock()

	fmt.Printf("Candidate %d started, with timeout of %d\n", rf.me, timeout)
	for i := range rf.peers {
		// Do not send RPC to itself
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		var lastLogTerm int64
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		} else {
			lastLogTerm = 1
		}
		rf.mu.Unlock()

		args := RequestVoteArgs{Term: rf.currentTerm, LastLogTerm: lastLogTerm}
		reply := RequestVoteReply{}
		go rf.countVotes(i, &args, &reply)
	}

	for time.Now().UnixMilli()-start <= timeout {
		rf.mu.Lock()
		receivedRPC := rf.receivedRPC
		votes := rf.votes
		status := rf.status
		rf.mu.Unlock()

		if status != 1 {
			return
		}

		if receivedRPC {
			atomic.StoreInt32(&rf.status, 0)
			return
		}

		if votes >= int32(len(rf.peers)+1)/2 {
			atomic.StoreInt32(&rf.status, 2)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) followerBehavior() {
	// Election timeout is set to 400ms - 800ms
	timeout := int64(rand.Intn(400) + 400)
	start := time.Now().UnixMilli()

	rf.mu.Lock()
	rf.receivedRPC = false
	voteForStatus := rf.votedFor
	rf.mu.Unlock()

	fmt.Printf("Follower %d started, with timeout of %d\n", rf.me, timeout)
	// Set receiveRPC to false at the beginning of election cycle
	for time.Now().UnixMilli()-start <= timeout {
		rf.mu.Lock()
		receivedRPC := rf.receivedRPC
		votedFor := rf.votedFor
		rf.mu.Unlock()

		if votedFor >= 0 && votedFor != voteForStatus || receivedRPC {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Did not receive any heartbeats / granted vote for any peers,
	// change to a candidate
	rf.mu.Lock()
	rf.status = 1
	rf.mu.Unlock()
}

func (rf *Raft) leaderBehavior() {
	for i := range rf.peers {
		// Do not send RPC to itself
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		go rf.handleAppendEntries(i, &args, &reply)
	}
	time.Sleep(150 * time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()

		if status == 2 {
			rf.leaderBehavior()
		} else if status == 1 {
			rf.candidateBehavior()
		} else {
			rf.followerBehavior()
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.currentTerm = 1

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
