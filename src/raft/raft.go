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
	Command interface{}
	Term    int
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
	applyCh         chan ApplyMsg       // A channel to send apply message

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all server
	currentTerm       int
	votedFor          int
	logs              map[int]Log
	lastLogIndex      int
	lastSnapshotIndex int
	lastSnapshotTerm  int

	// Volatile state on all servers
	status      int  // 0 for follower, 1 for candidate, 2 for leader
	commitIndex int  // index of the highest log entry known to be committed.
	lastApplied int  // index of the highest log entry applied to state machine
	receivedRPC bool // whether the current server has received heart beat RPC

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send.
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server

	// Volatile state on candidate
	votes int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.status == 2

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) writeByte() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	return w.Bytes()
}

func (rf *Raft) encodeSnapshot(snapshot []byte) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshot)
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveRaftState(rf.writeByte())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.logs) != nil ||
		d.Decode(&rf.lastLogIndex) != nil || d.Decode(&rf.lastSnapshotIndex) != nil ||
		d.Decode(&rf.lastSnapshotTerm) != nil {
		panic("Fail to decode persist state for server")
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	// If the snapshot is outdated, reply without changing
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[%d] write cond snapshot. rf.lastApplied: %d, lastIncludedIndex: %d, lastIncludedTerm: %d, rf.logs: %v\n", rf.me, rf.lastApplied, lastIncludedIndex, lastIncludedTerm, rf.logs)
	if rf.logs[rf.lastApplied].Term >= lastIncludedTerm || rf.lastApplied >= lastIncludedIndex {
		return false
	}

	for i := rf.lastSnapshotIndex + 1; i <= lastIncludedIndex; i++ {
		rf.lastSnapshotIndex = i
		rf.lastSnapshotTerm = rf.logs[i].Term
		delete(rf.logs, i)
	}
	rf.persister.SaveStateAndSnapshot(rf.writeByte(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[%d] write snapshot. rf.lastSnapshotIndex: %d, index: %d, rf.logs: %v\n", rf.me, rf.lastSnapshotIndex, index, rf.logs)
	// If the input index is less than the current snapshot index, return
	if index <= rf.lastSnapshotIndex {
		return
	}
	for i := rf.lastSnapshotIndex + 1; i <= index; i++ {
		rf.lastSnapshotIndex = i
		rf.lastSnapshotTerm = rf.logs[i].Term
		delete(rf.logs, i)
	}
	rf.persister.SaveStateAndSnapshot(rf.writeByte(), snapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true if candidate received vote
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) ApplyLogs() {
	var messageQueue []ApplyMsg
	// fmt.Printf("%d, %d, %d, %d, %v\n", rf.me, rf.status, rf.lastApplied, rf.commitIndex, rf.logs)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.mu.Lock()
		messageQueue = append(
			messageQueue,
			ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			})
		rf.mu.Unlock()
	}
	for _, msg := range messageQueue {
		rf.applyCh <- msg
		rf.lastApplied++
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Reply false if term < currentTerm or already voted
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	}

	// VoteFor may have changed in later code, persist the state
	defer rf.persist()

	// If RPC request or response contains term T > currentTerm,
	// set currentTerm = T, convert to follower, and reset vote status
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.status = 0
		rf.votedFor = -1
	}

	// If receiver has not voted in this term yet and candidate???s log is at least
	// as up-to-date as receiver???s log, grant vote
	var lastLogTerm int
	if rf.lastLogIndex > rf.lastSnapshotIndex {
		lastLogTerm = rf.logs[rf.lastLogIndex].Term
	} else {
		lastLogTerm = rf.lastSnapshotTerm
	}

	if rf.votedFor < 0 && (args.LastLogTerm > lastLogTerm ||
		args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastLogIndex) {

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

	defer rf.persist()

	// if RPC request or response contains term T > currentTerm,
	// set currentTerm = T, convert to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = reply.Term
		rf.status = 0
	}

	var prevLogTerm int
	if args.PrevLogIndex <= rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.logs[args.PrevLogIndex].Term
	}

	if rf.lastLogIndex < args.PrevLogIndex || args.PrevLogIndex >= 0 && prevLogTerm != args.PrevLogTerm {
		// fmt.Printf("Args PrevLogIndex: %d; Args PrevLogTerm: %d; Logs: %v; entries: %v\n", args.PrevLogIndex, args.PrevLogTerm, rf.logs, args.Entries)
		return
	}

	// Append tries to log
	lastNewEntryIndex := args.PrevLogIndex
	modified := false
	for _, entry := range args.Entries {
		lastNewEntryIndex++
		if lastNewEntryIndex <= rf.lastLogIndex && rf.logs[lastNewEntryIndex].Term != entry.Term {
			rf.logs[lastNewEntryIndex] = Log{Term: entry.Term, Command: entry.Command}
			modified = true
		} else if lastNewEntryIndex > rf.lastLogIndex {
			rf.lastLogIndex++
			rf.logs[rf.lastLogIndex] = Log{Term: entry.Term, Command: entry.Command}
		}
	}
	// fmt.Printf("Args PrevLogIndex: %d; Args PrevLogTerm: %d; Logs: %v; entries: %v\n", args.PrevLogIndex, args.PrevLogTerm, rf.logs, args.Entries)
	// fmt.Printf("Follower %d, current term: %d, last log index: %d, time %d, log: %v\n", rf.me, rf.currentTerm, rf.lastLogIndex, time.Now().UnixMilli(), rf.logs)
	// If the log has conflicts been overwritten, truncate all the following logs
	for i := lastNewEntryIndex + 1; modified && i <= rf.lastLogIndex; i++ {
		delete(rf.logs, i)
	}
	rf.lastLogIndex = lastNewEntryIndex

	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		// fmt.Printf("%d Update commitIndex from %d to %d, %d\n", rf.me, rf.commitIndex, args.LeaderCommit, lastNewEntryIndex)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewEntryIndex)))
		go rf.ApplyLogs()
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
			// fmt.Printf("Candidate %d get vote from %d, current votes %d, time: %d\n", rf.me, server, rf.votes, time.Now().UnixMilli())
		}

		// if RPC request or response contains term T > currentTerm,
		// set currentTerm = T, convert to follower
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.status = 0
			rf.persist()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// if RPC request or response contains term T > currentTerm,
		// set currentTerm = T, convert to follower, and return directly
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.status = 0
			rf.persist()
			return
		}

		if reply.Success {
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newMatchIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatchIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.updateLeaderCommitId() // Only update the leader commit id after successfully append
		} else if reply.Term < args.Term {
			rf.nextIndex[server] = args.PrevLogIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.status == 2

	// Your code here (2B).
	if !isLeader {
		return -1, rf.currentTerm, false
	}

	rf.lastLogIndex++
	rf.logs[rf.lastLogIndex] = Log{Term: rf.currentTerm, Command: command}
	rf.persist()

	return rf.lastLogIndex, rf.currentTerm, isLeader
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
	// Election timeout is set to 300ms - 600ms
	timeout := rand.Intn(300) + 300
	start := time.Now().UnixMilli()

	// Set receiveRPC to false, vote for itself and increase current term by one
	// at the beginning of election cycle
	rf.mu.Lock()
	rf.receivedRPC = false
	rf.votedFor = rf.me
	rf.currentTerm += 1
	rf.votes = 1
	rf.persist() // VotedFor & CurrentTerm changed, persist the state
	rf.mu.Unlock()

	// fmt.Printf("Candidate [%d] started, with timeout %d, term: %d, time: %d\n", rf.me, timeout, rf.currentTerm, time.Now().UnixMilli())
	for i := range rf.peers {
		// Do not send RPC to itself
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		var lastLogTerm int
		if rf.lastLogIndex > rf.lastSnapshotIndex {
			lastLogTerm = rf.logs[rf.lastLogIndex].Term
		} else {
			lastLogTerm = rf.lastSnapshotTerm
		}

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogTerm:  lastLogTerm,
			LastLogIndex: rf.lastLogIndex,
		}
		reply := RequestVoteReply{}
		rf.mu.Unlock()

		go rf.countVotes(i, &args, &reply)
	}

	for int(time.Now().UnixMilli()-start) <= timeout {
		rf.mu.Lock()
		receivedRPC := rf.receivedRPC
		votes := rf.votes
		status := rf.status
		rf.mu.Unlock()

		if status != 1 {
			return
		}

		if receivedRPC {
			rf.mu.Lock()
			rf.status = 0
			rf.mu.Unlock()
			return
		}

		if votes >= (len(rf.peers)+1)/2 {
			rf.mu.Lock()
			rf.status = 2
			rf.mu.Unlock()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) followerBehavior() {
	// Election timeout is set to 300ms - 600ms
	timeout := rand.Intn(300) + 300
	start := time.Now().UnixMilli()

	rf.mu.Lock()
	rf.receivedRPC = false
	voteForStatus := rf.votedFor
	rf.mu.Unlock()

	// fmt.Printf("Follower [%d] started, with timeout of %d, current Term: %d, time: %d\n", rf.me, timeout, rf.currentTerm, time.Now().UnixMilli())
	// Set receiveRPC to false at the beginning of election cycle
	for int(time.Now().UnixMilli()-start) <= timeout {
		rf.mu.Lock()
		receivedRPC := rf.receivedRPC
		votedFor := rf.votedFor
		rf.mu.Unlock()

		if votedFor >= 0 && votedFor != voteForStatus || receivedRPC {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Did not receive any heartbeats / granted vote for any peers,
	// change to a candidate
	rf.mu.Lock()
	rf.status = 1
	rf.mu.Unlock()
}

func (rf *Raft) leaderBehavior() {
	// fmt.Printf("Leader [%d], current term: %d, matched ids: %v, nextIndex: %v, time: %d, log: %v\n", rf.me, rf.currentTerm, rf.matchIndex, rf.nextIndex, time.Now().UnixMilli(), rf.logs)
	for i := range rf.peers {
		// Do not send RPC to itself
		if i == rf.me {
			continue
		}

		rf.mu.Lock()

		if rf.nextIndex[i] < rf.lastSnapshotIndex+1 {
			rf.nextIndex[i] = rf.lastSnapshotIndex + 1
		}
		var entries []Log
		for j := rf.nextIndex[i]; j <= rf.lastLogIndex; j++ {
			entries = append(entries, rf.logs[j])
		}

		var prevLogTerm int
		if rf.nextIndex[i]-1 <= rf.lastSnapshotIndex {
			prevLogTerm = rf.lastSnapshotTerm
		} else {
			prevLogTerm = rf.logs[rf.nextIndex[i]-1].Term
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: rf.nextIndex[i] - 1,
			Entries:      entries,
		}
		reply := AppendEntriesReply{}
		// fmt.Printf("[%d] Sending AppendEntriesArgs request last snapshot index: %v, term: %v, nextIndex: %v, logs: %v\n", rf.me, rf.lastSnapshotIndex, rf.lastSnapshotTerm, rf.nextIndex, rf.logs)
		rf.mu.Unlock()
		go rf.handleAppendEntries(i, &args, &reply)
	}
	time.Sleep(120 * time.Millisecond)
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ??? N,
// and log[N].term == currentTerm, then set commitIndex = N
func (rf *Raft) updateLeaderCommitId() {
	for N := rf.lastLogIndex; N > rf.commitIndex; N-- {
		if rf.logs[N].Term != rf.currentTerm {
			return
		}
		matchCount := 1
		for j := range rf.matchIndex {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= N {
				matchCount++
				if matchCount > len(rf.peers)/2 {
					rf.commitIndex = N
					go rf.ApplyLogs()
					return
				}
			}
		}
	}
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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm or already voted
	if rf.currentTerm > args.Term {
		return
	}

	// If the snapshot is outdated, reply without changing
	if rf.lastSnapshotTerm >= args.LastIncludedTerm || rf.lastSnapshotIndex >= args.LastIncludedIndex {
		return
	}

	fmt.Printf("[%d] install snapshot. rf.lastSnapshotTerm: %d, rf.lastSnapshotIndex: %d, args.LastIncludedTerm: %d, args.LastIncludedIndex: %d\n", rf.me, rf.lastSnapshotTerm, rf.lastSnapshotIndex, args.LastIncludedTerm, args.LastIncludedIndex)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) applySnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.sendInstallSnapshot(server, args, reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) SnapshotChecker() {
	for rf.killed() == false {
		rf.mu.Lock()
		status := rf.status
		leaderId := rf.me
		rf.mu.Unlock()

		if status != 2 {
			for i := range rf.peers {
				if i == leaderId {
					continue
				}

				rf.mu.Lock()
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedTerm:  rf.lastSnapshotTerm,
					LastIncludedIndex: rf.lastSnapshotIndex,
					data:              rf.persister.ReadSnapshot(),
				}
				reply := InstallSnapshotReply{}
				rf.mu.Unlock()

				go rf.applySnapshot(i, &args, &reply)
			}
		}

		time.Sleep(300 * time.Millisecond)
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentTerm = 1
	rf.status = 0
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 1
	rf.applyCh = applyCh
	rf.logs = make(map[int]Log)
	rf.lastLogIndex = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := range rf.matchIndex {
		rf.nextIndex[i] = rf.lastSnapshotIndex + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.SnapshotChecker()

	return rf
}
