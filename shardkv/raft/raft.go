package raft

//
// rf = Make(peers, me,...)
//   creates a new Raft server.
//
// rf.Start(command interface{}) (index, term, isleader)
//   starts agreement on a new log entry.
//
// rf.GetState() (term, isLeader)
//   asks a Raft for its current term, and whether it thinks it is leader.
//
// ApplyMsg
//   is sent by each Raft peer to the service each time
//   a new entry each time a new entry is committed to the log
//

import (
	"bytes"
	"encoding/gob"
	"lab5/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	follower = iota
	candidate
	leader

	timeoutIntv   = 250
	randomIntv    = 250
	heartbeatIntv = 100
	opIntv1       = 2
	opIntv2       = 1
)

//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer sends an ApplyMsg to the service on the same
// server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	Command      interface{}

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Inter-service communication channels
	applyCh chan ApplyMsg

	// Persistent state on all servers
	currentTerm       int
	votedFor          int
	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state        int
	timeoutIndex int
	timeout      bool
	cond         *sync.Cond

	// Heuristic
	leaderConfirmed bool

	isKilled int32 // set by Kill()
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var term int
	err := d.Decode(&term)
	if err != nil {
		return
	}
	var votedFor int
	err = d.Decode(&votedFor)
	if err != nil {
		return
	}
	var log []LogEntry
	err = d.Decode(&log)
	if err != nil {
		return
	}
	var lastIncludedIndex int
	err = d.Decode(&lastIncludedIndex)
	if err != nil {
		return
	}
	var lastIncludedTerm int
	err = d.Decode(&lastIncludedTerm)
	if err != nil {
		return
	}
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm ||
		args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
	}

	// If existing log entry has same index and term as snapshot's
	// last included entry, retain log entries folloiwing it,
	// discard the rest and reply
	if len(rf.log) > 0 &&
		args.LastIncludedIndex < rf.log[len(rf.log)-1].Index {
		for i := range rf.log {
			if rf.log[i].Index == args.LastIncludedIndex &&
				rf.log[i].Term == args.LastIncludedTerm {
				rf.lastIncludedTerm = rf.log[i].Term
				rf.log = append(make([]LogEntry, 0), rf.log[i+1:]...)
				goto done
			}
		}
	}
	// Discard the entrie log
	rf.log = make([]LogEntry, 0)
	rf.lastIncludedTerm = args.LastIncludedTerm

done:
	rf.lastIncludedIndex = args.LastIncludedIndex

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, args.Data)

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		// Reset state machine using snapshot contents
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
		rf.lastApplied = args.LastIncludedIndex
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", *args, reply)
	return ok
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if index <= rf.lastIncludedIndex {
			return
		}
		for i := range rf.log {
			if rf.log[i].Index == index {
				rf.lastIncludedTerm = rf.log[i].Term
				rf.log = append(make([]LogEntry, 0), rf.log[i+1:]...)
				break
			}
		}
		rf.lastIncludedIndex = index
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.log)
		e.Encode(rf.lastIncludedIndex)
		e.Encode(rf.lastIncludedTerm)
		state := w.Bytes()
		rf.persister.SaveStateAndSnapshot(state, snapshot)
	}()
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term == rf.currentTerm &&
		(rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term == rf.currentTerm &&
		(rf.leaderConfirmed || rf.state == leader) {
		reply.Term = rf.currentTerm
		return
	}
	if len(rf.log) == 0 {
		if args.LastLogTerm < rf.lastIncludedTerm ||
			(args.LastLogTerm == rf.lastIncludedTerm &&
				args.LastLogIndex < rf.lastIncludedIndex) {
			if args.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				rf.state = follower
				rf.votedFor = -1
				rf.persist()
			}
			reply.Term = rf.currentTerm
			return
		}
	} else {
		if args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
				args.LastLogIndex-rf.lastIncludedIndex < len(rf.log)) {
			if args.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				rf.state = follower
				rf.votedFor = -1
				rf.persist()
			}
			reply.Term = rf.currentTerm
			return
		}
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.state = follower

	//
	// Resetting the electection timer here makes it equally
	// likely for a server with an outdated log to step forward
	// as for the server with a longer log. It is especially
	// important in slow and unrealiable networks, where it is
	// very likely that followers has different logs.
	//
	rf.timeout = false
	rf.cond.Signal()
	rf.persist()
}

//
// Sends a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
//
// Returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", *args, reply)
	return ok
}

func (rf *Raft) startLeaderElection() {
	rf.mu.Lock()
	term := rf.currentTerm
	vote := 1

	// Issue RequestVote RPC to each of the other servers
	// Changed
	args := RequestVoteArgs{
		Term:        term,
		CandidateId: rf.me,
	}
	if len(rf.log) == 0 {
		args.LastLogIndex = rf.lastIncludedIndex
		args.LastLogTerm = rf.lastIncludedTerm
	} else {
		args.LastLogIndex = rf.log[len(rf.log)-1].Index
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	voteCh := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(svr int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(svr, &args, &reply)
			if !ok {
				voteCh <- false
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				voteCh <- false
				return
			}
			rf.mu.Unlock()

			// Need to check if the reply.Term matches
			// term sent in the orignial RPC because there
			// can be cases where both the candidate and
			// the follower has already moved to higher terms
			if reply.Term != term ||
				!reply.VoteGranted {
				voteCh <- false
				return
			}
			voteCh <- true
		}(i)
	}
	rf.mu.Unlock()

	// Wait for replies from the majority of servers
	majority := (len(rf.peers) + 1) / 2
	for i := 0; i < len(rf.peers)-1; i++ {
		voteGranted := <-voteCh
		if voteGranted {
			vote++
		}
		if vote >= majority {
			break
		}
	}
	rf.mu.Lock()
	if rf.currentTerm == term && vote >= majority {
		rf.state = leader

		// Initialize the two lists that the leader keeps
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.log)
			if i == rf.me {
				rf.matchIndex[i] =
					rf.lastIncludedIndex + len(rf.log) - 1
				continue
			}
			rf.matchIndex[i] = 0
		}
		go rf.doLeaderOperations()

		rf.timeout = false
		rf.cond.Signal()
	}
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XIndex  int
	XTerm   int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.XIndex = -1
		reply.XTerm = -1
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
		rf.persist()
	}

	logOff := rf.lastIncludedIndex + 1

	//
	// Instead of just replying false when log does not contain
	// any entry at PrevLogIndex whos term matches prevLogTerm,
	// as described in the paper, we do the following optimization
	// to accelerate log backtracking:
	//
	// - If a follower does not have prevLogIndex in its log,
	//   it returns with conflictIndex = len(log) and
	//   conflictTerm = -1 (None)
	// - If a follower does have prevLogIndex in its log, but the term
	//   does not match, it returns conflictTerm = log[preLogIndex].Term,
	//   then search its log for the first index whose entry has term
	//   that matches conflictTerm.
	//
	// The conflictIndex is returned as Xindex and the conflitTerm
	// (if any) is returned as Xterm in AppendEntriesReply.
	//
	// The accelerated log backtracking optimization is mentioned
	// in the paper but is very underspecidied. However it becomes
	// necessary when the protocal is operated upon a slow and
	// unreliable network, such as the one that the tests marked
	// "unreliable" emulate.
	//
	if args.PrevLogIndex-logOff >= len(rf.log) {
		// Does not have prevLogIndex in log; case 1
		reply.Term = rf.currentTerm
		reply.XIndex = len(rf.log) + logOff
		reply.XTerm = -1
		rf.leaderConfirmed = true
		rf.timeout = false
		rf.cond.Signal()
		return
	}
	if args.PrevLogIndex-logOff >= 0 && len(rf.log) > 0 &&
		rf.log[args.PrevLogIndex-logOff].Term != args.PrevLogTerm {
		// Has preLogIndex in log but the term doesn't match; case 2
		reply.Term = rf.currentTerm
		reply.XTerm = rf.log[args.PrevLogIndex-logOff].Term
		XXTermIndex := args.PrevLogIndex
		for rf.log[XXTermIndex-logOff].Term == reply.XTerm {
			XXTermIndex--
		}
		reply.XIndex = XXTermIndex + 1
		rf.leaderConfirmed = true
		rf.timeout = false
		rf.cond.Signal()
		return
	}

	// If an existing entry conflicts with a new one (same
	// index but different terms), overwrite the conflicting
	// entry with new entries not already in the log and
	// delete all that follow it
	addAt := args.PrevLogIndex + 1
	var i int
	for i = 0; i < len(args.Entries); i++ {
		if addAt < logOff {
			// i = logOff - addStartWith
			// addStartWith = logOff
			// max(logOff, commitIndex)
			jumpTo := Max(rf.commitIndex+1, logOff)
			i = jumpTo - addAt
			addAt = jumpTo
		}
		if addAt-logOff >= len(rf.log) || i >= len(args.Entries) ||
			rf.log[addAt-logOff].Term != args.Entries[i].Term {
			break
		}
		addAt++
	}
	if i < len(args.Entries) {
		rf.log = append(rf.log[:addAt-logOff], args.Entries[i:]...)
		rf.persist()
	}
	// Only delete the existing entry and all that follows if there is conflict

	// If leaderCommit > commitIndex, set commit be the less
	// of leaderCommit and the index of the last new entry
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(len(rf.log)-1+logOff, args.LeaderCommit)
	}

	// If commitIndex > lastApplied: increment lastApplied,
	// apply log[lastApplied] to state machine
	go rf.applyEntries()

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.XIndex = -1
	reply.XTerm = -1
	rf.leaderConfirmed = true
	rf.timeout = false
	rf.cond.Signal()
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", *args, reply)
	return ok
}

func (rf *Raft) replicateEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return
	}
	term := rf.currentTerm
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(svr int) {
			rf.mu.Lock()
			for rf.nextIndex[svr] <= rf.lastIncludedIndex {
				// Need to bring the follower up-to-date by
				// sending a snapshot to it
				args := InstallSnapshotArgs{
					Term:              term,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				rf.mu.Unlock()
				var reply InstallSnapshotReply
				ok := rf.sendInstallSnapshot(svr, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				if rf.state != leader {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = follower
					rf.votedFor = -1
					rf.persist()
					rf.mu.Unlock()
					return
				}
				rf.nextIndex[svr] = args.LastIncludedIndex + 1
				rf.matchIndex[svr] = args.LastIncludedIndex
			}
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[svr] - 1,
				LeaderCommit: rf.commitIndex,
			}
			logOff := rf.lastIncludedIndex + 1
			if rf.nextIndex[svr]-1-logOff < 0 {
				args.PrevLogTerm = rf.lastIncludedTerm
			} else {
				args.PrevLogTerm = rf.log[rf.nextIndex[svr]-1-logOff].Term
			}
			if rf.nextIndex[svr]-logOff < len(rf.log) {
				args.Entries = rf.log[rf.nextIndex[svr]-logOff:]
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(svr, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != leader {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = follower
				rf.votedFor = -1
				rf.persist()
				return
			}

			// Drop the reply and return for old RPC replies,
			// which happens when both the leader and the follow
			// has moved forward and pass the term sent in the
			// original RPC
			if reply.Term != term {
				return
			}

			if !reply.Success {
				if reply.XTerm == -1 {
					// Case 1
					rf.nextIndex[svr] = reply.XIndex
					return
				}
				if rf.lastIncludedIndex > logOff {
					// There were updates on the log; the log
					// offset is no longer valid and cannot be
					// used for futher backtracing operations
					rf.nextIndex[svr] = rf.lastIncludedIndex
					return
				}
				// Case 2
				// Search for the last entrie with XTerm
				lstEntXTerm := args.PrevLogIndex - 1 // rf.nextIndex[svr] - 2
				for lstEntXTerm-logOff > 0 &&
					rf.log[lstEntXTerm-logOff].Term != reply.XTerm {
					lstEntXTerm--
				} // somethings' not right with this formation
				if lstEntXTerm-logOff <= 0 {
					// Leader does not has XTerm:
					// all entries with XTerm in follower's
					// log have to be overwritten
					rf.nextIndex[svr] = reply.XIndex
					return
				}
				rf.nextIndex[svr] = lstEntXTerm + 1
				return
			}

			// Now safe to conclude that replication succeeeded
			rf.matchIndex[svr] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[svr] = rf.matchIndex[svr] + 1
		}(i)
	}
}

func (rf *Raft) commitEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return
	}

	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] >= N, and
	// log[N].term == currentTerm: set commitIndex = N
	majority := (len(rf.peers) + 1) / 2
	logOff := rf.lastIncludedIndex + 1
	for N := rf.commitIndex + 1; N-logOff < len(rf.log); N++ {
		if N < logOff {
			N = logOff
		}
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= N &&
				rf.log[N-logOff].Term == rf.currentTerm {
				count++
			}
			if count >= majority {
				break
			}
		}
		if count >= majority {
			rf.commitIndex = N
		}
	}
}

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex <= rf.lastApplied {
		return
	}
	logOff := rf.lastIncludedIndex + 1
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		if i < logOff {
			i = logOff
		}
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.log[i-logOff].Command,
		}
		rf.lastApplied = i
	}
}

func (rf *Raft) doLeaderOperations() {
	go rf.replicateEntries()
	time.Sleep(time.Duration(opIntv1) * time.Millisecond)
	go rf.commitEntries()
	time.Sleep(time.Duration(opIntv2) * time.Millisecond)
	rf.applyEntries()
}

//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.state == leader

	if !isLeader {
		return index, term, isLeader
	}

	index = len(rf.log) + rf.lastIncludedIndex + 1
	term = rf.currentTerm
	if len(rf.log) == 0 {
		rf.log = append(rf.log,
			LogEntry{
				Index:   rf.lastIncludedIndex + 1,
				Term:    term,
				Command: command,
			})
	} else {
		rf.log = append(rf.log,
			LogEntry{
				Index:   rf.log[len(rf.log)-1].Index + 1,
				Term:    term,
				Command: command,
			})
	}
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	rf.nextIndex[rf.me] = rf.log[len(rf.log)-1].Index + 1
	rf.persist()
	go rf.doLeaderOperations()

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.isKilled, 1)
}

func (rf *Raft) killed() bool {
	flag := atomic.LoadInt32(&rf.isKilled)
	return flag == 1
}

func (rf *Raft) ticker() {
	rf.mu.Lock()
	state := rf.state
	currIndex := rf.timeoutIndex
	rf.mu.Unlock()
	var duration int
	if state != leader {
		duration = timeoutIntv + rand.Intn(randomIntv)
	} else {
		duration = heartbeatIntv
	}
	time.Sleep(time.Duration(duration) * time.Millisecond)

	rf.mu.Lock()
	if rf.timeoutIndex != currIndex {
		rf.mu.Unlock()
		return
	}
	rf.timeout = true
	rf.cond.Signal()
	rf.mu.Unlock()
	return
}

//
// The service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it starts goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{0, 0, nil}}
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = follower
	rf.timeoutIndex = 0
	rf.timeout = false
	rf.cond = sync.NewCond(&rf.mu)
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	// Initialize from state persisted before a crash
	// if that is the case
	rf.readPersist(persister.ReadRaftState())

	go func() {
		rf.mu.Lock()
		data := persister.ReadSnapshot()
		if data != nil && len(data) > 0 {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      data,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.lastApplied = rf.lastIncludedIndex
		}
		rf.mu.Unlock()
		for rf.killed() == false {
			rf.mu.Lock()
			rf.timeoutIndex++
			rf.timeout = false
			go rf.ticker()

			rf.cond.Wait()
			if !rf.timeout {
				rf.mu.Unlock()
				continue
			}
			if rf.state != leader {
				rf.state = candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()
				rf.mu.Unlock()
				go rf.startLeaderElection()
				continue
			}
			rf.mu.Unlock()
			go rf.doLeaderOperations()
		}
	}()

	return rf
}
