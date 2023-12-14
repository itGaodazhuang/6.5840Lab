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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const heartbeatInterval int = 100
const electionTimeoutInterval int = 850
const DEBUG bool = true

type State uint8

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// state
	currentTerm int
	votedFor    int
	leaderId    int
	timer       time.Time
	state       State

	// log related
	log           []LogEntry
	logStartIndex int
	logStartTerm  int
	commitIndex   int
	lastApplied   int
	applyCh       chan ApplyMsg
	snapshot      []byte

	// leader state
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
//
// this is a synchronous call, if server crashes before this operation return,
// the complete operation is failed, this protects the state consistency.
// and you must lock Raft's mu before calling.
// * to advance your perfomrance call this function as little as possible.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.logStartIndex)
	e.Encode(rf.logStartTerm)
	raftstate := w.Bytes()
	snapshot := rf.snapshot
	rf.persister.Save(raftstate, snapshot)
	if DEBUG {
		log.Println("[", rf.me, "]", "persist", rf.currentTerm, rf.votedFor, rf.log, rf.logStartIndex, rf.logStartTerm)
		log.Println("[", rf.me, "]", "persist snapshot", snapshot)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm, votedFor, logStartIndex, logStartTerm int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&logStartIndex) != nil ||
		d.Decode(&logStartTerm) != nil {
		return
	} else {
		if DEBUG {
			log.Println("[", rf.me, "]", "reboot and read persist", currentTerm, votedFor, logs, logStartIndex, logStartTerm)
		}
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.logStartIndex = logStartIndex
		rf.logStartTerm = logStartTerm
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.logStartIndex {
		return
	}

	if DEBUG {
		log.Println("[", rf.me, "]", "snapshot", index)
	}

	rf.snapshot = snapshot
	// discard log
	// it must foollow the below order:
	// 1. record logStartTerm
	// 2. discard log
	// 3. change logStartIndex
	rf.logStartTerm = rf.log[index-rf.logStartIndex-1].Term
	if index >= len(rf.log)+rf.logStartIndex {
		rf.log = nil
	} else {
		rf.log = rf.log[index-rf.logStartIndex:]
	}
	rf.logStartIndex = index

	rf.persist()
}

//------------------------------data structure for RPC------------------------------

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

//------------------------------RPC handler------------------------------

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	var myLastLogIndex, myLastLogTerm int
	n := len(rf.log)
	if n == 0 {
		myLastLogIndex = rf.logStartIndex
		myLastLogTerm = rf.logStartTerm
	} else {
		myLastLogTerm = rf.log[n-1].Term
		myLastLogIndex = rf.logStartIndex + n
	}

	if args.Term > rf.currentTerm {
		// upadte term and convert to follower
		rf.currentTerm = args.Term
		rf.state = FOLLOWER

		// if candiadte's log is not up to date, refuse to vote
		if (args.LastLogTerm == myLastLogTerm && args.LastLogIndex < myLastLogIndex) || args.LastLogTerm < myLastLogTerm {
			reply.VoteGranted = false
			if DEBUG {
				log.Println("[", rf.me, "]", "log more update than", "[", args.CandidateId, "]")
			}
			return
		}

		// decide to vote
		// reset timer
		rf.timer = time.Now()
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		if DEBUG {
			log.Println("[", rf.me, "]", "vote for:", "[", args.CandidateId, "]")
		}

		rf.persist()
	} else {
		reply.VoteGranted = false
		if DEBUG {
			log.Println("[", rf.me, "]", "refuse to vote for:", "[", args.CandidateId, "]")
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		if DEBUG {
			log.Println("[", rf.me, "]", "reject appendEntries from", "[", args.LeaderId, "]", "at term:", rf.currentTerm, "his term", args.Term)
		}
		return
	}

	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	rf.timer = time.Now()
	rf.leaderId = args.LeaderId

	var myLastLogIndex int
	n := len(rf.log)
	if n == 0 {
		myLastLogIndex = rf.logStartIndex
	} else {
		myLastLogIndex = rf.logStartIndex + n
	}

	// args.PrevLogIndex must >= rf.logStartIndex
	// if follower's log is not sonsistency with leader's appendlog, refuse to append
	if (args.PrevLogIndex < rf.logStartIndex) ||
		(args.PrevLogIndex != rf.logStartIndex &&
			(args.PrevLogIndex > myLastLogIndex ||
				rf.log[args.PrevLogIndex-rf.logStartIndex-1].Term != args.PrevLogTerm)) {

		if DEBUG {
			log.Println("[", rf.me, "]", "reject appendEntries from", "[", args.LeaderId, "]", "at term:", rf.currentTerm, "beacuse of log inconsistency")
		}
		return
	}

	// consistency check passed
	reply.Success = true

	if len(args.Entries) != 0 {
		rf.log = append(rf.log[:args.PrevLogIndex-rf.logStartIndex], args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)+rf.logStartIndex)
		if DEBUG {
			log.Println("[", rf.me, "]", "commit log", rf.commitIndex)
		}
		go rf.commitLog()
	}
	rf.persist()

	if DEBUG {
		log.Println("[", rf.me, "]", "accept appendEntries from", "[", args.LeaderId, "]", "at term:", rf.currentTerm)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.logStartIndex {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.snapshot = args.Data
	// discard log
	// it must foollow the below order:
	// 1. discard rf.log
	// 2. change rf.logStartIndex
	if args.LastIncludedIndex >= len(rf.log)+rf.logStartIndex {
		rf.log = nil
	} else {
		rf.log = rf.log[args.LastIncludedIndex-rf.logStartIndex:]
	}
	rf.logStartIndex = args.LastIncludedIndex
	rf.logStartTerm = args.LastIncludedTerm

	rf.commitIndex = max(rf.commitIndex, rf.logStartIndex)

	if DEBUG {
		log.Println("[", rf.me, "]", "receive InstallSnapshot from", "[", args.LeaderId, "]", "at term:", rf.currentTerm)
		log.Println("[", rf.me, "]", "logStartIndex", rf.logStartIndex, "logStartTerm", rf.logStartTerm, "commitIndex", rf.commitIndex)
	}
	go rf.commitLog()
	rf.persist()
}

// ------------------------------helper functions------------------------------
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// ------------------------------main flow------------------------------

// when you decide to start a leader election process, you should keep the state consistency between
// the time you decide to start and the time before you send RPC.
// so you should lock the state before you start the election process

// =====================consistency promise=================
func (rf *Raft) leaderElection() {
	// convert to candidate
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.timer = time.Now()
	term := rf.currentTerm

	var lastLogIndex, lastLogTerm int
	n := len(rf.log)
	if n == 0 {
		lastLogIndex = rf.logStartIndex
		lastLogTerm = rf.logStartTerm
	} else {
		lastLogTerm = rf.log[n-1].Term
		lastLogIndex = rf.logStartIndex + n
	}
	// ================consistency promise==================
	rf.mu.Unlock()

	if DEBUG {
		log.Println("[", rf.me, "]", "start to election at term", rf.currentTerm)
	}

	requestVoteArgs := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}

	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	// election variables
	cntVotes := 1
	finished := 1

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var requestVoteReply RequestVoteReply
				if rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply) {
					// if the term is outdated, convert to follower
					if DEBUG {
						log.Println("[", rf.me, "]", "send requestVote to", "[", server, "]", "at term:", term)
					}
					mu.Lock()
					defer mu.Unlock()
					if requestVoteReply.Term > term {
						rf.currentTerm = requestVoteReply.Term
						rf.state = FOLLOWER
					} else if requestVoteReply.VoteGranted {
						cntVotes += 1
					}
					// this rpc process is finished
					finished += 1
					cond.Broadcast()
				} else {
					go rf.continueRequestVote(server, requestVoteArgs)
				}
			}(i)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for !rf.killed() && rf.state == CANDIDATE && cntVotes <= len(rf.peers)/2 && finished < len(rf.peers) {
		cond.Wait()
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if condition satisfied and still candidate, convert to leader
	if rf.state == CANDIDATE && cntVotes > len(rf.peers)/2 {
		rf.state = LEADER
		rf.leaderId = rf.me

		// initialize nextIndex and matchIndex
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.log) + 1 + rf.logStartIndex
			rf.matchIndex[i] = rf.logStartIndex
		}

		go rf.heartBeat()

		if DEBUG {
			log.Println("<", rf.me, ">", "becomes leader at term", rf.currentTerm)
		}
	} else {
		if DEBUG {
			log.Println("[", rf.me, "]", "election failed at term", term)
		}
	}

	rf.persist()
}

func (rf *Raft) continueRequestVote(server int, requestVoteArgs RequestVoteArgs) {
	var Reply RequestVoteReply
	ok := false
	for !ok && !rf.killed() {
		ok = rf.sendRequestVote(server, &requestVoteArgs, &Reply)
	}
}

func (rf *Raft) heartBeat() {
	for !rf.killed() {
		// =====================consistency promise=================
		rf.mu.Lock()
		if rf.state == LEADER {
			// commit log related
			var mu sync.Mutex
			cond := sync.NewCond(&mu)

			finished := 1
			cntSuccess := 1

			// the tail log index to be appended
			appendLogIndex := len(rf.log) + rf.logStartIndex
			// keep a batch of heartbeats term same
			term := rf.currentTerm
			commitIndex := rf.commitIndex

			logStartIndex := rf.logStartIndex
			logStartTerm := rf.logStartTerm
			snapshot := rf.snapshot

			appendLog := make([][]LogEntry, len(rf.peers))
			prevLogIndex := make([]int, len(rf.peers))
			prevLogTerm := make([]int, len(rf.peers))

			// rf.log may be changed beacuse of snapshot
			// therefore, we should calculate the appendLog for each server
			for i := range rf.peers {
				if i != rf.me && rf.nextIndex[i] > rf.logStartIndex {
					appendLog[i] = append(appendLog[i], rf.log[rf.nextIndex[i]-rf.logStartIndex-1:appendLogIndex-rf.logStartIndex]...)
					prevLogIndex[i] = rf.nextIndex[i] - 1
					if prevLogIndex[i] == 0 {
						prevLogTerm[i] = 0
					} else if prevLogIndex[i] <= rf.logStartIndex {
						prevLogTerm[i] = rf.logStartTerm
					} else {
						prevLogTerm[i] = rf.log[prevLogIndex[i]-1-rf.logStartIndex].Term
					}
				}
			}

			// ================consistency promise==================
			rf.mu.Unlock()

			//send hartbeat to other peers
			for i := range rf.peers {
				if i != rf.me {
					go func(server int) {
						if rf.nextIndex[server] <= logStartIndex { // send InstallSnapshot RPC
							installSnapshotArgs := InstallSnapshotArgs{
								Term:              term,
								LeaderId:          rf.me,
								LastIncludedIndex: logStartIndex,
								LastIncludedTerm:  logStartTerm,
								Data:              snapshot,
								Done:              true,
							}

							var installSnapshotReply InstallSnapshotReply

							if DEBUG {
								log.Println("<", rf.me, ">", "send installSnapshot to", "[", server, "]", "at term:", term)
							}

							if rf.sendInstallSnapshot(server, &installSnapshotArgs, &installSnapshotReply) {
								if DEBUG {
									log.Println("<", rf.me, ">", "receive reply from", "[", server, "]", "at term:", term, "with info: ", installSnapshotReply)
								}

								if term == rf.currentTerm {
									rf.mu.Lock()

									if installSnapshotReply.Term > term {
										rf.currentTerm = installSnapshotReply.Term
										rf.state = FOLLOWER
										rf.persist()
									} else {
										rf.nextIndex[server] = logStartIndex + 1
									}

									rf.mu.Unlock()
								}
							}
						} else { // send appendEntries
							// all thses info are calculated by the above info
							appendEntriesArgs := AppendEntriesArgs{
								Term:         term,
								LeaderId:     rf.me,
								PrevLogIndex: prevLogIndex[server],
								PrevLogTerm:  prevLogTerm[server],
								Entries:      appendLog[server],
								LeaderCommit: commitIndex,
							}

							var appendEntriesReply AppendEntriesReply

							if DEBUG {
								log.Println("<", rf.me, ">", "send appendEntries to", "[", server, "]", "at term:", term)
							}

							if rf.sendAppendEntries(server, &appendEntriesArgs, &appendEntriesReply) {
								mu.Lock()
								defer mu.Unlock()

								rf.mu.Lock()
								defer rf.mu.Unlock()

								if DEBUG {
									log.Println("<", rf.me, ">", "send appendEntries to", "[", server, "]", "at term:", term, "sucessfully!", "with info: ", appendEntriesArgs)
									log.Println("<", rf.me, ">", "receive reply from", "[", server, "]", "at term:", term, "with info: ", appendEntriesReply)
								}

								// if the term is changed, exit
								if term == rf.currentTerm {
									if appendEntriesReply.Term > term {
										rf.currentTerm = appendEntriesReply.Term
										rf.state = FOLLOWER
										rf.persist()
									} else if appendEntriesReply.Success {
										rf.matchIndex[server] = appendLogIndex
										rf.nextIndex[server] = appendLogIndex + 1
										cntSuccess += 1
									} else {
										rf.nextIndex[server] = rf.searchFirstLogIndex(prevLogIndex[server])
									}
									finished += 1
									cond.Broadcast()
								}
							}
						}
					}(i)
				}
			}

			// only commit log which is current term
			// avoiding situation of figure 8
			if term == rf.currentTerm &&
				appendLogIndex > commitIndex &&
				appendLogIndex > logStartIndex &&
				rf.log[appendLogIndex-logStartIndex-1].Term == rf.currentTerm {
				go func() {
					mu.Lock()
					defer mu.Unlock()
					for !rf.killed() && rf.state == LEADER && cntSuccess <= len(rf.peers)/2 && finished < len(rf.peers) {
						cond.Wait()
					}
					// update commitIndex
					if cntSuccess > len(rf.peers)/2 {
						rf.mu.Lock()
						rf.commitIndex = appendLogIndex
						if DEBUG {
							log.Println("<", rf.me, ">", "commit log", rf.commitIndex)
						}
						rf.mu.Unlock()
						go rf.commitLog()
					}
				}()
			}
		} else {
			rf.mu.Unlock()
			return
		}
		// sleep heartbeat interval then send next heartbeat
		time.Sleep(time.Duration(heartbeatInterval) * time.Millisecond)
	}
}

// 2C
// search the first log whose term is rf.log[index - rf.startLogIndex - 1].term
func (rf *Raft) searchFirstLogIndex(index int) int {
	if index == 0 || index > len(rf.log)+rf.logStartIndex {
		return 1
	}
	if index <= rf.logStartIndex {
		return rf.logStartIndex
	}

	term := rf.log[index-rf.logStartIndex-1].Term
	left, right := 0, index-rf.logStartIndex-1

	for left <= right {
		mid := left + ((right - left) >> 1)
		if rf.log[mid].Term >= term {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	// return index
	return left + rf.logStartIndex + 1
}

// ------------------------------client command------------------------------
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log) + 1 + rf.logStartIndex
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader && !rf.killed() {
		appendLog := LogEntry{command, term}
		rf.log = append(rf.log, appendLog)
		rf.persist()
		if DEBUG {
			log.Println("<", rf.me, ">", "been client append log", appendLog)
		}
	}

	// Your code here (2B).

	return index, term, isLeader
}

// ------------------------------log commit------------------------------
func (rf *Raft) commitLog() {
	for rf.commitIndex > rf.lastApplied {
		rf.mu.Lock()
		if rf.lastApplied < rf.logStartIndex { //commit snapshot
			rf.lastApplied = rf.logStartIndex
			msg := ApplyMsg{false, 0, 0, true, rf.snapshot, rf.logStartTerm, rf.logStartIndex}
			rf.mu.Unlock()

			rf.applyCh <- msg
		} else if rf.commitIndex > rf.lastApplied { //commit log
			// because commitLog is concurrent routine, there may two goroutines
			// come into loop structure at the same state (for rf.co... > rf.las...),
			// so we check it again to avoid executing the same log twice
			rf.lastApplied++
			applyLog := rf.log[rf.lastApplied-1-rf.logStartIndex]
			msg := ApplyMsg{true, applyLog.Command, rf.lastApplied, false, nil, 0, 0}
			if DEBUG {
				log.Println("[", rf.me, "]", "commit log", applyLog, "log index: ", rf.lastApplied)
			}
			rf.mu.Unlock()

			rf.applyCh <- msg
		}

	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ------------------------------ticker------------------------------
func (rf *Raft) ticker() {
	for !rf.killed() {
		timeStamp := rf.timer

		// pause for a random amount of time between 850 and 1000 ms
		ms := int64(electionTimeoutInterval) + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// check if a new election should be started
		rf.mu.Lock()
		if timeStamp == rf.timer && rf.state != LEADER {
			go rf.leaderElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logStartIndex = 0
	rf.logStartTerm = 0

	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.timer = time.Now()
	go rf.ticker()

	return rf
}
