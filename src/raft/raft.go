package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command any) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
}

type ServerRole int

const (
	Follower ServerRole = iota
	Candidate
	Leader
)

type LogEntry struct {
	// Timestamp time.Time
	Term int
	Data any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	logger    *slog.Logger

	role ServerRole // follower, leader or candidate

	// --- Persistent state on all servers (Updated on stable storage before responding to RPCs) ---
	// latest term server has seen (initialized to 0 on boot, increases monotonically)
	currentTerm    int
	lastHeartbeat  time.Time
	currentTimeout time.Duration // when there was no heartbeat this long, start election

	// candidateId that received vote in current term (or -1/0 if none)
	votedFor int
	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	// TODO: check what type this is / make new LogEntry type
	log []LogEntry

	// maybe remove them and just look it up in the log
	lastLogIndex int // index of candidate’s last log entry (§5.4)
	lastLogTerm  int // term of candidate’s last log entry (§5.4)

	// --- Volatile state on all servers ---
	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	// --- Volatile state on leaders (Reinitialized after election) ---
	// for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func randomTimeout() time.Duration {
	min := 1000
	max := 1500
	rNum := rand.Intn(max-min) + min
	return time.Duration(rNum * int(time.Millisecond))
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeat = time.Now()
	rf.currentTimeout = randomTimeout()

	if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if rf.votedFor < 0 {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	rf.logger.Debug("RequestVoteHandler", "args", args, "reply: ", reply)
}

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

type AppendEntriesArgs struct {
	Term         int // leader’s term
	LeaderId     int // so follower can redirect clients prevLogIndex
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries      []LogEntry
	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Debug("AppenEntriesHandler", "Server", rf.me, "args", args)
	rf.lastHeartbeat = time.Now()
	rf.currentTimeout = randomTimeout()

	if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.logger.Debug("Sending Append Entries", "Args: ", args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) spin() {
	for {
		if rf.killed() {
			return
		}
		if rf.role == Leader {
			go rf.sendHeartbeats()
		} else {
			if time.Since(rf.lastHeartbeat) > rf.currentTimeout {
				rf.newElection()
			}
		}

		time.Sleep(300 * time.Millisecond) // Heartbeat Timeout
	}
}

func (rf *Raft) newElection() {
	rf.mu.Lock()
	rf.logger.Debug("Election Started", "timeout", rf.currentTimeout.Milliseconds())
	rf.role = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	electionTerm := rf.currentTerm
	rf.mu.Unlock()

	votes := 1
	var votesLock sync.Mutex

	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.lastLogTerm,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			voteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &voteArgs, &voteReply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if electionTerm != rf.currentTerm || rf.role != Candidate || rf.killed() {
				return
			}
			if voteReply.Term > rf.currentTerm {
				rf.currentTerm = voteReply.Term
				rf.role = Follower
				rf.votedFor = -1
				return
			}
			if voteReply.VoteGranted == true {
				votesLock.Lock()
				defer votesLock.Unlock()
				votes += 1
				if votes > len(rf.peers)/2 {
					rf.logger.Debug("Election successful")
					rf.role = Leader
				}
			}

		}(i)
	}
}

func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		appendArgs := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		appendReply := AppendEntriesReply{}
		rf.sendAppendEntries(i, &appendArgs, &appendReply)
		if appendReply.Term > rf.currentTerm {
			rf.role = Follower
			return
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
	rf.currentTimeout = randomTimeout()
	rf.lastHeartbeat = time.Now()
	rf.role = Follower
	// the rest is correctly initialized to 0

	go rf.spin()

	// Your initialization code here (2A, 2B, 2C).
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				t := a.Value.Time()
				return slog.String(a.Key, t.Format("15:04:05.000"))
			}
			return a
		},
	}).WithAttrs([]slog.Attr{
		slog.Int("node", rf.me),
	})
	rf.logger = slog.New(handler)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
