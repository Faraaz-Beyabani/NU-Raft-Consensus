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
	"labgob"
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

// ApplyMsg ...
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

// Raft ...
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	heard bool

	// persistent state on all servers
	currentTerm int
	votedFor    int
	Logs        []Log
	applyCh     chan ApplyMsg

	// volatile local state
	// 0 for follower, 1 for candidate, 2 for leader
	serverStatus uint

	commitIndex int
	lastApplied int

	// volatile leader state
	nextIndex  []int
	matchIndex []int

	dead bool
}

// Log ...
type Log struct {
	Command interface{}
	Term    int
}

// RequestVoteArgs ...
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply ...
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int

	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// GetState ...
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.serverStatus == 2
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.Logs)
}

// RequestVote ...
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	rf.heard = true

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {

		if args.Term > rf.currentTerm {
			rf.serverStatus = 0
			rf.votedFor = -1
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
		}

		hasOKLog := false

		thisTerm := 0
		thisIndex := len(rf.Logs)

		if thisIndex > 0 {
			thisTerm = rf.Logs[thisIndex-1].Term
		}

		if args.LastLogTerm != thisTerm {
			if args.LastLogTerm > thisTerm {
				hasOKLog = true
			}
		} else {
			if args.LastLogIndex >= thisIndex {
				hasOKLog = true
			}
		}

		hasValidVote := false

		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			hasValidVote = true
		}

		if hasValidVote && hasOKLog {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}

	}
	rf.persist()
	rf.mu.Unlock()
}

// AppendEntries ...
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.heard = true

	if args.Term >= rf.currentTerm {
		rf.votedFor = args.LeaderID
		rf.currentTerm = args.Term
		rf.serverStatus = 0
		reply.Success = true
		reply.Term = rf.currentTerm

		if args.PrevLogIndex > 0 {
			reply.Success = false
			if len(rf.Logs) >= args.PrevLogIndex && rf.Logs[args.PrevLogIndex-1].Term == args.PrevLogTerm {
				reply.Success = true
			}
		}

	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}

	if reply.Success {
		rf.Logs = rf.Logs[0:args.PrevLogIndex]
		rf.Logs = append(rf.Logs, args.Entries...)

		for l := rf.commitIndex; l < args.LeaderCommit; l++ {
			command := rf.Logs[l].Command
			rf.applyCh <- ApplyMsg{Command: command, CommandIndex: l + 1, CommandValid: true}
		}
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.Logs))))
	}
	rf.persist()

	rf.mu.Unlock()
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

// Start ...
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
	index := len(rf.Logs) + 1
	term := rf.currentTerm
	isLeader := rf.serverStatus == 2

	if isLeader {
		log := Log{Command: command, Term: term}
		rf.Logs = append(rf.Logs, log)
		rf.persist()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// Kill ...
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.dead = true
	rf.mu.Unlock()
}

// Make ...
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

	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.serverStatus = 0
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heard = false
	rf.dead = false
	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.mu.Unlock()

	rf.readPersist(persister.ReadRaftState())

	go func() {

		for {

			rf.mu.Lock()
			if rf.serverStatus == 0 && !rf.dead {
				rf.heard = false
				rf.mu.Unlock()

				time.Sleep(getRandTimeout())

				rf.mu.Lock()
				if !rf.heard {
					rf.serverStatus = 1
					rf.votedFor = rf.me
					rf.currentTerm++
					rf.persist()
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}

			rf.mu.Lock()
			if rf.serverStatus == 1 && !rf.dead {
				var votes int64 = 1

				peers := rf.peers
				me := rf.me
				term := 0
				if len(rf.Logs) != 0 {
					term = rf.Logs[len(rf.Logs)-1].Term
				}
				rf.mu.Unlock()

				for peer := range peers {
					if peer == me {
						continue
					}
					go func(peer int, term int) {

						rf.mu.Lock()
						args := &RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me, LastLogIndex: len(rf.Logs), LastLogTerm: term}
						reply := &RequestVoteReply{}
						rf.mu.Unlock()

						ok := rf.sendRequestVote(peer, args, reply)

						rf.mu.Lock()
						if ok && reply.VoteGranted {
							atomic.AddInt64(&votes, 1)
						} else {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.serverStatus = 0
								rf.votedFor = -1
								rf.persist()
							}
						}
						rf.mu.Unlock()

						rf.mu.Lock()
						if int(votes)*2 > len(rf.peers) && rf.serverStatus == 1 {
							rf.serverStatus = 2
							rf.votedFor = -1

							for p := 0; p < len(rf.peers); p++ {
								rf.nextIndex[p] = len(rf.Logs) + 1
								rf.matchIndex[p] = 0
							}

							rf.mu.Unlock()
							go rf.heartbeats()
						} else {
							rf.mu.Unlock()
						}
					}(peer, term)
				}

				time.Sleep(getRandTimeout())

				rf.mu.Lock()
				if rf.serverStatus != 2 {
					rf.serverStatus = 0
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}

			rf.mu.Lock()
			if rf.dead {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()

	return rf
}

func (rf *Raft) heartbeats() {
	for {
		rf.mu.Lock()
		if rf.serverStatus == 2 && !rf.dead {
			peers := rf.peers
			me := rf.me
			rf.mu.Unlock()

			for peer := range peers {
				if peer == me {
					continue
				}
				go func(peer int) {

					rf.mu.Lock()
					index := rf.nextIndex[peer]
					term := 0
					if index > 1 {
						term = rf.Logs[index-2].Term
					}

					var entries []Log
					if len(rf.Logs) >= rf.nextIndex[peer] {
						entries = append(entries, rf.Logs[rf.nextIndex[peer]-1:]...)
					}

					args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, PrevLogIndex: rf.nextIndex[peer] - 1, PrevLogTerm: term, Entries: entries, LeaderCommit: rf.commitIndex}
					reply := &AppendEntriesReply{}
					rf.mu.Unlock()

					ok := rf.sendAppendEntries(peer, args, reply)

					rf.mu.Lock()
					if ok && reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.serverStatus = 0
						rf.votedFor = -1
						rf.persist()
					}
					if ok && rf.serverStatus == 2 {
						if !reply.Success {
							rf.nextIndex[peer]--
						} else if reply.Success && args.Term == rf.currentTerm {
							rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
							rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
							for l := rf.commitIndex; l < len(rf.Logs); l++ {
								consensus := 1
								for p := range rf.peers {
									if p == rf.me {
										continue
									}

									if rf.matchIndex[p] > l {
										consensus++
									}
								}

								if consensus*2 > len(rf.peers) {
									for k := rf.commitIndex; k <= l; k++ {
										rf.commitIndex = k + 1
										command := rf.Logs[k].Command
										rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: k + 1, Command: command}
									}
								} else {
									break
								}
							}
						}
					}
					rf.mu.Unlock()

				}(peer)
			}

			time.Sleep(100 * time.Millisecond)
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

func getRandTimeout() time.Duration {
	return (time.Duration(rand.Intn(350) + rand.Intn(150) + 500)) * time.Millisecond
}
