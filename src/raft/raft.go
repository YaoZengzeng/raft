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

import "context"
import "math/rand"
import "sort"
import "sync"
import "sync/atomic"
import "time"
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

type Role string

const (
	Leader    Role = "leader"
	Candidate      = "candidate"
	Follower       = "follower"
)

type LogEntry struct {
	// Term when entry was received by leader.
	Term int
	// Command for state machine.
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
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leader int

	role Role

	currentTerm int
	votedFor    int

	// Log entries; each entry contains command for state machine
	// and term when entry was received by leader.
	// First index is 1.
	log []LogEntry

	// Index of highest log entry known to be commited(initialized to 0, increases monotonically).
	commitIndex int
	// Index of highest log entry applied to state machine(initialized to 0, increases monotonically).
	lastApplied int

	// Volatile states on leaders.
	// For each server, index of the next log entry to send to that server.
	// Initialized to leader last log index + 1.
	nextIndex []int
	// For each server, index of the highest log entry known to be replicated
	// on server.
	matchIndex []int

	// The timestamp of latest received valid RPC(AppendEntries from current leader or granting vote
	// to candidate).
	validRpcTimestamp time.Time

	// The rollback is used to indicate that a valid RPC has received, so the role of instance should
	// rollback to Follower.
	rollback chan struct{}

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader

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

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	// Index of log entry immediately preceding new ones.
	PrevLogIndex int
	// Term of PrevLogIndex.
	PrevLogTerm int

	// Log entries to store(empty for heartbeat; may send more than one for efficiency).
	Entries []LogEntry

	// Leader's commitIndex.
	LeaderCommit int
}

type AppendEntriesReply struct {
	// currentTerm, for leader to update itself.
	Term int
	// True if follower contained entry matching PrevLogIndex and PrevLogTerm.
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || args.PrevLogIndex > len(rf.log) || (args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		// 1. The AppendEntries is out of date.
		// 2. The PrevLogIndex is longer than Log.
		// 3. Log doesn't contain an entry at PrevLogIndex
		// whose term match PrevLogTerm.
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		reply.Term = rf.currentTerm

		if rf.role == Follower {
			rf.validRpcTimestamp = time.Now()
		} else {
			close(rf.rollback)
			rf.role = Follower
		}
		rf.currentTerm = args.Term
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		}

		if len(args.Entries) == 0 {
			// Heartbeat.
			if args.LeaderCommit > len(rf.log) {
				// commitIndex = min(leaderCommit, index of last new entry).
				rf.commitIndex = len(rf.log)
			}
		} else {
			rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
	// Index of candidate's last log entry.
	LastLogIndex int
	// Term of candidate's last log entry.
	LastLogTerm int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex, lastLogTerm := len(rf.log), 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && args.CandidateID != rf.votedFor) || args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && lastLogIndex > args.LastLogIndex) {
		// 1. The Term of RequestVote is out of date.
		// 2. The instance vote for other peer in this term.
		// 3. The log of Candidate is not the most update.
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		DPrintf("instance %d vote for %d, lastLogIndex is %d, args.LastLogIndex is %d, lastLogTerm is %d, args.LastLogTerm is %d", rf.me, args.CandidateID, lastLogIndex, args.LastLogIndex, lastLogTerm, args.LastLogTerm)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm

		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term

		if rf.role == Follower {
			rf.validRpcTimestamp = time.Now()
		} else {
			// Notify the change of the role of instance.
			close(rf.rollback)
			rf.role = Follower
		}
	}

	return
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

	if rf.role != Leader {
		return -1, -1, false
	}

	// DPrintf("Leader %d receive new command %v", rf.me, command)

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.matchIndex[rf.me] = len(rf.log)

	return len(rf.log), rf.currentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) runAsFollower(ts time.Time) {
	// Wait for election timeout.
	time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.validRpcTimestamp.Equal(ts) {
		// Received AppendEntries RPC from current leader or granting
		// vote to candidate during the elapsed time.
		return
	}

	// Otherwise convert to candidate.
	rf.role = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.rollback = make(chan struct{})
	DPrintf("instance %d follower -> candidate, currentTerm %d", rf.me, rf.currentTerm)

	return
}

func (rf *Raft) runAsCandidate(args *RequestVoteArgs) {
	var mu sync.Mutex

	cnt := 1
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				// Keep sending VoteRequest until get a reply or this term is no longer valid.
				for {
					reply := &RequestVoteReply{}
					if ok := rf.sendRequestVote(i, args, reply); ok {
						if reply.VoteGranted {
							mu.Lock()
							cnt = cnt + 1
							if cnt == (len(rf.peers)+1)/2 {
								// This instance could become Leader.
								cancel()
								rf.mu.Lock()
								if rf.role == Candidate && rf.currentTerm == args.Term {
									// If this instance is still Candidate and term has never changed.
									rf.role = Leader
									// Initialize the volatile state of Leader.
									for i := 0; i < len(rf.peers); i++ {
										if i != rf.me {
											rf.nextIndex[i] = len(rf.log) + 1
											rf.matchIndex[i] = 0
										} else {
											// Initialize the matchIndex of leader to len(rf.log).
											rf.matchIndex[i] = len(rf.log)
										}
									}
								}
								DPrintf("instance %d candidate -> leader, currentTerm %d", rf.me, rf.currentTerm)
								rf.mu.Unlock()
							}
							mu.Unlock()
						} else if reply.Term > args.Term {
							// The term of this instance may be out of date.
							// Canel the request of this vote anyway.
							cancel()
							rf.mu.Lock()
							if rf.currentTerm < reply.Term {
								// Convert back to Follower.
								DPrintf("instance %d candidate -> follower, received RequestVoteReply, currentTerm: %d, reply.Term: %d", rf.me, rf.currentTerm, reply.Term)
								rf.role = Follower
								rf.currentTerm = reply.Term
								// This instance will never vote for others in this term.
								rf.votedFor = rf.me
							}
							rf.mu.Unlock()
						}

						return
					}

					select {
					case <-ctx.Done():
						return
					default:
					}
				}
			}(i)
		}
	}

	select {
	case <-time.After(time.Duration(150+rand.Intn(150)) * time.Millisecond):
		rf.mu.Lock()
		if rf.role == Candidate && rf.currentTerm == args.Term {
			// Election timeout, maybe because of split vote, start new election.
			rf.currentTerm = rf.currentTerm + 1
			DPrintf("instance %d is candidate update to term %d because of timeout", rf.me, rf.currentTerm)
			cancel()
		}
		rf.mu.Unlock()
	case <-ctx.Done():

	case <-rf.rollback:
		cancel()
	}
}

func (rf *Raft) runAsLeader() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			// Heartbeat goroutine.
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				return
			}
			args := []*AppendEntriesArgs{}
			for i := 0; i < len(rf.peers); i++ {
				commit := rf.commitIndex
				if commit > rf.matchIndex[i] {
					// Avoid to commit log that has not been synced with Leader already.
					commit = rf.matchIndex[i]
				}
				args = append(args, &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					LeaderCommit: commit,
				})
			}
			rf.mu.Unlock()

			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(i int) {
						rf.sendAppendEntries(i, args[i], &AppendEntriesReply{})
					}(i)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Keep appending entries if necessary.
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				for {
					rf.mu.Lock()
					if rf.role != Leader {
						rf.mu.Unlock()
						return
					}
					// TODO: maybe change to condition variable.
					if len(rf.log) == 0 || (rf.matchIndex[i] != 0 && rf.nextIndex[i] == len(rf.log)+1) {
						// 1. Instance don't have log, so don't need to append.
						// 2. Log has been appended.
						rf.mu.Unlock()
						time.Sleep(10 * time.Millisecond)
						continue
					}

					prevLogIndex, prevLogTerm := rf.nextIndex[i]-1, 0
					if prevLogIndex > 0 {
						prevLogTerm = rf.log[prevLogIndex-1].Term
					}
					entries := rf.log[prevLogIndex:]
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						LeaderCommit: rf.commitIndex,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
					}
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					if ok := rf.sendAppendEntries(i, args, reply); ok {
						rf.mu.Lock()
						if reply.Success {
							rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
							rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
							DPrintf("instance %d AppendEntries to %d succeeded, matchIndex[i] is %d", rf.me, i, rf.matchIndex[i])
						} else {
							if reply.Term > rf.currentTerm {
								// Convert back to Follower.
								// DPrintf("instance %d leader -> follower, received RequestVoteReply, currentTerm: %d, reply.Term: %d", rf.me, rf.currentTerm, reply.Term)
								rf.currentTerm = reply.Term
								rf.role = Follower
								// This instance will never vote for others in this term.
								rf.votedFor = rf.me
								cancel()
							} else {
								// DPrintf("instance %d AppendEntries to %d failed because of inconsistent log", rf.me, i)
								rf.nextIndex[i] = rf.nextIndex[i] - 1
							}
						}
						rf.mu.Unlock()
					}

					select {
					case <-ctx.Done():
						return
					default:
					}

				}
			}(i)
		}
	}

	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				return
			}

			var s []int
			for i := 0; i < len(rf.matchIndex); i++ {
				s = append(s, rf.matchIndex[i])
			}
			sort.Ints(s)

			if s[len(s)/2] > rf.commitIndex {
				DPrintf("CommandIndex of leader %d has been updated to %d", rf.me, s[len(s)/2])
				rf.commitIndex = s[len(s)/2]
			}

			rf.mu.Unlock()
		}
	}()

	select {
	case <-ctx.Done():
	case <-rf.rollback:
		cancel()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.validRpcTimestamp = time.Now()
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {
			rf.mu.Lock()
			role := rf.role

			switch role {
			case Follower:
				ts := rf.validRpcTimestamp
				rf.mu.Unlock()
				rf.runAsFollower(ts)
			case Candidate:
				lastLogIndex := len(rf.log)
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: lastLogIndex,
				}
				if lastLogIndex > 0 {
					args.LastLogTerm = rf.log[lastLogIndex-1].Term
				}
				rf.mu.Unlock()
				rf.runAsCandidate(args)
			case Leader:
				rf.mu.Unlock()
				rf.runAsLeader()
			}
		}
	}()

	// Wait for commitIndex being updated.
	go func() {
		record := 0

		for {
			time.Sleep(10 * time.Millisecond)
			current := rf.commitIndex
			if current > record && len(rf.log) >= current {
				for i := record + 1; i <= current; i++ {
					applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i-1].Command,
						CommandIndex: i,
					}
				}
				record = current
			}
		}
	}()

	// Wait for commitIndex to be updated.
	go func() {
		record := 0

		for {
			time.Sleep(10 * time.Millisecond)

			var entries []LogEntry
			rf.mu.Lock()
			if rf.commitIndex > record {
				entries = rf.log[record:rf.commitIndex]
			}
			rf.mu.Unlock()

			for i := 0; i < len(entries); i++ {
				record = record + 1
				applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entries[i].Command,
					CommandIndex: record,
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
