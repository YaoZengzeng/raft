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

import "math/rand"
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

	done chan struct{}
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
}

type AppendEntriesReply struct {
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var notify bool

	rf.mu.Lock()
	if args.Term >= rf.currentTerm {
		if rf.role != Follower {
			DPrintf("Valid AppendEntries, instance %d fallback to follower\n", rf.me)
		}

		rf.currentTerm = args.Term
		rf.leader = args.LeaderID
		// Change back to follower anyway.
		rf.role = Follower

		notify = true
	} else {
		DPrintf("instance %d (currentTerm %d) reject AppendEntries: Term %d, LeaderID %d", rf.me, rf.currentTerm, args.Term, args.LeaderID)
	}
	rf.mu.Unlock()

	if notify {
		rf.done <- struct{}{}
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
	var notify bool

	rf.mu.Lock()
	if args.Term > rf.currentTerm || args.Term == rf.currentTerm && rf.votedFor == args.CandidateID {
		// If it is newer term or the RequestVote we have voted.
		if rf.role != Follower {
			// Only output log if necessary.
			DPrintf("Valid RequestVote, instance %d fallback to follower\n", rf.me)
		}
		reply.VoteGranted = true

		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		rf.role = Follower

		notify = true
	} else {
		DPrintf("instance %d (currentTerm %d) reject RequestVote: Term %d, CandidateID %d", rf.me, rf.currentTerm, args.Term, args.CandidateID)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()

	if notify {
		// If in Candidate or Leader state, notify to fallback to Follower.
		rf.done <- struct{}{}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) runAsFollower() {
	select {
	case <-time.After(time.Duration(150+rand.Intn(150)) * time.Millisecond):
		rf.role = Candidate
		rf.votedFor = rf.me

		rf.mu.Lock()
		rf.currentTerm = rf.currentTerm + 1
		rf.mu.Unlock()

		DPrintf("instance %d follower -> candidate, currentTerm %d\n", rf.me, rf.currentTerm)

	case <-rf.done:
		// Receive valid RequestVote or AppendEntries.
	}
}

func (rf *Raft) runAsCandidate() {
	var stop bool

	replyCh := make(chan *RequestVoteReply, len(rf.peers)-1)

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// Already voted for self.
			continue
		}

		go func(i int) {
			// Keep send VoteRequest until get a reply or go to next term.
			reply := &RequestVoteReply{}
			for !stop {
				if ok := rf.sendRequestVote(i, args, reply); ok {
					break
				}
			}
			replyCh <- reply
		}(i)
	}

	timeoutCh := make(chan struct{})
	go func() {
		time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)
		close(timeoutCh)
	}()

	// Vote for self.
	cnt := 1
	for {
		select {
		case <-timeoutCh:
			rf.mu.Lock()
			// It is possible we reply to the valid RequestVote and AppendEntries, but run this
			// branch first, so we only increment currentTerm, if we still in Candidate state.
			if rf.role == Candidate {
				// Election timeout, another term.
				rf.currentTerm = rf.currentTerm + 1
				DPrintf("instance %d is candidate, timeout update term to %d", rf.me, rf.currentTerm)
			}
			rf.mu.Unlock()

			stop = true
			return

		case reply := <-replyCh:
			rf.mu.Lock()
			if reply.VoteGranted {
				cnt++
				if cnt >= (len(rf.peers)+1)/2 {
					// Be elected as leader.
					DPrintf("instance %d (currentTerm %d) candidate -> leader\n", rf.me, rf.currentTerm)

					// Only become Leader if now still is Candidate.
					if rf.role == Candidate {
						rf.role = Leader
					}
					rf.mu.Unlock()

					stop = true
					return
				}
			} else if reply.Term > rf.currentTerm {
				// Someone has higher term, fallback to Follower.
				DPrintf("instance %d is candidate, but receive reply.Term = %d > currentTerm = %d, back to follower", rf.me, reply.Term, rf.currentTerm)
				rf.role = Follower
				rf.mu.Unlock()

				stop = true
				return
			}
			rf.mu.Unlock()

		case <-rf.done:
			// Receive valid RequestVote or AppendEntries, fallback to Follower.
			stop = true
			return
		}
	}
}

func (rf *Raft) runAsLeader() {
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}

	select {
	case <-time.After(100 * time.Millisecond):
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.sendAppendEntries(i, args, &AppendEntriesReply{})
			}(i)
		}

	case <-rf.done:
		// Receive valid RequestVote or AppendEntries, fallback to Follower.
		DPrintf("instance %d leader -> follower\n", rf.me)
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
	rf.done = make(chan struct{})

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		for {
			switch rf.role {
			case Follower:
				rf.runAsFollower()
			case Candidate:
				rf.runAsCandidate()
			case Leader:
				rf.runAsLeader()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
