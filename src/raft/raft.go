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
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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
	LogIndex   int
	LogTerm    int
	LogCommand interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	//added by myself
	votesCount int // how many votes does this server get.
	state      int // 0 is follower | 1 is candidate | 2 is leader

	// 5.22 added
	heartBeats   chan bool // used reset timeout when received leader heartbeats
	resetTimeOut chan bool // used reset timeout when received requestvote from candidates
	chanLeader   chan bool

	// added for project 4
	commitIndex int
	lastApplied int
	nextIndex   []int         // index of next log entry to send to that server
	matchIndex  []int         // index of highest log entry known to be replicated on server
	applyChMsg  chan ApplyMsg // just for convenience
	commitChan  chan bool     // channel for commit!
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here.
	// Don't forget lock here!!!! (But why???)
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == 2
	rf.mu.Unlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (3C).
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
	// Your code here (3C).
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
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Println("error")
	} else{
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here.
	Term      int
	Success   bool
	NextIndex int // added 5.31
}

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//currentTerm, _ := rf.GetState()

	reply.VoteGranted = false

	// $5.1: reply false if candidate's term < follower's term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// $5.1: if RPC request contains term T > currentTerm;
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0
		rf.votedFor = -1
		rf.persist()
	}

	//Receiver Implementation - Point 2
	isUpdated := false
	reply.Term = rf.currentTerm

	// How to judge whether is updated????
	// candidate's log is at least up-to-date as receiver's log (ref: 5.4.1)

	// 1) if the logs have last entries with diff terms, then
	// the log with the later term is more up-to-date.
	if args.LastLogTerm > rf.log[len(rf.log)-1].LogTerm {
		isUpdated = true
	}

	// 2) if the logs end with the same term, then whichever log
	// is longer is more up-to-date
	if args.LastLogTerm == rf.log[len(rf.log)-1].LogTerm && args.LastLogIndex >= rf.log[len(rf.log)-1].LogIndex {
		isUpdated = true
	}

	// 3) if voteFor is null or is candidateId, and candidateId is update-to-date, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpdated {
		rf.state = 0
		reply.VoteGranted = true
		// this server can reset its timeout
		rf.resetTimeOut <- true
		rf.votedFor = args.CandidateId
	}

	rf.persist()

	// rf.mu.Unlock()
	return
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	// $5.1: reply false if leader's term < follower's term (reject AppendEntries)
	if args.Term < rf.currentTerm {
		//fmt.Println("is it here 1?")
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.log[len(rf.log)-1].LogIndex + 1
		return
	}

	// Added 6.8
	// Only valid AppendEntries can succeed.
	if rf.commitIndex > rf.lastApplied {
		//fmt.Println("is it here 9???")
		rf.commitChan <- true
	}
	// Ended add.......

	reply.Term = args.Term

	// All servers rule:
	// if RPC request contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		//fmt.Println("is it here 2?")
		rf.currentTerm = args.Term
		rf.state = 0
		rf.votedFor = -1
		rf.persist()
	}

	rf.heartBeats <- true

	// 5.26 Added:
	// $5.3: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.log[len(rf.log)-1].LogIndex < args.PrevLogIndex {
		//fmt.Println("is it here 3?")
		reply.Success = false
		reply.NextIndex = rf.log[len(rf.log)-1].LogIndex + 1
		return
	}

	// $ 5.3 If an existing entry conflicts with a new one (same index but with diff terms),
	// delete the existing entry and all that follow it
	if rf.log[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
		//fmt.Println("is it here 4?")
		reply.NextIndex = args.PrevLogIndex

		reply.Success = false
		return
	}


	// Append any new entries not already in the log
	rf.log = rf.log[:args.PrevLogIndex+1]
	for i := 0; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	rf.persist()

	/*fmt.Println("rf.log length => ", len(rf.log))
	fmt.Println("==============================")*/
	reply.Success = true
	reply.NextIndex = rf.log[len(rf.log)-1].LogIndex + 1

	//fmt.Println("is it here 6?")

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		//fmt.Println("is it here 5?")
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].LogIndex)
		rf.commitChan <- true
	}

	// Added 6.8
	//fmt.Println("How about these value???")
	//fmt.Println("leader's commit => ", args.LeaderCommit)
	//fmt.Println("commitIndex => ", rf.commitIndex)
	//fmt.Println("lastApplied => ", rf.lastApplied)
	if rf.commitIndex > rf.lastApplied {
		//fmt.Println("is it here 9???")
		rf.commitChan <- true
	}

	return
}

func (rf *Raft) sendHeartBeat(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	/*fmt.Println("======= leader log ======")
	for i := range rf.log {
		fmt.Println("leader.log index => ", rf.log[i].LogIndex)
		fmt.Println("leader.log term => ", rf.log[i].LogTerm)
	}*/
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != 2 {
			return ok
		}

		if args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = 0 // convert it to follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}

		if reply.Success {
			if len(args.Entries) > 0 {
				/*fmt.Println("args Entries logIndex => ", args.Entries[len(args.Entries)-1].LogIndex)
				fmt.Println("args Entries PrevIndex => ", args.PrevLogIndex)*/
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
				//rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	//rf.mu.Unlock()
	return ok
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
	// Why it would return true or false?
	// https://golang.org/pkg/net/rpc/#Client.Call

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock() // defer unlock will unlock before "return"
	if ok {
		// if it's not candidate any more, just return
		if rf.state != 1 {
			//rf.mu.Unlock()
			return ok
		}
		if args.Term != rf.currentTerm {
			//rf.mu.Unlock()
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = 0
			rf.votedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.votesCount++
			// Question: should be '>' or '>='???????????
			if rf.state == 1 && rf.votesCount > len(rf.peers)/2 {
				rf.state = 2
				//rf.currentLeader = rf.me
				rf.chanLeader <- true
				// go rf.sendLeaderHeartBeat()
			}
		}
	}
	//rf.mu.Unlock()

	// if rf.currentLeader > -1 ||

	return ok
}

/**
Leader sends heartbeats to other followers
*/
func (rf *Raft) sendLeaderHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] >= N, and log[N].term == currentTerm:
	// set commitIndex = N
	/*fmt.Println("rf.commitIndex => ", rf.commitIndex)
	fmt.Println("rf.LogIndex => ", rf.log[len(rf.log)-1].LogIndex)
	fmt.Println("rf.LogIndex => ", rf.log[0].LogIndex)
	fmt.Println("rf log length => ", len(rf.log))*/
	N := rf.commitIndex
	for n := rf.commitIndex + 1; n <= rf.log[len(rf.log)-1].LogIndex; n++ {
		num := 1
		for i := range rf.peers {
			if rf.me != i &&  rf.matchIndex[i] >= n && rf.log[n].LogTerm == rf.currentTerm {
				num++
			}
		}

		if num > len(rf.peers)/2 {
			N = n
			//fmt.Println("N is updated => ", N)
		}
	}

	//fmt.Println("rf.log[len(rf.log)-1].LogIndex => ", rf.log[len(rf.log)-1].LogIndex)
	if N > rf.commitIndex && N <= rf.log[len(rf.log)-1].LogIndex && rf.log[N].LogTerm == rf.currentTerm {
		//fmt.Println("is it here 8?")
		/*fmt.Println("lastApplied => ", rf.lastApplied)
		fmt.Println("commitIndex => ", rf.commitIndex)
		fmt.Println("N => ", N)*/
		rf.commitIndex = N
		rf.commitChan <- true
	}

	//fmt.Println("rf.commitIndex => ", rf.commitIndex)
	//fmt.Println("rf.lastApplied => ", rf.lastApplied)

	// leader sends heart beats to every followers
	for i := range rf.peers {
		if i != rf.me && rf.state == 2 {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].LogTerm
			args.Entries = nil
			for j := rf.nextIndex[i]; j <= rf.log[len(rf.log)-1].LogIndex; j++ {
				args.Entries = append(args.Entries, rf.log[j])
			}
			args.LeaderCommit = rf.commitIndex
			var reply AppendEntriesReply
			go rf.sendHeartBeat(i, args, &reply)
		}
	}
}

func (rf *Raft) election() {
	// Don't forget to use lock!
	rf.mu.Lock()

	// Send RequestVote RPCs to all other servers
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log[len(rf.log)-1].LogIndex
	args.LastLogTerm = rf.log[len(rf.log)-1].LogTerm
	rf.mu.Unlock()

	// rf.mu.Lock()
	for peer := range rf.peers {
		//fmt.Println("what does peer look like? ", peer)
		if peer != rf.me && rf.state == 1 {
			var reply RequestVoteReply
			go rf.sendRequestVote(peer, &args, &reply)
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
// The Start() call at the leader begins the process of adding a new operation to the log;
// Subsequently, the leader sends new operations to the other servers using AppendEntries RPCs.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	isLeader, index, term := false, -1, -1

	// Your code here (3B).
	term = rf.currentTerm
	isLeader = rf.state == 2
	index = rf.log[len(rf.log)-1].LogIndex + 1
	//fmt.Println("before append rf.log length => ", len(rf.log))
	//fmt.Println("rf.log length => ", rf.log[len(rf.log)-1].LogIndex)
	if isLeader {
		rf.log = append(rf.log, LogEntry{LogTerm: term, LogIndex: index, LogCommand: command})
		rf.persist()
	}
	//fmt.Println("After append rf.log length => ", len(rf.log))
	//fmt.Println("rf.log => ", rf.log[len(rf.log)-1].LogIndex, " Term => ", rf.log[len(rf.log)-1].LogTerm)
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.votedFor = -1
	rf.state = 0 // 0 is follower | 1 is candidate | 2 is leader
	rf.currentTerm = 0

	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{LogTerm: 0, LogIndex: 0})

	// Your initialization code here.
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// added 5.22
	rf.heartBeats = make(chan bool, 150)
	rf.resetTimeOut = make(chan bool, 150)
	rf.chanLeader = make(chan bool, 150)
	rf.commitChan = make(chan bool, 150)		// forget to make this variable to be an array of channel!!!

	go func() {
		for {
			// Question: make code sleep periodically?? Does that help??
			//time.Sleep(50 * time.Millisecond)
			electionTimeOut := time.Duration(time.Millisecond) * time.Duration(300+(rand.Intn(200)))

			// the only tricky part!!!!
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			switch state {
			case 0:
				select {
				case <-rf.heartBeats:
				case <-rf.resetTimeOut:
				case <-time.After(electionTimeOut):
					rf.mu.Lock()
					rf.state = 1
					rf.mu.Unlock()
				}
			case 1:
				rf.mu.Lock()
				// // See Candidates $5.2
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.votesCount = 1 // initialize vote count = 1
				rf.persist()
				rf.mu.Unlock()
				go rf.election()
				select {
				case <-time.After(electionTimeOut):
				case <-rf.heartBeats:
					rf.mu.Lock()
					rf.state = 0
					rf.mu.Unlock()
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = 2

					// Added 5.28 - initialized to leader last log index + 1
					rf.nextIndex = nil			// clear an array
					leaderLastLogIndex := rf.log[len(rf.log)-1].LogIndex
					//fmt.Println("leaderLastLogIndex => ", leaderLastLogIndex)
					for _ = range rf.peers {
						rf.nextIndex = append(rf.nextIndex, leaderLastLogIndex+1)
					}

					/*fmt.Println("nextIndex initialization .....")
					for i := range rf.nextIndex {
						fmt.Print(" ", rf.nextIndex[i])
					}
					fmt.Println()
					fmt.Println("nextIndex End .....")*/

					// Question: why do we need to set match Index=0 every time?
					rf.matchIndex = nil
					for _ = range rf.peers {
						rf.matchIndex = append(rf.matchIndex, 0)
					}
					rf.mu.Unlock()
				}
			case 2:
				rf.sendLeaderHeartBeat()
				// Question: is this correct?????
				// Due to: Do not send out heartbeat RPCs from the leader more than ten times per second.
				time.Sleep(120 * time.Millisecond)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.commitChan:
				rf.mu.Lock()
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{true, rf.log[i].LogCommand, i}
					applyCh <- msg
				}
				rf.lastApplied = rf.commitIndex
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}