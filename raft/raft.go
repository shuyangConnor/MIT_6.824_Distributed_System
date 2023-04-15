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
	"sync"
	"sync/atomic"

	"cs350/labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "cs350/labgob"

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

type Entry struct {
	Term  int
	Index  int
	Command  interface{}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentRole  string  //  Leader, Follower, or Candidate
	votedFor  int   // candidateId that received vote in current term (or null if none)
	currentTerm  int  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	commitIndex  int  // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied  int  // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	log []Entry  //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	nextIndex  []int  //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int  //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	applyCh chan ApplyMsg
	heartBeatTime time.Duration
	electionTime time.Time
	randomDuration time.Duration
}

func min(a, b int) int {
	if a < b {
			return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
			return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if (rf.currentRole == "Leader"){
		isleader = true
	}else{
		isleader = false
	}
	return term, isleader
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int  //candidate’s term
	CandidateId  int  //candidate requesting vote
	LastLogIndex  int  //index of candidate’s last log entry
	LastLogTerm  int  //term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term  int  // currentTerm, for candidate to update itself
	VoteGranted  bool  // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	// Your code here (2A, 2B).
	//Reply false if term < currentTerm (§5.1)
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.currentRole = "Follower"
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm{
		return
	}
	//If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	// fmt.Printf("RequestVote from %v, args.LastLogIndex: %v, args.LastLogTerm: %v, rf.log[len(rf.log)-1].Term: %v, len(rf.log): %v\n", args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.log[len(rf.log)-1].Term, len(rf.log))
	if ((rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index))){
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rand.Seed(time.Now().UnixNano())
		minDuration := time.Duration(500) * time.Millisecond
		maxDuration := time.Duration(1000) * time.Millisecond
		randomDuration := minDuration + time.Duration(rand.Int63n(int64(maxDuration-minDuration)))
		rf.electionTime = time.Now().Add(randomDuration)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("args.Term: %d, rf.currentTerm: %d\n", args.Term, rf.currentTerm)
	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm{
		rf.currentRole="Follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		return
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	rand.Seed(time.Now().UnixNano())
	minDuration := time.Duration(500) * time.Millisecond
	maxDuration := time.Duration(1000) * time.Millisecond
	randomDuration := minDuration + time.Duration(rand.Int63n(int64(maxDuration-minDuration)))
	rf.electionTime = time.Now().Add(randomDuration)

	//Reply false if term < currentTerm 
	if args.Term < rf.currentTerm{
		reply.Success = false
		return
	}

	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex != 0 && (len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm){
		reply.Success = false
		return
	}
	
	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	rollBackIndex := -1
	for i:=len(args.Entries)-1; i >=0; i--{
		if args.Entries[i].Index < len(rf.log) && rf.log[args.Entries[i].Index].Term != args.Entries[i].Term{
			rollBackIndex = args.Entries[i].Index
		}
	}
	if rollBackIndex != -1{
		// fmt.Printf("rollBackIndex: %v, REPLY: %v\n",rollBackIndex, reply.Success)
		rf.log = rf.log[:rollBackIndex]
	}
	//Append any new entries not already in the log
	lastNewEntryIndex := -1
	for i:=0; i < len(args.Entries); i++{
		if args.Entries[i].Index == len(rf.log){
			lastNewEntryIndex = args.Entries[i].Index
			rf.log = append(rf.log, args.Entries[i])
		}
	}
	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if (lastNewEntryIndex != -1){
			rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		}else{
			rf.commitIndex = args.LeaderCommit
		}
    rf.Apply()
	}
}

func (rf *Raft) leaderTryCommit(){
	for n := rf.commitIndex + 1; n < len(rf.log); n++ {
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				break
			}
		}
	}
	rf.Apply()
}

func (rf *Raft) Apply(){
	// fmt.Printf("rf.me: %d, try to apply. lastApplied: %d, commitIndex: %d, len(rf.log): %d\n", rf.me, rf.lastApplied, rf.commitIndex, len(rf.log))
	for i := rf.lastApplied + 1; i <= rf.commitIndex;i++{
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command: rf.log[i].Command,
			CommandIndex: rf.log[i].Index,
		}
		rf.applyCh <- applyMsg
		// fmt.Printf("%d applied commandIndex: %d\n", rf.me, rf.log[i].Index)
	}
}

func (rf *Raft) sendAppendEntriesThread(i int, args *AppendEntriesArgs){
	reply:=AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("%d sent to %d, len(args.Entries): %d\n", rf.me, i, len(args.Entries))
	if reply.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.currentRole = "Follower"
		rf.votedFor = -1
		return
	}
	if (reply.Success == true && len(args.Entries) > 0){
		rf.matchIndex[i] = args.Entries[len(args.Entries)-1].Index
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.leaderTryCommit()
	}else if (reply.Success == false){
		rf.nextIndex[i] = rf.nextIndex[i] - 1
		sendEntries := rf.log[rf.nextIndex[i]:]
		newArgs := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: rf.log[max(0,rf.nextIndex[i]-1)].Index,
			PrevLogTerm: rf.log[max(0,rf.nextIndex[i]-1)].Term,
			Entries: sendEntries,
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendAppendEntriesThread(i, &newArgs)
	}
}

func (rf *Raft) heartBeat(heartbeat bool){
	
	for i := 0; i < len(rf.peers); i++ {
		if (rf.me == i){
			continue
		}
		if (heartbeat || rf.log[len(rf.log)-1].Index >= rf.nextIndex[i]){
			sendEntries := rf.log[rf.nextIndex[i]:]
			prevLogIndex := 0
			prevLogTerm := 0
			if rf.nextIndex[i] != 0{
				prevLogIndex = rf.log[rf.nextIndex[i]-1].Index
				prevLogTerm = rf.log[rf.nextIndex[i]-1].Term
			}
			args:=AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				Entries: sendEntries,
				LeaderCommit: rf.commitIndex,
			}
			// if (!heartbeat){
			// fmt.Printf("args:=AppendEntriesArgs{\nTerm: %d,\nLeaderId: %d,\nPrevLogIndex: %d,\nPrevLogTerm: %d,\nlen(Entries): %d,\nLeaderCommit: %d,\n}\n", rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, len(sendEntries), rf.commitIndex)
			
			go rf.sendAppendEntriesThread(i, &args)
		}
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentRole != "Leader"{
		return -1, -1, false
	}
	index := len(rf.log)
	term := rf.currentTerm
	newEntry := Entry{
		Term: term,
		Index: index,
		Command: command,
	}
	rf.log=append(rf.log, newEntry)
	// fmt.Printf("Entry{Term: %d, Index: %d}, len(rf.log): %d, Leader: %v\n", newEntry.Term, newEntry.Index, len(rf.log), rf.me)
	rf.heartBeat(false)
	return index, term, true
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
	// fmt.Printf("%d shuts down.", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getVote(voteCount *int, args *RequestVoteArgs, i int){
	reply := RequestVoteReply{}
  result := rf.sendRequestVote(i, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (result == false){
		return
	}
	if reply.Term > rf.currentTerm{
		rf.currentRole = "Follower"
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		return
	}
	if reply.VoteGranted == true{
		*voteCount++
	}
	
	if *voteCount > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.currentRole == "Candidate"{
		rf.currentRole = "Leader"
		// fmt.Printf("%v wins the election!\n", rf.me)
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}

		rf.heartBeat(true)
	}
}

func (rf *Raft) startElection(){
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rand.Seed(time.Now().UnixNano())
	minDuration := time.Duration(500) * time.Millisecond
	maxDuration := time.Duration(1000) * time.Millisecond
	randomDuration := minDuration + time.Duration(rand.Int63n(int64(maxDuration-minDuration)))
	rf.electionTime = time.Now().Add(randomDuration)
	rf.currentTerm++
	rf.currentRole = "Candidate"
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	voteCount := 0
	for i := 0; i < len(rf.peers); i++ {
		
		
		// reply := RequestVoteReply{}
    // result := rf.sendRequestVote(i, &args, &reply)
		
		// if (result == false){
		// 	return
		// }
		// if reply.Term > rf.currentTerm{
		// 	rf.currentRole = "Follower"
		// 	rf.currentTerm = reply.Term
		// 	rf.votedFor = -1
		// 	return
		// }
		// if reply.VoteGranted == true{
		// 	voteCount++
		// }
		go rf.getVote(&voteCount, &args, i)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(rf.heartBeatTime)
		
		rf.mu.Lock()
		
		if rf.currentRole == "Leader" {
			rf.heartBeat(true)
		}else if time.Now().After(rf.electionTime) {
			// fmt.Printf("!!!!%d starts elections\n", rf.me)
			rf.startElection()
		}
		rf.mu.Unlock()
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
	rf.votedFor = -1
	rf.log = []Entry{}
	dummy := Entry{
		Term: 0,
		Index: 0,
		Command: nil,
	}
	rf.log = append(rf.log, dummy)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
    rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
    rf.matchIndex[i] = 0
	}
	rf.currentRole = "Follower"
	rand.Seed(time.Now().UnixNano())
	minDuration := time.Duration(500) * time.Millisecond
	maxDuration := time.Duration(1000) * time.Millisecond
	randomDuration := minDuration + time.Duration(rand.Int63n(int64(maxDuration-minDuration)))
	rf.randomDuration = randomDuration
	rf.heartBeatTime = 150 * time.Millisecond
	rf.electionTime = time.Now().Add(randomDuration)
	rf.applyCh = applyCh
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command: nil,
		CommandIndex: 0,
	}
	rf.applyCh <- applyMsg
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
