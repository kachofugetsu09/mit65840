package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type PeerState int

const (
	Follower PeerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state       PeerState  // 是否是leader
	currentTerm int        // 当前任期号
	votedFor    int        // 当前任期内投票给的候选人ID，-1表示没有投票
	log         []logEntry // 日志条目，包含任期号和命令

	//所有服务器都有的状态
	commitIndex int // 已知已提交的日志条目索引
	lastApplied int // 已知已应用到状态机的日志条目索引

	//leader身份下的状态
	nextIndex  []int // 每个服务器的下一条日志索引
	matchIndex []int // 每个服务器已知的已提交日志条目索引

	electionTimer  *time.Timer // 选举定时器
	heartbeatTimer *time.Timer // 心跳定时器
	lastHeartbeat  time.Time   // 上次收到心跳的时间
}

type logEntry struct {
	LogIndex int
	Term     int
	Command  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人任期号
	CandidateId  int // 候选人ID
	LastLogIndex int // 候选人最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 接收者当前任期
	VoteGranted bool // 投票授予结果
}

type AppendEntriesArgs struct {
	Term              int        // Leader的term
	LeaderId          int        // Leader的ID
	PrevLogIndex      int        // 前一个日志条目的索引
	PrevLogTerm       int        // 前一个日志条目的任期号
	Entries           []logEntry // log entries to store (empty for heartbeat)
	LeaderCommitIndex int        // Leader已知的已提交日志条目索引

}

type AppendEntriesReply struct {
	Term    int  // 接收者当前任期
	Success bool // 是否成功接收日志条目
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		DPrintf("[%d] 任期更新: %d->%d", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	rf.lastHeartbeat = time.Now()
	if rf.electionTimer != nil {
		rf.electionTimer.Reset(rf.getRandomElectionTimeout())
	}

	rf.state = Follower
	reply.Term = rf.currentTerm
	reply.Success = true

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		DPrintf("[%d] 任期更新: %d->%d", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	rf.lastHeartbeat = time.Now()

	canVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogTerm(lastLogIndex)

	logIsUpToDate := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if canVote && logIsUpToDate {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		DPrintf("[%d] 投票给[%d]", rf.me, args.CandidateId)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("节点[%d] (任期%d) 拒绝投票给节点[%d]: 已投票给%d 或日志不够新",
			rf.me, rf.currentTerm, args.CandidateId, rf.votedFor)
	}
}

func (rf *Raft) sendHeartbeats() {
	if rf.state != Leader {
		return
	}

	//DPrintf("Leader[%d] (任期%d) 开始发送心跳", rf.me, rf.currentTerm)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				args := &AppendEntriesArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					PrevLogIndex:      rf.nextIndex[server] - 1,
					PrevLogTerm:       rf.getLogTerm(rf.nextIndex[server] - 1),
					Entries:           make([]logEntry, 0),
					LeaderCommitIndex: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}

				if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						DPrintf("[%d] 发现更高任期，退位: %d->%d", rf.me, rf.currentTerm, reply.Term)
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				}
				break
			}
		}(i)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.startNewElection()
			}
			rf.electionTimer.Reset(rf.getRandomElectionTimeout())
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.sendHeartbeats()
				rf.heartbeatTimer.Reset(time.Duration(100) * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startNewElection() {
	oldTerm := rf.currentTerm
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()

	DPrintf("[%d] 开始选举: %d->%d", rf.me, oldTerm, rf.currentTerm)
	go rf.startElection()
}

// 获取最后一个日志条目的任期
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	votesReceived := 1 // 投给自己的一票
	voteCh := make(chan bool, len(rf.peers)-1)

	// 向其他所有节点发送投票请求
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			//DPrintf("[%d] 向节点[%d] 发送投票请求：任期=%d", rf.me, server, currentTerm)
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				voteCh <- rf.handleRequestVoteReply(server, currentTerm, reply)
			} else {
				DPrintf("[%d] 向节点[%d] 的投票请求失败", rf.me, server)
				voteCh <- false
			}
		}(i)
	}

	// 等待投票结果
	for i := 0; i < len(rf.peers)-1; i++ {
		if <-voteCh {
			votesReceived++
			rf.mu.Lock()
			if rf.state == Candidate && rf.currentTerm == currentTerm {
				if votesReceived >= len(rf.peers)/2+1 {
					DPrintf("[%d] 获得多数票 (%d票), 成为Leader (任期%d)", rf.me, votesReceived, rf.currentTerm)
					rf.becomeLeader()
					rf.mu.Unlock()
					return
				}
			} else {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	// 初始化leader状态
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	// 立即发送心跳
	rf.sendHeartbeats()
	// 重置心跳定时器
	rf.heartbeatTimer.Reset(time.Duration(100) * time.Millisecond)
}

func (rf *Raft) handleRequestVoteReply(server int, requestTerm int, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		DPrintf("[%d] 发现更高的任期 %d > %d, 转为Follower", rf.me, reply.Term, rf.currentTerm)
		rf.becomeFollower(reply.Term)
		return false
	}

	if rf.currentTerm != requestTerm {
		return false
	}

	return reply.VoteGranted
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	rf.electionTimer.Reset(rf.getRandomElectionTimeout())
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}

// 获取指定索引日志条目的任期
func (rf *Raft) getLogTerm(index int) int {
	if index < 0 || index >= len(rf.log) { // 检查索引边界
		return -1
	}
	return rf.log[index].Term
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]logEntry, 1)
	rf.log[0] = logEntry{LogIndex: 0, Term: 0, Command: nil}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := range peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.lastHeartbeat = time.Now()
	rf.electionTimer = time.NewTimer(rf.getRandomElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(100 * time.Millisecond)

	go rf.ticker()

	return rf
}

// 生成随机选举超时时间
func (rf *Raft) getRandomElectionTimeout() time.Duration {
	// 增加选举超时的范围，减少冲突概率
	return time.Duration(400+rand.Int63n(400)) * time.Millisecond
}
