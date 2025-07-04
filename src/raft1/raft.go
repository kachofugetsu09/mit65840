package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"6.5840/labgob"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "6.5840/labgob"
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

	applyCh chan raftapi.ApplyMsg // 用于发送已提交的日志条目
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

	//保存Log currentTerm votedFor
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)

	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var log []logEntry
	var currentTerm int
	var votedFor int
	if d.Decode(&log) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil {
		DPrintf("节点[%d] 恢复持久化状态失败", rf.me)
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		DPrintf("节点[%d] 恢复持久化状态成功: term=%d, votedFor=%d, log长度=%d",
			rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	}
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

	// 快速回退优化字段
	XTerm  int // 这个是Follower中与Leader冲突的Log对应的任期号。如果Follower在对应位置没有Log，那么这里会返回 -1。
	XIndex int // 这个是Follower中，对应任期号为XTerm的第一条Log条目的槽位号。
	XLen   int // 这个是Follower中Log的长度。如果Follower没有任何Log，那么这里会返回 0。
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//1.Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("[%d] 任期更新: %d->%d", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist() // 持久化状态变化
	}

	rf.lastHeartbeat = time.Now()
	if rf.electionTimer != nil {
		rf.electionTimer.Reset(rf.getRandomElectionTimeout())
	}

	rf.state = Follower
	reply.Term = rf.currentTerm

	//2.reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// 统一检查所有 PrevLogIndex，包括 0
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		// 快速回退：日志太短的情况
		reply.XLen = len(rf.log)
		reply.XIndex = -1
		reply.XTerm = -1
		DPrintf("[%d] AppendEntries失败: prevLogIndex %d 越界，当前日志长度 %d, Leader=%d, 设置XLen=%d",
			rf.me, args.PrevLogIndex, len(rf.log), args.LeaderId, reply.XLen)
		return
	}

	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Success = false
		// 快速回退：任期不匹配的情况
		conflictTerm := rf.log[args.PrevLogIndex].Term
		reply.XTerm = conflictTerm

		// 找到XTerm任期的第一个条目索引
		reply.XIndex = args.PrevLogIndex
		for reply.XIndex > 0 && rf.log[reply.XIndex-1].Term == conflictTerm {
			reply.XIndex--
		}
		reply.XLen = len(rf.log)

		DPrintf("[%d] AppendEntries失败: prevLogTerm %d 不匹配, 当前日志[%d]任期 %d, Leader=%d, 设置XTerm=%d, XIndex=%d, XLen=%d",
			rf.me, args.PrevLogTerm, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.LeaderId,
			reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	//3.If an existing entry conflicts with a new one (same index but different term), delete the existing entry and all that follow it
	//4.Append any new entries not already in the log
	if len(args.Entries) > 0 {
		insertIndex := args.PrevLogIndex + 1 // 新条目开始插入的位置

		DPrintf("[%d] 收到来自Leader[%d]的%d个日志条目，插入位置=%d，当前日志长度=%d",
			rf.me, args.LeaderId, len(args.Entries), insertIndex, len(rf.log))

		// 找到第一个冲突的位置
		conflictIndex := -1
		for i, newEntry := range args.Entries {
			currentIndex := insertIndex + i
			if currentIndex < len(rf.log) {
				if rf.log[currentIndex].Term != newEntry.Term {
					conflictIndex = currentIndex
					DPrintf("[%d] 发现冲突在索引 %d: 我的任期=%d, Leader的任期=%d",
						rf.me, currentIndex, rf.log[currentIndex].Term, newEntry.Term)
					break
				}
			} else {
				// 超出当前日志长度，从这里开始追加
				conflictIndex = currentIndex
				break
			}
		}

		// 如果发现冲突或需要扩展日志
		if conflictIndex != -1 {
			// 截断从冲突位置开始的所有日志
			if conflictIndex < len(rf.log) {
				DPrintf("[%d] 截断日志从索引 %d 到 %d", rf.me, conflictIndex, len(rf.log)-1)
				rf.log = rf.log[:conflictIndex]

				// 如果截断的位置影响了已应用的日志，需要调整lastApplied和commitIndex
				if rf.lastApplied >= conflictIndex {
					oldLastApplied := rf.lastApplied
					rf.lastApplied = conflictIndex - 1
					DPrintf("[%d] 由于日志截断，调整lastApplied: %d->%d", rf.me, oldLastApplied, rf.lastApplied)
				}
				if rf.commitIndex >= conflictIndex {
					oldCommitIndex := rf.commitIndex
					rf.commitIndex = conflictIndex - 1
					DPrintf("[%d] 由于日志截断，调整commitIndex: %d->%d", rf.me, oldCommitIndex, rf.commitIndex)
				}
			}

			// 追加从冲突位置开始的所有新条目
			startAppendIdx := conflictIndex - insertIndex
			for i := startAppendIdx; i < len(args.Entries); i++ {
				rf.log = append(rf.log, args.Entries[i])
				DPrintf("[%d] 追加新日志条目: index=%d, term=%d, command=%v",
					rf.me, args.Entries[i].LogIndex, args.Entries[i].Term, args.Entries[i].Command)
			}
			rf.persist() // 持久化日志变化
		} else {
			// 没有冲突，但仍需检查是否有多余的日志需要截断
			lastNewEntryIndex := insertIndex + len(args.Entries) - 1
			if len(rf.log) > lastNewEntryIndex+1 {
				DPrintf("[%d] 截断多余日志从索引 %d 到 %d", rf.me, lastNewEntryIndex+1, len(rf.log)-1)
				rf.log = rf.log[:lastNewEntryIndex+1]
				rf.persist() // 持久化日志截断
			}
		}
	} else {
		// 即使没有新条目，也要检查是否需要截断多余的日志
		// 这是心跳消息的情况，确保日志不会超过PrevLogIndex
		if len(rf.log) > args.PrevLogIndex+1 {
			DPrintf("[%d] 心跳消息截断多余日志从索引 %d 到 %d", rf.me, args.PrevLogIndex+1, len(rf.log)-1)
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.persist() // 持久化日志截断
		}
	}

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommitIndex > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		// 计算这次AppendEntries后的最后一个日志条目索引
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommitIndex, lastNewEntryIndex)
		DPrintf("[%d] Follower 更新 commitIndex: %d->%d (LeaderCommit=%d, lastNewEntryIndex=%d)",
			rf.me, oldCommitIndex, rf.commitIndex, args.LeaderCommitIndex, lastNewEntryIndex)
	}

	reply.Success = true
	// 成功时也初始化快速回退字段
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = len(rf.log)

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果候选人的任期小于当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("[%d] 任期更新: %d->%d", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist() // 持久化状态变化
	}

	reply.Term = rf.currentTerm

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogTerm(lastLogIndex)

	// 选举限制：候选人的日志必须至少和投票者一样新
	logIsUpToDate := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if canVote && logIsUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// 重置选举定时器，因为我们投了票
		rf.electionTimer.Reset(rf.getRandomElectionTimeout())
		rf.persist() // 持久化投票状态
		DPrintf("[%d] 投票给[%d]", rf.me, args.CandidateId)
	} else {
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
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			// 检查是否有新的日志条目需要发送
			prevLogIndex := rf.nextIndex[server] - 1
			var entries []logEntry

			if rf.nextIndex[server] <= rf.getLastLogIndex() {
				// 有需要复制的日志条目，发送包含日志条目的 AppendEntries
				startArrayIndex := rf.nextIndex[server]
				if startArrayIndex < len(rf.log) {
					entries = make([]logEntry, len(rf.log)-startArrayIndex)
					copy(entries, rf.log[startArrayIndex:])
				} else {
					entries = make([]logEntry, 0)
				}
			} else {
				// 没有新的日志条目，发送空的心跳
				entries = make([]logEntry, 0)
			}

			args := &AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      prevLogIndex,
				PrevLogTerm:       rf.getLogTerm(prevLogIndex),
				Entries:           entries,
				LeaderCommitIndex: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}

			if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
				rf.mu.Lock()
				// 检查是否仍然是相同任期的 Leader
				if rf.state != Leader || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					DPrintf("[%d] 心跳中发现更高任期，退位: %d->%d", rf.me, rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.electionTimer.Reset(rf.getRandomElectionTimeout())
					rf.persist() // 持久化状态变化
				} else if reply.Success {
					// 成功，更新 nextIndex 和 matchIndex
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					if len(args.Entries) > 0 {
						DPrintf("[%d] 心跳中成功复制到节点[%d]: nextIndex=%d, matchIndex=%d",
							rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
						// 检查是否可以提交新的日志条目
						rf.updateCommitIndex()
					}
				} else {
					// 失败，使用快速回退算法
					if reply.Term <= rf.currentTerm {
						oldNextIndex := rf.nextIndex[server]
						rf.nextIndex[server] = rf.optimizeNextIndex(server, reply)
						DPrintf("[%d] 心跳失败，快速回退 nextIndex[%d]: %d->%d (XTerm=%d, XIndex=%d, XLen=%d)",
							rf.me, server, oldNextIndex, rf.nextIndex[server], reply.XTerm, reply.XIndex, reply.XLen)
					}
				}
				rf.mu.Unlock()
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

// 这是一个Leader节点调用的函数，用于提交新的命令到日志中
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果不是 Leader，直接返回
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// 记录当前任期，防止在操作过程中任期变化
	currentTerm := rf.currentTerm

	//获得新的要添加的日志的索引位置
	newLogIndex := len(rf.log) // 使用数组长度作为新的日志索引
	//创建一个新的Entry
	newEntry := logEntry{
		LogIndex: newLogIndex,
		Term:     currentTerm,
		Command:  command,
	}

	//把新日志添加到自己的日志数组当中
	rf.log = append(rf.log, newEntry)
	rf.persist() // 持久化新日志

	// 再次检查Leader身份和任期，如果发生变化则回滚
	if rf.state != Leader || rf.currentTerm != currentTerm {
		// 回滚刚才添加的日志条目
		rf.log = rf.log[:len(rf.log)-1]
		rf.persist() // 持久化回滚
		DPrintf("[%d] Start函数执行期间失去Leader身份，回滚日志条目", rf.me)
		return -1, rf.currentTerm, false
	}

	DPrintf("[%d] Leader 添加新日志条目: index=%d, term=%d, command=%v", rf.me, newLogIndex, currentTerm, command)

	// 立即开始复制到所有followers
	go rf.replicateLogEntries()

	return newLogIndex, currentTerm, true
}

// 复制日志条目到所有 followers
func (rf *Raft) replicateLogEntries() {
	//检查自己是不是Leader，如果不是就返回
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	//遍历所有节点，发送 AppendEntries RPC
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesToPeer(i)
	}
}

// 向特定节点发送 AppendEntries
func (rf *Raft) sendAppendEntriesToPeer(server int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		// 准备要发送的日志条目，nextIndex里存储了每个节点的下一个日志索引
		prevLogIndex := rf.nextIndex[server] - 1
		var entries []logEntry

		if rf.nextIndex[server] <= rf.getLastLogIndex() {
			// 有需要复制的日志条目
			startArrayIndex := rf.nextIndex[server] // nextIndex本身就是要发送的第一个条目的逻辑索引，也等于数组索引
			if startArrayIndex < len(rf.log) {
				entries = make([]logEntry, len(rf.log)-startArrayIndex)
				copy(entries, rf.log[startArrayIndex:])
			} else {
				entries = make([]logEntry, 0)
			}
		} else {
			// 没有新的日志条目，发送空的 AppendEntries（心跳）
			entries = make([]logEntry, 0)
		}
		//构建 AppendEntries RPC 的参数
		args := &AppendEntriesArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			PrevLogIndex:      prevLogIndex,
			PrevLogTerm:       rf.getLogTerm(prevLogIndex),
			Entries:           entries,
			LeaderCommitIndex: rf.commitIndex,
		}
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
			rf.mu.Lock()
			// 检查任期和Leader状态
			if rf.state != Leader || rf.currentTerm != currentTerm {
				rf.mu.Unlock()
				return
			}

			if reply.Success {
				// 成功复制，更新 nextIndex 和 matchIndex
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				DPrintf("[%d] 成功复制到节点[%d]: nextIndex=%d, matchIndex=%d",
					rf.me, server, rf.nextIndex[server], rf.matchIndex[server])

				// 检查是否可以提交新的日志条目
				rf.updateCommitIndex()

				rf.mu.Unlock()
				return
			} else {
				// 失败，可能是日志不一致
				if reply.Term > rf.currentTerm {
					// 发现更高任期，立即退位
					DPrintf("[%d] 发现更高任期，退位: %d->%d", rf.me, rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					// 立即重置选举定时器
					rf.electionTimer.Reset(rf.getRandomElectionTimeout())
					rf.mu.Unlock()
					return
				} else {
					// 日志不一致，使用快速回退算法
					oldNextIndex := rf.nextIndex[server]
					rf.nextIndex[server] = rf.optimizeNextIndex(server, reply)
					DPrintf("[%d] 快速回退 nextIndex[%d]: %d->%d (XTerm=%d, XIndex=%d, XLen=%d)",
						rf.me, server, oldNextIndex, rf.nextIndex[server], reply.XTerm, reply.XIndex, reply.XLen)
				}
			}
			rf.mu.Unlock()
		} else {
			// RPC 调用失败，直接返回
			DPrintf("[%d] 向节点[%d] 发送AppendEntries RPC失败", rf.me, server)
			return
		}
	}
}

// 更新 commitIndex：检查是否有新的日志条目可以提交
func (rf *Raft) updateCommitIndex() {
	if rf.state != Leader {
		return
	}

	// 从最后一个日志条目开始向前检查
	for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
		// 检查索引 N 处的日志条目是否可以提交
		if N < len(rf.log) && rf.log[N].Term == rf.currentTerm { // 只能提交当前任期的日志条目
			count := 1 // Leader 自己算一票

			// 统计有多少个节点已经复制了这个日志条目
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}

			// 如果大多数节点都复制了，就可以提交
			if count >= len(rf.peers)/2+1 {
				DPrintf("[%d] 提交日志条目到索引 %d (任期 %d), 获得 %d/%d 票支持, matchIndex=%v",
					rf.me, N, rf.log[N].Term, count, len(rf.peers), rf.matchIndex)
				rf.commitIndex = N
				return
			} else {
				DPrintf("[%d] 索引 %d 未获得足够支持: %d/%d 票, matchIndex=%v",
					rf.me, N, count, len(rf.peers), rf.matchIndex)
			}
		}
	}
}

// 辅助函数：返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
	rf.persist() // 持久化选举状态

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
	// Leader 自己的 matchIndex 应该是最后一个日志条目的索引
	rf.matchIndex[rf.me] = rf.getLastLogIndex()

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
		rf.persist() // 持久化状态变化
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
	rf.persist() // 持久化状态变化
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// 获取指定索引日志条目的任期
func (rf *Raft) getLogTerm(index int) int {
	if index < 0 {
		return -1
	}
	if index == 0 {
		return 0 // 索引0的日志条目任期为0
	}
	if index >= len(rf.log) { // 检查索引边界
		DPrintf("[%d] getLogTerm: 索引 %d 越界，日志长度 %d", rf.me, index, len(rf.log))
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
	rf.applyCh = applyCh

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

	// 初始化时恢复持久化状态
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier() // 启动应用日志的 goroutine

	return rf
}

// 生成随机选举超时时间
func (rf *Raft) getRandomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Int63n(150)) * time.Millisecond
}

// applier 持续检查是否有新提交的日志条目需要应用
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		// 检查是否有新的日志条目需要应用
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// 一次性收集所有需要应用的条目，避免在发送过程中释放锁
		var toApply []raftapi.ApplyMsg

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyIndex := rf.lastApplied

			// 检查日志索引是否有效
			if applyIndex >= len(rf.log) {
				DPrintf("[%d] 错误: 尝试应用索引 %d 但日志长度只有 %d", rf.me, applyIndex, len(rf.log))
				rf.lastApplied-- // 回滚
				break
			}

			if rf.log[applyIndex].Command != nil {
				// 使用日志条目中的逻辑索引，而不是数组索引
				logicalIndex := rf.log[applyIndex].LogIndex
				applyMsg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.log[applyIndex].Command,
					CommandIndex: logicalIndex,
				}
				toApply = append(toApply, applyMsg)
				DPrintf("[%d] 准备应用日志条目: arrayIndex=%d, logicalIndex=%d, command=%v",
					rf.me, applyIndex, logicalIndex, rf.log[applyIndex].Command)
			}
		}
		rf.mu.Unlock()

		// 按顺序发送所有待应用的消息
		for _, msg := range toApply {
			DPrintf("[%d] 应用日志条目: index=%d, command=%v", rf.me, msg.CommandIndex, msg.Command)
			rf.applyCh <- msg
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// optimizeNextIndex 根据快速回退字段优化 nextIndex
func (rf *Raft) optimizeNextIndex(server int, reply *AppendEntriesReply) int {
	if reply.XTerm == -1 {
		// 情况 1: Follower日志太短 (PrevLogIndex 越界)
		// 直接设置为Follower的日志长度，这样下次 prevLogIndex = XLen - 1
		return reply.XLen
	}

	// 情况 2: Follower日志与Leader日志在PrevLogIndex处任期不匹配
	conflictTerm := reply.XTerm
	lastIndexOfXTerm := -1

	// Leader 从自己的日志中倒序查找是否有和 Follower 冲突的 XTerm 任期的日志条目
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == conflictTerm {
			lastIndexOfXTerm = i
			break
		}
	}

	if lastIndexOfXTerm != -1 {
		// Leader 有与 Follower 冲突的 XTerm 任期，
		// 则 Leader 的 nextIndex[server] 应该设置为 Leader 中该 XTerm 的最后一个条目的下一个位置。
		// 这样可以确保跳过 Follower 的冲突任期，并从 Leader 自己的该任期之后开始同步。
		return lastIndexOfXTerm + 1
	} else {
		// Leader 没有 Follower 报告的 XTerm 任期（这意味着 Leader 的日志比 Follower 的更旧或者完全不同）
		// Leader 的 nextIndex[server] 应该设置为 Follower 报告的 XIndex。
		// 这样可以跳过 Follower 的整个冲突任期。
		return reply.XIndex
	}
}
