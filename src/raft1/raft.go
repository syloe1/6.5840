package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"

	heartbeatInterval  = 120 * time.Millisecond
	minElectionTimeout = 450 * time.Millisecond
	electionJitter     = 300 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //新日志前面紧邻那条日志的索引。例如：要从索引 6 开始发日志，prevLogIndex=5
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int //Leader 本地的 commitIndex，同步告知 Follower 日志提交水位
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	// Fast backup optimization (Section 5.3)
	XTerm  int // term of conflicting entry, or -1 if none
	XIndex int // first index of XTerm in follower's log
	XLen   int // follower's log length
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	//论文Figure2强制标准持久化状态
	currentTerm int //节点见过的最新任期号
	votedFor    int //当前任期内， 本节点投票给哪个人（null, -1还没投票. )
	state       string
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int
	//论文Figure2 volatile 状态， 不需要持久化
	commitIndex int //已知被提交的最高日志条目索引， 单增
	lastApplied int //已经应用到上层状态机的最高日志索引,如果lastApplied < commitIndex就lastApplied++,

	// Volatile state for leaders (Figure 2)
	nextIndex  []int // for each peer, index of next log entry to send
	matchIndex []int // for each peer, highest log entry known to be replicated

	lastHeartBeat     time.Time     //Follower视角
	electionTimeout   time.Duration //选举超时阈值
	lastHeartbeatSent time.Time     //Leader视角，上一次向外广播心跳的时间

	apply     chan raftapi.ApplyMsg //推送日志通道
	applyCond *sync.Cond            // condition variable to signal new committed entries
	dead      int32                 //进程停止标识
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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
	Term         int //候选人当前任期
	CandidateId  int //发起选举的候选人编号
	LastLogIndex int //候选人本地最后一条日志的索引
	LastLogTerm  int //候选人本地最后一条日志对应的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	/*
		接收节点的 currentTerm；
		候选人收到后，如果对方 term 更大，立刻更新自身 term，变回 Follower
	*/
	Term        int
	VoteGranted bool //true：同意投票；false：拒绝投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//请求任期 < 本地任期 ， 集群进入更高任期， 拒绝投票， 并且把我当前最新term告诉对方
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 请求任期 > 本地任期， 切换为Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	/*
	   节点只会把票投给日志不比自己旧的候选人：
	   候选人日志更新，满足下面二者其一：

	   1. 候选人最后一条日志的 Term > 我本地最后一条日志 Term
	   OR
	   2. Term 相等，候选人日志索引 ≥ 我本地索引
	*/
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	lastLogIndex, lastLogTerm := rf.lastLogInfoLocked()
	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logIsUpToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		//重置选举超时计数器
		rf.resetElectionTimerLocked()
		reply.VoteGranted = true
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

// AppendEntries handles both log replication and the empty AppendEntries RPCs
// used as heartbeats in 3A.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.logLength()

	DPrintf("S%d AE recv: T=%d PT=%d PI=%d LC=%d nEntries=%d myTerm=%d myLogLen=%d myCommit=%d log=%v",
		rf.me, args.Term, args.PrevLogTerm, args.PrevLogIndex, args.LeaderCommit,
		len(args.Entries), rf.currentTerm, rf.logLength(), rf.commitIndex, rf.log)

	// 请求任期 < 本机任期， 拒绝
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	} else if rf.state != Follower {
		rf.state = Follower
	}
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	// 检查日志在PrevLogIndex处是否包含匹配任期
	if args.PrevLogIndex > rf.logLength() {
		// 我们的日志与PrevLogIndex之间缺失条目
		reply.XLen = rf.logLength()
		return
	}

	if args.PrevLogIndex == rf.lastIncludedIndex {
		if args.PrevLogTerm != rf.lastIncludedTerm {
			return
		}
	} else if args.PrevLogIndex > rf.lastIncludedIndex {
		localIdx := args.PrevLogIndex - rf.lastIncludedIndex - 1
		if localIdx >= len(rf.log) || rf.log[localIdx].Term != args.PrevLogTerm {
			// 发现冲突：提供信息并返回
			if localIdx < len(rf.log) {
				reply.XTerm = rf.log[localIdx].Term
				// 找到该任期的第一个索引
				for j := args.PrevLogIndex; j > rf.lastIncludedIndex; j-- {
					jj := j - rf.lastIncludedIndex - 1
					if rf.log[jj].Term != reply.XTerm {
						reply.XIndex = j + 1
						break
					}
				}
				if reply.XIndex == -1 {
					reply.XIndex = rf.lastIncludedIndex + 1
				}
			}
			return
		}
	} else {
		// PrevLogIndex < lastIncludedIndex: 已被快照截断
		return
	}

	// PrevLogIndex匹配，现在追加新条目
	for i, entry := range args.Entries {
		globalIdx := args.PrevLogIndex + 1 + i
		if globalIdx > rf.lastIncludedIndex {
			localIdx := globalIdx - rf.lastIncludedIndex - 1
			if localIdx < len(rf.log) {
				if rf.log[localIdx].Term != entry.Term {
					// 冲突：从此处截断日志
					rf.log = rf.log[:localIdx]
				}
			}
			if localIdx == len(rf.log) {
				rf.log = append(rf.log, entry)
			}
			// 如果条目已存在且任期相同，则保留（无操作）
		}
		// 如果globalIdx <= rf.lastIncludedIndex，则已在快照中，跳过
	}

	// 更新提交索引
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
		DPrintf("S%d AE commit updated: commitIndex=%d", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}

	reply.Success = true
	DPrintf("S%d AE success: log=%v commit=%d", rf.me, rf.log, rf.commitIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// Append to leader's log
	entry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	index := rf.lastIncludedIndex + len(rf.log) // 1-indexed position of new entry

	DPrintf("S%d Start: cmd=%v index=%d term=%d logLen=%d", rf.me, command, index, rf.currentTerm, len(rf.log))

	// Update leader's own matchIndex
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	// Trigger immediate replication
	rf.broadcastAppendEntriesLocked()

	return index, rf.currentTerm, true
}

// Kill asks long-running Raft goroutines to stop after a test or restart.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}
func (rf *Raft) randomElectionTimeoutLocked() time.Duration {
	return minElectionTimeout + time.Duration(rand.Int63n(int64(electionJitter)))
}
func (rf *Raft) resetElectionTimerLocked() {
	rf.lastHeartBeat = time.Now()
	rf.electionTimeout = rf.randomElectionTimeoutLocked()
}
func (rf *Raft) becomeFollowerLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
}
func (rf *Raft) lastLogInfoLocked() (int, int) {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	//lastIncludedIndex = 快照中最后一条日志的全局Index
	// lastIncludedIndex = 5    // 快照截止到 index=5
	// rf.log = [entry6, entry7, entry8]
	// len(rf.log) = 3
	// 内存切片下标	全局索引
	// rf.log[0]	5+1 = 6
	// rf.log[1]	5+2 = 7
	// rf.log[2]	5+3 = 8

	return rf.lastIncludedIndex + len(rf.log), rf.log[len(rf.log)-1].Term
}

// getLogEntry returns the LogEntry at global index i.
// Returns the entry and true if found, or empty entry and false otherwise.
func (rf *Raft) getLogEntry(i int) (LogEntry, bool) {
	if i == rf.lastIncludedIndex {
		return LogEntry{Term: rf.lastIncludedTerm, Command: nil}, true
	}
	if i > rf.lastIncludedIndex {
		localIdx := i - rf.lastIncludedIndex - 1
		if localIdx < len(rf.log) {
			return rf.log[localIdx], true
		}
	}
	return LogEntry{}, false
}

// logLength returns the total number of log entries including snapshotted ones.
func (rf *Raft) logLength() int {
	return rf.lastIncludedIndex + len(rf.log)
}

// getPrevLogInfo returns the index and term for the entry before the given nextIndex.
func (rf *Raft) getPrevLogInfo(nextIdx int) (int, int) {
	prevIdx := nextIdx - 1
	entry, ok := rf.getLogEntry(prevIdx)
	if !ok {
		// This shouldn't happen; fall back to lastIncludedIndex
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	return prevIdx, entry.Term
}

func (rf *Raft) startElectionLocked() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimerLocked()

	term := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.lastLogInfoLocked()
	//candidate向其他节点拉票
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	// ticket initialize
	votes := 1
	majority := len(rf.peers)/2 + 1 //过半数
	//单机集群
	if votes >= majority {
		rf.becomeLeaderLocked()
		return
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.resetElectionTimerLocked()
				return
			}
			// 三种情况不统计：
			// ① 当前已经不是Candidate（收到Leader心跳，中途放弃竞选）
			// ② 当前term已经改变（等待响应过程中超时，开启新一轮选举）
			// ③ 对方没有投赞成票
			if rf.state != Candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= majority && rf.state == Candidate {
				rf.becomeLeaderLocked()
			}
		}(peer) //避免goroutine闭包问题
	}
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader

	// Initialize nextIndex and matchIndex for all peers
	lastLogIndex, _ := rf.lastLogInfoLocked()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	// Leader's own matchIndex is the last log index
	rf.matchIndex[rf.me] = lastLogIndex

	rf.lastHeartbeatSent = time.Time{} //刚当选Leader立刻发送第一轮心跳
	rf.broadcastAppendEntriesLocked()
}

// broadcastAppendEntriesLocked sends AppendEntries RPCs to all peers.
// It sends log entries if the peer is behind, or an empty heartbeat if caught up.
func (rf *Raft) broadcastAppendEntriesLocked() {
	if rf.state != Leader {
		return
	}
	term := rf.currentTerm
	rf.lastHeartbeatSent = time.Now()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// Prepare AppendEntries for this specific peer
		prevIdx, prevTerm := rf.getPrevLogInfo(rf.nextIndex[peer])
		lastLogIndex, _ := rf.lastLogInfoLocked()

		var entries []LogEntry
		if rf.nextIndex[peer] <= lastLogIndex {
			// Peer is behind: send log entries starting from nextIndex[peer]
			startLocalIdx := rf.nextIndex[peer] - rf.lastIncludedIndex - 1
			entries = make([]LogEntry, len(rf.log)-startLocalIdx)
			copy(entries, rf.log[startLocalIdx:])
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func(server int, sendArgs AppendEntriesArgs) {
			var reply AppendEntriesReply
			if !rf.sendAppendEntries(server, &sendArgs, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.resetElectionTimerLocked()
				return
			}

			if rf.state != Leader || rf.currentTerm != term {
				return
			}

			if reply.Success {
				// Update nextIndex and matchIndex
				lastSentIdx := sendArgs.PrevLogIndex + len(sendArgs.Entries)
				if lastSentIdx >= rf.nextIndex[server] {
					rf.nextIndex[server] = lastSentIdx + 1
					rf.matchIndex[server] = lastSentIdx
				}
				// Check if we can advance commitIndex
				rf.advanceCommitIndexLocked()
			} else {
				// Fast backup: use conflict info from reply
				if reply.XTerm != -1 {
					// Find the last entry with the conflicting term in leader's log
					found := false
					for j := sendArgs.PrevLogIndex; j > rf.lastIncludedIndex; j-- {
						jj := j - rf.lastIncludedIndex - 1
						if jj < len(rf.log) && rf.log[jj].Term == reply.XTerm {
							// Last entry in leader's log with this term
							rf.nextIndex[server] = j + 1
							found = true
							break
						}
					}
					if !found {
						// No entries with this term in leader's log: skip past them
						rf.nextIndex[server] = reply.XIndex
					}
				} else if reply.XLen > 0 {
					// Follower's log is too short
					rf.nextIndex[server] = reply.XLen + 1
				} else {
					// Simple decrement
					rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
				}
				// Don't retry here; ticker will retry at next heartbeat
			}
		}(peer, args)
	}
}

// advanceCommitIndexLocked checks if the commitIndex can be advanced.
// If there exists an N > commitIndex such that a majority of matchIndex[i] >= N,
// and log[N].term == currentTerm, then set commitIndex = N.
func (rf *Raft) advanceCommitIndexLocked() {
	lastLogIndex, _ := rf.lastLogInfoLocked()

	for n := lastLogIndex; n > rf.commitIndex; n-- {
		// Only commit entries from our current term (Figure 8)
		entry, ok := rf.getLogEntry(n)
		if !ok || entry.Term != rf.currentTerm {
			continue
		}

		count := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= len(rf.peers)/2+1 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

// applier is a goroutine that sends committed entries on applyCh.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// Collect entries to apply
		var msgs []raftapi.ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry, ok := rf.getLogEntry(rf.lastApplied)
			if !ok {
				// Shouldn't happen; skip
				continue
			}
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			})
			DPrintf("S%d applier: applying index=%d cmd=%v", rf.me, rf.lastApplied, entry.Command)
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.apply <- msg
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		switch rf.state {
		case Leader:
			//该发心跳了
			if time.Since(rf.lastHeartbeatSent) >= heartbeatInterval {
				rf.broadcastAppendEntriesLocked()
			}
		default:
			if time.Since(rf.lastHeartBeat) >= rf.electionTimeout {
				rf.startElectionLocked()
			}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.apply = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Lock()
	rf.resetElectionTimerLocked()
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to apply committed entries
	go rf.applier()

	return rf
}
