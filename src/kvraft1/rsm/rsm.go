package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Req any
	Id  int64
}

type notify struct {
	err rpc.Err
	rep any
}
type pendingOp struct {
	id   int64       //唯一请求Id
	term int         //提交时的leader任期
	ch   chan notify //客户端submit阻塞等待的通道
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // 快照阈值，>0开启日志裁剪
	sm           StateMachine
	// Your definitions here.
	nextId      int64              //自增序列， 生成唯一Op.Id
	pending     map[int]*pendingOp //key是日志index, value等待提交的客户端请求
	results     map[int64]notify   //key是Op.Id, 缓存已执行请求结果
	lastApplied int                //上一条已执行的Raft日志Index
	lastTerm    int                //上一次记录的Raft任期
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[int]*pendingOp),
		results:      make(map[int64]notify),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	rsm.nextId = rand.Int63() & ((1 << 30) - 1)
	rsm.lastTerm, _ = rsm.rf.GetState()

	snap := persister.ReadSnapshot()
	if snap != nil && len(snap) > 0 {
		rsm.sm.Restore(snap)
	}
	//reader() 是 RSM核心循环， 持续阻塞读取applyCh
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	rsm.nextId++ //高32位节点编号me, 低30位自增nextId

	id := int64(rsm.me)<<32 | rsm.nextId
	op := Op{Req: req, Id: id}
	/*
		index: 这条日志在Raft日志数组中的下标
		term： 当前Leader的任期
		isLeader: 本机是否是Leader
	*/
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	ch := make(chan notify, 1)
	rsm.pending[index] = &pendingOp{id: id, term: term, ch: ch}
	rsm.mu.Unlock()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()

	for {
		// 优先检查结果通道，避免select随机选择ticker
		select {
		case result := <-ch:
			return result.err, result.rep
		default:
		}
		select {
		case result := <-ch:
			return result.err, result.rep
		case <-ticker.C:
			curTerm, isLeader := rsm.rf.GetState()
			if !isLeader || curTerm != term {
				rsm.mu.Lock()
				if p, ok := rsm.pending[index]; ok && p.id == id {
					delete(rsm.pending, index)
				}
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		case <-timeout.C:
			rsm.mu.Lock()
			if p, ok := rsm.pending[index]; ok && p.id == id {
				delete(rsm.pending, index)
			}
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}
}
func (rsm *RSM) reader() {
	/*
		type ApplyMsg struct {
			CommandValid bool
			Command      interface{}
			CommandIndex int

			SnapshotValid bool
			Snapshot      []byte
			SnapshotTerm  int
			SnapshotIndex int
		}
	*/
	for msg := range rsm.applyCh {
		rsm.mu.Lock()
		// 收到正常已提交日志msg.CommandValid == true
		if msg.SnapshotValid {
			if msg.SnapshotIndex > rsm.lastApplied {
				rsm.sm.Restore(msg.Snapshot)
				rsm.lastApplied = msg.SnapshotIndex
			}
			rsm.mu.Unlock()
			continue
		}
		if msg.CommandValid {
			if msg.CommandIndex <= rsm.lastApplied {
				rsm.mu.Unlock()
				continue
			}
			op := msg.Command.(Op)
			// 仅从本机创建的op中追赶nextId，防止被其他server的op.Id污染
			// op.Id = int64(me)<<32 | counter
			if (op.Id >> 32) == int64(rsm.me) {
				counter := op.Id & ((1 << 30) - 1) // 提取低30位计数器
				if counter > rsm.nextId {
					rsm.nextId = counter
				}
			}
			// 清理上一任期所有pending请求
			curTerm, _ := rsm.rf.GetState()
			if curTerm != rsm.lastTerm {
				for idx, p := range rsm.pending {
					if p.term != curTerm {
						select {
						case p.ch <- notify{err: rpc.ErrWrongLeader, rep: nil}:
						default:
						}
						delete(rsm.pending, idx)
					}
				}
				rsm.lastTerm = curTerm
			}
			var result notify
			if prev, ok := rsm.results[op.Id]; ok {
				result = prev
			} else {
				result.rep = rsm.sm.DoOp(op.Req)
				result.err = rpc.OK
				rsm.results[op.Id] = result
			}
			if p, ok := rsm.pending[msg.CommandIndex]; ok {
				if p.id == op.Id {
					select {
					case p.ch <- result:
					default:
					}
				} else {
					select {
					case p.ch <- notify{err: rpc.ErrWrongLeader, rep: nil}:
					default:
					}
				}
				delete(rsm.pending, msg.CommandIndex)
			}
			rsm.lastApplied = msg.CommandIndex
			if rsm.maxraftstate > 0 && rsm.rf.PersistBytes() > rsm.maxraftstate {
				rsm.taskSnapshot(msg.CommandIndex)
			}
		}
		rsm.mu.Unlock()
	}
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for _, p := range rsm.pending {
		select {
		case p.ch <- notify{err: rpc.ErrWrongLeader, rep: nil}:
		default:
		}
	}
	rsm.pending = make(map[int]*pendingOp)
}

func (rsm *RSM) taskSnapshot(index int) {
	if index <= 0 {
		return
	}
	snapshot := rsm.sm.Snapshot()
	rsm.rf.Snapshot(index, snapshot)
}
