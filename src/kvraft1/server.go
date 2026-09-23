package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVValue struct {
	Value   string       //KV存储的实际字符串值
	Version rpc.Tversion //乐观锁版本号， 用于并发Put
}
type KVServer struct {
	me  int
	rsm *rsm.RSM

	mu   sync.Mutex
	data map[string]KVValue
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch r := req.(type) {
	case rpc.GetArgs:
		if v, ok := kv.data[r.Key]; ok {
			return rpc.GetReply{
				Value:   v.Value,
				Version: v.Version,
				Err:     rpc.OK,
			}
		}
		return rpc.GetReply{
			Err: rpc.ErrNoKey,
		}
	case rpc.PutArgs:
		v, ok := kv.data[r.Key]
		if !ok {
			if r.Version == 0 {
				kv.data[r.Key] = KVValue{Value: r.Value, Version: 1}
				return rpc.PutReply{Err: rpc.OK}
			}
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		//key exist, version not match

		if r.Version != v.Version {
			return rpc.PutReply{
				Err: rpc.ErrVersion,
			}
		}
		kv.data[r.Key] = KVValue{Value: r.Value, Version: v.Version + 1}
		return rpc.PutReply{
			Err: rpc.OK,
		}
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var m map[string]KVValue
	if d.Decode(&m) != nil {
		kv.data = make(map[string]KVValue)
		return
	}
	kv.data = m
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	err, r := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := r.(rpc.GetReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	err, r := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := r.(rpc.PutReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})

	kv := &KVServer{
		me:   me,
		data: make(map[string]KVValue),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
