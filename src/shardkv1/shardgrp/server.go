package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	shardcfg "6.5840/shardkv1/shardcfg"

	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type KVValue struct {
	Value   string
	Version rpc.Tversion
}

type ShardState struct {
	State  []byte        //serialized map
	Num    shardcfg.Tnum //config num
	Frozen bool
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	mu     sync.Mutex
	data   map[string]KVValue              //key->value
	shards [shardcfg.NShards]*ShardState   //per-shard state
	maxNum [shardcfg.NShards]shardcfg.Tnum //highest num seen per shard(to reject old RPCS)
}

func (kv *KVServer) isResponsible(shard shardcfg.Tshid) bool {
	s := kv.shards[shard]
	return s != nil && s.State != nil && !s.Frozen
}

func (kv *KVServer) getShardData(shard shardcfg.Tshid) ([]byte, bool) {
	s := kv.shards[shard]
	if s == nil || s.State == nil {
		return nil, false
	}
	return s.State, true
}

func (kv *KVServer) marshalData(shard shardcfg.Tshid) []byte {
	subset := make(map[string]KVValue)
	for k, v := range kv.data {
		if shardcfg.Key2Shard(k) == shard {
			subset[k] = v
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(subset)
	return w.Bytes()
}
func (kv *KVServer) unmarshalData(state []byte) map[string]KVValue {
	if state == nil || len(state) == 0 {
		return make(map[string]KVValue)
	}
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var m map[string]KVValue

	if d.Decode(&m) != nil {
		return make(map[string]KVValue)
	}
	return m
}
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch r := req.(type) {
	case rpc.GetArgs:
		shard := shardcfg.Key2Shard(r.Key)
		if !kv.isResponsible(shard) {
			return rpc.GetReply{
				Err: rpc.ErrWrongGroup,
			}
		}
		v, ok := kv.data[r.Key]
		if !ok {
			return rpc.GetReply{
				Err: rpc.ErrNoKey,
			}
		}
		return rpc.GetReply{
			Value:   v.Value,
			Version: v.Version,
			Err:     rpc.OK,
		}

	case rpc.PutArgs:
		shard := shardcfg.Key2Shard(r.Key)
		if !kv.isResponsible(shard) {
			return rpc.PutReply{
				Err: rpc.ErrWrongGroup,
			}
		}
		v, ok := kv.data[r.Key]
		if !ok {
			if r.Version == 0 {
				kv.data[r.Key] = KVValue{
					Value:   r.Value,
					Version: 1,
				}
				return rpc.PutReply{
					Err: rpc.OK,
				}
			}
			return rpc.PutReply{
				Err: rpc.ErrVersion,
			}
		}
		if r.Version != v.Version {
			return rpc.PutReply{
				Err: rpc.ErrVersion,
			}
		}
		kv.data[r.Key] = KVValue{
			Value:   r.Value,
			Version: v.Version + 1,
		}
		return rpc.PutReply{
			Err: rpc.OK,
		}
	case shardrpc.FreezeShardArgs:
		if r.Num < kv.maxNum[r.Shard] {
			return shardrpc.FreezeShardReply{
				Num: kv.maxNum[r.Shard],
				Err: rpc.ErrVersion,
			}
		}
		kv.maxNum[r.Shard] = r.Num
		if kv.shards[r.Shard] != nil {
			kv.shards[r.Shard].Frozen = true
		}
		state := kv.marshalData(r.Shard)
		return shardrpc.FreezeShardReply{
			State: state,
			Num:   r.Num,
			Err:   rpc.OK,
		}
	case shardrpc.InstallShardArgs:
		if r.Num < kv.maxNum[r.Shard] {
			return shardrpc.InstallShardReply{
				Err: rpc.ErrVersion,
			}
		}
		if r.Num == kv.maxNum[r.Shard] && kv.shards[r.Shard] != nil {
			return shardrpc.InstallShardReply{
				Err: rpc.OK,
			}
		}
		kv.maxNum[r.Shard] = r.Num
		incoming := kv.unmarshalData(r.State)
		for k, v := range incoming {
			if existing, ok := kv.data[k]; !ok || v.Version > existing.Version {
				kv.data[k] = v
			}
		}
		kv.shards[r.Shard] = &ShardState{
			State:  r.State,
			Num:    r.Num,
			Frozen: false,
		}
		return shardrpc.InstallShardReply{
			Err: rpc.OK,
		}
	case shardrpc.DeleteShardArgs:
		if r.Num < kv.maxNum[r.Shard] {
			return shardrpc.DeleteShardReply{
				Err: rpc.ErrVersion,
			}
		}
		kv.maxNum[r.Shard] = r.Num
		for k := range kv.data {
			if shardcfg.Key2Shard(k) == r.Shard {
				delete(kv.data, k)
			}
		}
		kv.shards[r.Shard] = &ShardState{
			State:  nil,
			Num:    r.Num,
			Frozen: false,
		}
		return shardrpc.DeleteShardReply{
			Err: rpc.OK,
		}
	default:
		return nil
	}
}
func (kv *KVServer) fullSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.shards)
	e.Encode(kv.maxNum)
	return w.Bytes()
}
func (kv *KVServer) Snapshot() []byte {
	return kv.fullSnapshot()
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
	var s [shardcfg.NShards]*ShardState
	var n [shardcfg.NShards]shardcfg.Tnum

	if d.Decode(&m) != nil || d.Decode(&s) != nil || d.Decode(&n) != nil {
		return
	}
	kv.data = m
	kv.shards = s
	kv.maxNum = n
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, r := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := r.(shardrpc.FreezeShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, r := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := r.(shardrpc.InstallShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, r := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := r.(shardrpc.DeleteShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardReply{})

	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me, data: make(map[string]KVValue)}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	//默认把10个分片全部交给git = 1的分片组管理
	//initialize every state

	if gid == shardcfg.Gid1 {
		for sh := shardcfg.Tshid(0); sh < shardcfg.NShards; sh++ {
			kv.shards[sh] = &ShardState{
				State:  kv.marshalData(sh),
				Num:    shardcfg.NumFirst,
				Frozen: false,
			}
		}
	}
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
