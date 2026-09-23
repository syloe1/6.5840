package shardgrp

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const controllerRPCAttempts = 3

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])

	mu sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}
func (ck *Clerk) setLeader(i int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.leader = i
}
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{
		Key: key,
	}
	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()
	for {
		for i := 0; i < len(ck.servers); i++ {
			srv := (start + i) % len(ck.servers)
			var reply rpc.GetReply
			ok := ck.Call(ck.servers[srv], "KVServer.Get", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup {
				ck.setLeader(srv)
				return reply.Value, reply.Version, reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}

	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	firstAttempt := true
	for {
		for i := 0; i < len(ck.servers); i++ {
			srv := (start + i) % len(ck.servers)
			var reply rpc.PutReply
			ok := ck.Call(ck.servers[srv], "KVServer.Put", &args, &reply)
			if !ok {
				firstAttempt = false
				continue
			}
			if reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup {
				ck.setLeader(srv)
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				firstAttempt = false
				continue
			}
			if reply.Err == rpc.ErrVersion {
				if firstAttempt {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
		}
		firstAttempt = false
		ck.mu.Lock()
		start = ck.leader
		ck.mu.Unlock()
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}

	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	for attempt := 0; attempt < controllerRPCAttempts; attempt++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (start + i) % len(ck.servers)
			var reply shardrpc.FreezeShardReply
			ok := ck.Call(ck.servers[srv], "KVServer.FreezeShard", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == rpc.OK || reply.Err == rpc.ErrVersion {
				ck.setLeader(srv)
				return reply.State, reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				continue
			}
			return nil, reply.Err
		}
		ck.mu.Lock()
		start = ck.leader
		ck.mu.Unlock()
	}
	return nil, rpc.ErrWrongLeader
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}

	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	for attempt := 0; attempt < controllerRPCAttempts; attempt++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (start + i) % len(ck.servers)
			var reply shardrpc.InstallShardReply
			ok := ck.Call(ck.servers[srv], "KVServer.InstallShard", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == rpc.OK || reply.Err == rpc.ErrVersion {
				ck.setLeader(srv)
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				continue
			}
			return reply.Err
		}
		ck.mu.Lock()
		start = ck.leader
		ck.mu.Unlock()
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}

	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	for attempt := 0; attempt < controllerRPCAttempts; attempt++ {
		for i := 0; i < len(ck.servers); i++ {
			srv := (start + i) % len(ck.servers)
			var reply shardrpc.DeleteShardReply
			ok := ck.Call(ck.servers[srv], "KVServer.DeleteShard", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == rpc.OK || reply.Err == rpc.ErrVersion {
				ck.setLeader(srv)
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				continue
			}
			return reply.Err
		}
		ck.mu.Lock()
		start = ck.leader
		ck.mu.Unlock()
	}
	return rpc.ErrWrongLeader
}
