package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
}

func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

func (ck *Clerk) getOrCreateClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	if rck, ok := ck.rcks[gid]; ok {
		return rck
	}
	rck := shardgrp.MakeClerk(ck.clnt, servers)
	ck.rcks[gid] = rck
	return rck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	const maxWrongGroup = 50
	wrongGroupCount := 0
	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(shard)
		if !ok || gid == 0 {
			continue
		}
		rck := ck.getOrCreateClerk(gid, servers)
		val, ver, err := rck.Get(key)
		if err == rpc.OK || err == rpc.ErrNoKey {
			return val, ver, err
		}
		if err == rpc.ErrWrongGroup {
			wrongGroupCount++
			if wrongGroupCount >= maxWrongGroup {
				return "", 0, rpc.ErrNoKey
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err == rpc.ErrWrongLeader {
			continue
		}
		return val, ver, err
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	const maxWrongGroup = 50
	wrongGroupCount := 0
	firstAttempt := true
	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(shard)
		if !ok || gid == 0 {
			continue
		}
		rck := ck.getOrCreateClerk(gid, servers)
		err := rck.Put(key, value, version)
		if err == rpc.OK {
			return rpc.OK
		}
		if err == rpc.ErrWrongGroup {
			wrongGroupCount++
			if wrongGroupCount >= maxWrongGroup {
				return rpc.ErrMaybe
			}
			time.Sleep(10 * time.Millisecond)
			firstAttempt = false
			continue
		}
		if err == rpc.ErrWrongLeader {
			firstAttempt = false
			continue
		}
		if err == rpc.ErrVersion {
			if firstAttempt {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
		if err == rpc.ErrMaybe {
			return rpc.ErrMaybe
		}
		firstAttempt = false
	}
}
