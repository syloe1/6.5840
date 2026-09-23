package kvraft

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])

	mu sync.Mutex //保存并发读写Leader字段
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
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

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	var reply rpc.GetReply

	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	for {
		for i := 0; i < len(ck.servers); i++ {
			srv := (start + i) % len(ck.servers)
			ok := ck.clnt.Call(ck.servers[srv], "KVServer.Get", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				ck.setLeader(srv)
				return reply.Value, reply.Version, reply.Err
			}
			//follower
			if reply.Err == rpc.ErrWrongLeader {
				continue
			}
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	var reply rpc.PutReply
	ck.mu.Lock()
	start := ck.leader
	ck.mu.Unlock()

	first := true
	for {
		for i := 0; i < len(ck.servers); i++ {
			srv := (start + i) % len(ck.servers)
			ok := ck.clnt.Call(ck.servers[srv], "KVServer.Put", &args, &reply)
			if !ok {
				first = false
				continue
			}
			//net fail, 换一台
			if reply.Err == rpc.OK {
				ck.setLeader(srv)
				return rpc.OK
			}
			if reply.Err == rpc.ErrWrongLeader {
				first = false
				continue
			}
			if reply.Err == rpc.ErrVersion {
				if first {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
		}
		first = false
		ck.mu.Lock()

		start = ck.leader
		ck.mu.Unlock()
	}

}
