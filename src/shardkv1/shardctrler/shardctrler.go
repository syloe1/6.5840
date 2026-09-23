package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	configKeyCurr = "SHARDCFG"      // current/active config
	configKeyNext = "SHARDCFG_NEXT" // next/pending config
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	sck.finishPending()
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.Put(configKeyCurr, cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		curr, _, ok := sck.readConfig(configKeyCurr)
		if !ok || new.Num != curr.Num+1 {
			return
		}

		next, nver, ok := sck.readNext()
		if !ok {
			return
		}
		if next != nil {
			if next.Num <= curr.Num {
				if sck.clearNext(next, nver) {
					continue
				}
				return
			}
			if sameConfig(next, new) {
				sck.doMigration(curr, new)
			}
			return
		}

		err := sck.Put(configKeyNext, new.String(), nver)
		if err == rpc.OK {
			sck.doMigration(curr, new)
			return
		}
		next, _, ok = sck.readNext()
		if ok && next != nil && sameConfig(next, new) {
			sck.doMigration(curr, new)
		}
		return
	}
}

// doMigration performs the full two-phase migration for the given next config.
func (sck *ShardCtrler) doMigration(old, new *shardcfg.ShardConfig) {
	if new.Num <= old.Num {
		return
	}

	for {
		curr, _, ok := sck.readConfig(configKeyCurr)
		if !ok {
			return
		}
		if curr.Num >= new.Num {
			if curr.Num == new.Num {
				sck.deleteOldShards(old, new)
				sck.clearNextConfig(new)
			}
			return
		}
		if !sameConfig(curr, old) {
			return
		}

		next, _, ok := sck.readNext()
		if !ok || next == nil || !sameConfig(next, new) {
			return
		}

		// Phase 1: Freeze old groups and Install data at new groups.
		if !sck.freezeAndInstall(old, new) {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		// Phase 2: Promote config so clients route to new groups.
		curr, ver, ok := sck.readConfig(configKeyCurr)
		if !ok {
			return
		}
		if curr.Num >= new.Num {
			if curr.Num == new.Num {
				sck.deleteOldShards(old, new)
				sck.clearNextConfig(new)
			}
			return
		}
		if !sameConfig(curr, old) {
			return
		}
		next, _, ok = sck.readNext()
		if !ok || next == nil || !sameConfig(next, new) {
			return
		}
		err := sck.Put(configKeyCurr, new.String(), ver)

		curr, _, ok = sck.readConfig(configKeyCurr)
		if !ok || (err != rpc.OK && !sameConfig(curr, new)) {
			return
		}
		if !sameConfig(curr, new) {
			return
		}

		// Phase 3: Delete shards from old groups.
		if !sck.deleteOldShards(old, new) {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		// Clear the next config.
		sck.clearNextConfig(new)
		return
	}
}

// freezeAndInstall freezes shards at old groups and installs data at new groups.
func (sck *ShardCtrler) freezeAndInstall(old, new *shardcfg.ShardConfig) bool {
	for sh := shardcfg.Tshid(0); sh < shardcfg.NShards; sh++ {
		oldGid := old.Shards[sh]
		newGid := new.Shards[sh]
		if oldGid == newGid {
			continue
		}
		var state []byte
		if srvs, ok := old.Groups[oldGid]; ok {
			ck := shardgrp.MakeClerk(sck.clnt, srvs)
			st, err := ck.FreezeShard(sh, new.Num)
			if err != rpc.OK {
				return false
			}
			state = st
		}
		if state != nil {
			if srvs, ok := new.Groups[newGid]; ok {
				dck := shardgrp.MakeClerk(sck.clnt, srvs)
				if err := dck.InstallShard(sh, state, new.Num); err != rpc.OK {
					return false
				}
			}
		}
	}
	return true
}

// deleteOldShards removes shard data from old groups after config promotion.
func (sck *ShardCtrler) deleteOldShards(old, new *shardcfg.ShardConfig) bool {
	for sh := shardcfg.Tshid(0); sh < shardcfg.NShards; sh++ {
		oldGid := old.Shards[sh]
		newGid := new.Shards[sh]
		if oldGid == newGid {
			continue
		}
		if srvs, ok := old.Groups[oldGid]; ok {
			ck := shardgrp.MakeClerk(sck.clnt, srvs)
			if err := ck.DeleteShard(sh, new.Num); err != rpc.OK {
				return false
			}
		}
	}
	return true
}

func (sck *ShardCtrler) finishPending() {
	for {
		curr, _, ok := sck.readConfig(configKeyCurr)
		if !ok {
			return
		}
		next, nver, ok := sck.readNext()
		if !ok || next == nil {
			return
		}
		if next.Num <= curr.Num {
			if sck.clearNext(next, nver) {
				continue
			}
			return
		}
		sck.doMigration(curr, next)
		return
	}
}

func (sck *ShardCtrler) readConfig(key string) (*shardcfg.ShardConfig, rpc.Tversion, bool) {
	val, ver, err := sck.Get(key)
	if err != rpc.OK || len(val) == 0 {
		return nil, 0, false
	}
	return shardcfg.FromString(val), ver, true
}

func (sck *ShardCtrler) readNext() (*shardcfg.ShardConfig, rpc.Tversion, bool) {
	val, ver, err := sck.Get(configKeyNext)
	if err == rpc.ErrNoKey {
		return nil, 0, true
	}
	if err != rpc.OK {
		return nil, 0, false
	}
	if len(val) == 0 {
		return nil, ver, true
	}
	return shardcfg.FromString(val), ver, true
}

func (sck *ShardCtrler) clearNextConfig(cfg *shardcfg.ShardConfig) bool {
	next, ver, ok := sck.readNext()
	if !ok || next == nil || !sameConfig(next, cfg) {
		return false
	}
	return sck.clearNext(next, ver)
}

func (sck *ShardCtrler) clearNext(cfg *shardcfg.ShardConfig, ver rpc.Tversion) bool {
	if cfg == nil {
		return false
	}
	return sck.Put(configKeyNext, "", ver) == rpc.OK
}

func sameConfig(a, b *shardcfg.ShardConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.String() == b.String()
}

// Query returns the committed current configuration.
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	val, _, err := sck.Get(configKeyCurr)
	if err != rpc.OK {
		return shardcfg.MakeShardConfig()
	}
	return shardcfg.FromString(val)
}
