package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string //锁名， 不同name是独立锁
	id   string //锁持有者标识， 随机生成
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:   ck,
		name: lockname,
		id:   kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.name)
		//KV中不存在这个key, （锁无人持有， 空锁状态)
		if err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.name, lk.id, 0)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrMaybe {
				value, _, err := lk.ck.Get(lk.name)
				if err == rpc.OK && value == lk.id {
					return //KV是我id = 上锁成功
				}
			}
		} else if err == rpc.OK {
			if value == lk.id {
				return //自己拥有， 可以重入
			}
			if value == "" { //锁被release
				err = lk.ck.Put(lk.name, lk.id, version)
				if err == rpc.OK {
					return //抢到锁
				}
				//处理不确定场景
				if err == rpc.ErrMaybe {
					value, _, err := lk.ck.Get(lk.name)
					if err == rpc.OK && value == lk.id {
						return
					}
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
}

// **谁持有锁，谁才能释放；不能释放别人的锁**。
func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			return
		}
		//Get网络异常
		if err != rpc.OK {
			time.Sleep(time.Millisecond)
			continue
		}
		if value != lk.id {
			return
		}
		//持有者， 执行Put清空value释放锁
		err = lk.ck.Put(lk.name, "", version)
		if err == rpc.OK {
			return
		}
		if err == rpc.ErrMaybe {
			value, _, err := lk.ck.Get(lk.name)
			if err == rpc.OK && value != lk.id {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
}
