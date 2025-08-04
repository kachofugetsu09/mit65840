package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a g`o interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	
	lid string
	key string // the key to store the lock state
	
	
}

const (
	Unlocked =iota
	Locking
	Locked
	Unlocking
)

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
			ck:  ck,
			key: l,
			lid: kvtest.RandValue(8), // 直接存储字符串，不是int！
		}
		return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for{
		value, version , err :=lk.ck.Get(lk.key);
		if err == rpc.ErrNoKey || (err ==rpc.OK && value == ""){
			putErr := lk.ck.Put(lk.key,lk.lid , version);
			if putErr == rpc.OK{
				return;
			}else if err == rpc.OK && value == lk.lid {
				//自己已经成功获取了
				// return;
			}
		}
	}
	
}

func (lk *Lock) Release() {
	// Your code here
	for {
		value,version,err := lk.ck.Get(lk.key)
		
		if err ==rpc.OK && value == lk.lid{
			putErr := lk.ck.Put(lk.key, "", version)
			if putErr == rpc.OK {
				return
			} else if putErr == rpc.ErrMaybe {
				// 可能已经释放了
				return
			}
		}
	}
}
