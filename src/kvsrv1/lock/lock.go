package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	lid string // 锁ID：用于标识当前客户端持有的锁（随机生成的唯一标识符）
	key string // 锁键名：在KV存储中用于存储锁状态的键
}

const (
	Unlocked = iota
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
		key: l,                   // 锁在KV存储中的键名
		lid: kvtest.RandValue(8), // 生成唯一的客户端标识符（8位随机字符串）
	}
	return lk
}

// Acquire 获取锁，如果锁已被其他客户端持有则阻塞等待
//
// 锁获取的逻辑和异常情况处理：
// 1. 检查锁的当前状态
// 2. 如果锁可用（不存在或值为空），尝试获取
// 3. 如果已经持有锁，直接返回
// 4. 如果被其他客户端持有，继续等待
// 5. 处理网络异常和不确定状态
func (lk *Lock) Acquire() {
	for {
		// 获取锁的当前状态
		value, version, err := lk.ck.Get(lk.key)

		// 情况1: 锁不存在或锁未被持有（值为空字符串）
		if err == rpc.ErrNoKey || (err == rpc.OK && value == "") {
			// 尝试获取锁：设置锁的值为我们的客户端ID
			putErr := lk.ck.Put(lk.key, lk.lid, version)

			if putErr == rpc.OK {
				// 成功获取锁
				return
			} else if putErr == rpc.ErrMaybe {
				// Put操作可能成功了，需要验证是否真的获取到了锁
				// 这种情况发生在：Put请求成功但响应丢失
				checkValue, _, checkErr := lk.ck.Get(lk.key)
				if checkErr == rpc.OK && checkValue == lk.lid {
					// 验证确认我们已经持有锁
					return
				}
				// 验证失败，继续尝试获取锁
			}
			// Put失败（ErrVersion等），说明锁状态在我们Get和Put之间被其他客户端改变了
			// 继续循环重试

		} else if err == rpc.OK && value == lk.lid {
			// 情况2: 我们已经持有了这个锁（重入情况）
			return

		} else if err == rpc.OK && value != "" {
			// 情况3: 锁被其他客户端持有，等待锁释放
			// 不执行任何操作，继续循环等待
			continue

		}
		// 情况4: Get操作失败（网络问题等）
		// 继续循环重试，直到成功获取锁状态
	}
}

// Release 释放锁
//
// 锁释放的逻辑和异常情况处理：
// 1. 检查当前是否持有锁
// 2. 如果持有锁，尝试释放（设置值为空字符串）
// 3. 如果不持有锁，直接返回
// 4. 处理网络异常和不确定状态
func (lk *Lock) Release() {
	for {
		// 获取锁的当前状态
		value, version, err := lk.ck.Get(lk.key)

		// 情况1: 成功获取锁状态，且确认我们持有这个锁
		if err == rpc.OK && value == lk.lid {
			// 尝试释放锁：将锁的值设置为空字符串
			putErr := lk.ck.Put(lk.key, "", version)

			if putErr == rpc.OK {
				// 成功释放锁
				return
			} else if putErr == rpc.ErrMaybe {
				// Put操作可能成功了，需要验证锁是否真的被释放
				// 这种情况发生在：Put请求成功但响应丢失
				checkValue, _, checkErr := lk.ck.Get(lk.key)
				if checkErr == rpc.OK && checkValue != lk.lid {
					// 验证确认锁已经被释放（值不再是我们的ID）
					return
				}
				// 验证失败，可能锁仍然被我们持有，继续尝试释放
			}
			// Put失败（ErrVersion等），说明锁状态在我们Get和Put之间被改变了
			// 继续循环重试

		} else if err == rpc.OK && value != lk.lid {
			// 情况2: 锁不是由我们持有的（可能已经被释放或被其他客户端持有）
			return
		}

		// 情况3: Get操作失败（网络问题等）
		// 继续循环重试，直到能够确定锁的状态
	}
}
