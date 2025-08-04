package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// 网络不可靠环境下需要不断重试直到收到回复

	args := &rpc.GetArgs{Key: key}

	for {
		reply := &rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", args, reply)
		if ok {
			// 成功收到服务器回复，根据错误类型处理
			if reply.Err == rpc.OK {
				// 正常情况：键存在，返回值和版本号
				return reply.Value, reply.Version, rpc.OK
			} else if reply.Err == rpc.ErrNoKey {
				// 键不存在：这是确定的结果，直接返回
				return "", 0, rpc.ErrNoKey
			} else if reply.Err == rpc.ErrVersion {
				// 理论上Get不应该返回版本错误，但为了安全起见返回ErrMaybe
				return "", 0, rpc.ErrMaybe
			}
			// 其他服务器错误，直接返回
			return "", 0, reply.Err
		}
		// 网络调用失败（超时或网络错误），继续重试
		// 因为Get操作不修改服务器状态，重试是安全的
	}
}

// Put 仅在请求中的版本号与服务器上键的版本号匹配时更新键值。
// 如果版本号不匹配，服务器会返回 ErrVersion。
//
// 异常情况处理逻辑：
// 1. 首次RPC返回ErrVersion：确定Put没有执行，返回ErrVersion
// 2. 重发RPC返回ErrVersion：不确定之前的RPC是否成功，返回ErrMaybe
//   - 可能情况1：首次RPC成功但响应丢失，重发RPC收到ErrVersion
//   - 可能情况2：首次RPC失败，重发RPC也因版本冲突收到ErrVersion
//
// 3. 网络故障：需要不断重试，但要区分首次调用和重试调用
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// 区分首次调用和重试调用，以正确处理ErrVersion

	args := &rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := &rpc.PutReply{}

	// 首次尝试调用
	ok := ck.clnt.Call(ck.server, "KVServer.Put", args, reply)

	if ok {
		// 首次调用成功收到回复
		if reply.Err == rpc.OK {
			// Put操作成功执行
			return rpc.OK
		} else if reply.Err == rpc.ErrVersion {
			// 首次调用返回版本错误：确定Put没有执行，直接返回ErrVersion
			// 这种情况下我们知道操作肯定没有成功
			return rpc.ErrVersion
		}
		if reply.Err == rpc.ErrNoKey {
			// 键不存在且版本号不为0
			return rpc.ErrNoKey
		}
		// 其他错误直接返回
		return reply.Err
	}

	// 首次调用失败（网络问题），需要不断重试
	// 从这里开始的所有调用都被认为是"重试"
	for {
		reply = &rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", args, reply)

		if ok {
			// 重试调用成功收到回复
			if reply.Err == rpc.OK {
				// Put操作成功执行
				return rpc.OK
			} else if reply.Err == rpc.ErrVersion {
				// 重试调用返回版本错误：
				// 我们不知道之前的RPC是否成功：
				//  可能首次RPC成功但响应丢失，重发收到ErrVersion
				//  可能首次RPC失败，重发也收到ErrVersion
				// 因此必须返回ErrMaybe表示不确定状态
				return rpc.ErrMaybe
			}
			if reply.Err == rpc.ErrNoKey {
				// 键不存在且版本号不为0
				return rpc.ErrNoKey
			}
			// 其他错误直接返回
			return reply.Err
		}
		// 重试调用也失败，继续重试
	}
}
