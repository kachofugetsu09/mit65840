package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KeyValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	data map[string]KeyValue
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]KeyValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kvData, ok := kv.data[key]; ok {
		reply.Value = kvData.Value
		reply.Version = kvData.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value := args.Value
	//如果存在map当中
	if kvData, ok := kv.data[key];ok{
		
		if kvData.Version != args.Version {
			reply.Err = rpc.ErrVersion
			return
			
		}else{
			kvData.Value = value
			kvData.Version++
			kv.data[key] = kvData
			reply.Err = rpc.OK
		}
	}else{
		
		if args.Version == 0 {
			kv.data[key] = KeyValue{Value: value, Version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
