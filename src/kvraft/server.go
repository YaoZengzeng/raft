package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string // "Get", "Put" or "Append"
	Key   string
	Value string
}

type Result struct {
	Err   Err
	Value string
}

type Handler struct {
	command Op
	ch      chan *Result
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage map[string]string // key/value database.
	notify  map[int]*Handler
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	command := Op{
		Op:  "Get",
		Key: args.Key,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	handler := &Handler{
		command: command,
		ch:      make(chan *Result),
	}
	kv.mu.Lock()
	kv.notify[index] = handler
	kv.mu.Unlock()

	result := <-handler.ch
	reply.Err = result.Err
	reply.Value = result.Value

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	handler := &Handler{
		command: command,
		ch:      make(chan *Result),
	}
	kv.mu.Lock()
	kv.notify[index] = handler
	kv.mu.Unlock()

	result := <-handler.ch
	reply.Err = result.Err

	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.notify = make(map[int]*Handler)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			msg := <-kv.applyCh
			op, ok := msg.Command.(Op)
			if !ok {
				panic("assert command failed")
			}

			var value string
			switch op.Op {
			case "Get":
				value = kv.storage[op.Key]

			case "Put":
				kv.storage[op.Key] = op.Value
				value = op.Value

			case "Append":
				value = kv.storage[op.Key] + op.Value
				kv.storage[op.Key] = value

			default:
				panic("invalid command op")

			}

			handler, ok := kv.notify[msg.CommandIndex]
			if !ok {
				continue
			}

			var result *Result
			if !reflect.DeepEqual(handler.command, op) {
				DPrintf("command %v not committed", op)
				result = &Result{
					Err: "command not committed",
				}
			} else {
				result = &Result{
					Value: value,
				}
			}

			handler.ch <- result
		}
	}()

	return kv
}
