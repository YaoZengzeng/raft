package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
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

	// Make requests idempotent.
	ClerkID   int64
	RequestID int64
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

	lastIndex int
	// lastTerm int

	// ClerkID to RequestID
	records map[int64]int64
}

// Log compaction.
func (kv *KVServer) snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.storage); err != nil {
		panic(fmt.Sprintf("encode storage of snapshot failed: %v", err))
	}
	if err := e.Encode(kv.lastIndex); err != nil {
		panic(fmt.Sprintf("enocode lastindex of snapshot failed: %v", err))
	}
	data := w.Bytes()
	DPrintf("snapshot is %v", data)
	// e.Encode(kv.lastTerm)
	return data
}

// Restore the snapshot.
func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("the persist data of instance %d is empty, maybe start first time?", kv.me)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var (
		lastIndex int //, lastTerm int
		storage   map[string]string
	)

	if err := d.Decode(&storage); err != nil {
		panic(fmt.Sprintf("parse storage of snapshot failed: %v", err))
	}

	if err := d.Decode(&lastIndex); err != nil {
		panic(fmt.Sprintf("pase lastindex of snapshot failed: %v", err))
	}

	kv.storage = storage
	kv.lastIndex = lastIndex
	// kv.lastTerm = lastTerm
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// It's OK for Get to be duplicated.
	command := Op{
		Op:  "Get",
		Key: args.Key,
	}

	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	handler := &Handler{
		command: command,
		ch:      make(chan *Result),
	}
	kv.notify[index] = handler
	kv.mu.Unlock()

	for {
		select {
		case result := <-handler.ch:
			reply.Err = result.Err
			reply.Value = result.Value
			return
		case <-time.After(100 * time.Millisecond):
			current, isLeader := kv.rf.GetState()
			if !isLeader || term != current {
				// This request may never be committed, just return.
				reply.Err = ErrWrongLeader
				go func() {
					<-handler.ch
				}()
				return
			}
		}
	}

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if kv.records[args.ClerkID] > args.RequestID {
		// The request has been committed.
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// Your code here.
	command := Op{
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,

		ClerkID:   args.ClerkID,
		RequestID: args.RequestID,
	}

	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	handler := &Handler{
		command: command,
		ch:      make(chan *Result),
	}
	kv.notify[index] = handler
	kv.mu.Unlock()

	for {
		select {
		case result := <-handler.ch:
			reply.Err = result.Err
			return
		case <-time.After(100 * time.Millisecond):
			current, isLeader := kv.rf.GetState()
			if !isLeader || term != current {
				// This request may never be committed, just return.
				reply.Err = ErrWrongLeader
				go func() {
					<-handler.ch
				}()
				return
			}
		}
	}

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
	kv.records = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			msg := <-kv.applyCh
			DPrintf("Server %d get a message %v from applyCh", kv.me, msg)

			if !msg.CommandValid {
				// It is a snapshot message.
				kv.readSnapshot(msg.Snapshot)

				kv.mu.Lock()
				for index, handler := range kv.notify {
					if index <= kv.lastIndex {
						handler.ch <- &Result{
							Err: "command not committed",
						}
					}
				}
				kv.mu.Unlock()
				continue
			}

			op, ok := msg.Command.(Op)
			if !ok {
				panic("assert command failed")
			}

			var value string
			switch op.Op {
			case "Get":
				value = kv.storage[op.Key]

			case "Put":
				kv.mu.Lock()
				requestID := kv.records[op.ClerkID]
				if op.RequestID > requestID {
					kv.records[op.ClerkID] = op.RequestID
					kv.storage[op.Key] = op.Value
				}
				kv.mu.Unlock()

			case "Append":
				kv.mu.Lock()
				requestID := kv.records[op.ClerkID]
				if op.RequestID > requestID {
					kv.records[op.ClerkID] = op.RequestID
					kv.storage[op.Key] = kv.storage[op.Key] + op.Value
				}
				kv.mu.Unlock()

			default:
				panic("invalid command op")

			}

			kv.lastIndex = msg.CommandIndex

			if persister.RaftStateSize() >= maxraftstate {
				// Make snapshot.
				DPrintf("instance %d make a snapshot", kv.me)
				kv.rf.PersistWithSnapshot(kv.snapshot(), kv.lastIndex)
			}

			kv.mu.Lock()
			handler, ok := kv.notify[msg.CommandIndex]
			kv.mu.Unlock()
			if !ok {
				continue
			}

			var result *Result
			if !reflect.DeepEqual(handler.command, op) {
				if handler.command.Op != "Get" {
					// It's still possible that the request has beem committed.
					kv.mu.Lock()
					requestID := kv.records[handler.command.ClerkID]
					kv.mu.Unlock()
					if handler.command.RequestID <= requestID {
						handler.ch <- &Result{}
						continue
					}
				}
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
