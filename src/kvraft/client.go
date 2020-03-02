package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id        int64
	requestID int64

	mu     sync.Mutex
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	DPrintf("number of server is %v", len(servers))
	// You'll have to add code here.
	ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{
		Key:       key,
		ClerkID:   ck.id,
		RequestID: ck.requestID,
	}
	ck.requestID = ck.requestID + 1

	ck.mu.Lock()
	i := ck.leader
	ck.mu.Unlock()
	for {
		reply := &GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", args, reply)
		if !ok {
			DPrintf("clerk %d timeout for server %d", ck.id, i)
			// Keep request same instance.
			continue
		} else if reply.Err != "" {
			// DPrintf("clerk %d Get reply error is %v", ck.id, reply.Err)
			i = (i + 1) % len(ck.servers)
			continue
		}

		// Remember leader.
		ck.mu.Lock()
		ck.leader = i
		ck.mu.Unlock()
		return reply.Value
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Op:        op,
		Key:       key,
		Value:     value,
		ClerkID:   ck.id,
		RequestID: ck.requestID,
	}
	ck.requestID = ck.requestID + 1

	ck.mu.Lock()
	i := ck.leader
	ck.mu.Unlock()
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)
		if !ok {
			DPrintf("clerk %d timeout for server %d", ck.id, i)
			// Keep request same instance.
			continue
		} else if reply.Err != "" {
			// DPrintf("clerk %d %v reply error is %v", ck.id, op, reply.Err)
			i = (i + 1) % len(ck.servers)
			continue
		}

		// Remember leader.
		ck.mu.Lock()
		ck.leader = i
		ck.mu.Unlock()
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
