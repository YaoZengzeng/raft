package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	// If the request timeout, we need to retry to figure out
	// whether the request is commited. If some instance reply
	// us that it is not , it will return ErrNotCommmited, then
	// we could safely launch another request with new index and term.
	ErrNotCommitted = "ErrNotCommitted"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID   int64
	RequestID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID   int64
	RequestID int64
}

type GetReply struct {
	Err   Err
	Value string
}
