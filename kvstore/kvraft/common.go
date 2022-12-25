package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrOpFailed    = "ErrOpFailed"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	SerialNr uint64
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	SerialNr uint64
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}
