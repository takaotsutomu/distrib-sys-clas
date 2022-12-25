package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrOpFailed    = "ErrOpFailed"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "PUT" or "APPEND"
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
