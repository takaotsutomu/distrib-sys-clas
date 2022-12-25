package kvraft

import (
	"crypto/rand"
	"lab4/labrpc"
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	serialNr uint64
	leaderId int
	clientId int64
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
	ck.serialNr = 0 // 1-indexed
	ck.leaderId = 0
	ck.clientId = nrand()
	return ck
}

//
// Fetch the current value for a key.
// Returns "" if the key does not exist.
// Keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		SerialNr: atomic.AddUint64(&ck.serialNr, 1),
		ClientId: ck.clientId,
	}
	var reply GetReply
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			break
		}
	}
	return reply.Value
}

//
// Shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		SerialNr: atomic.AddUint64(&ck.serialNr, 1),
		ClientId: ck.clientId,
	}
	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
