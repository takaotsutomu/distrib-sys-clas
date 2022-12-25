package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"lab5/labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	serialNr uint64
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
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:      num,
		SerialNr: atomic.AddUint64(&ck.serialNr, 1),
		ClientId: ck.clientId,
	}
	for {
		// Try each known server
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:  servers,
		SerialNr: atomic.AddUint64(&ck.serialNr, 1),
		ClientId: ck.clientId,
	}
	for {
		// try each known server
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:     gids,
		SerialNr: atomic.AddUint64(&ck.serialNr, 1),
		ClientId: ck.clientId,
	}
	for {
		// Try each known server
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		SerialNr: atomic.AddUint64(&ck.serialNr, 1),
		ClientId: ck.clientId,
	}
	for {
		// Try each known server
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
