package kvraft

import (
	"bytes"
	"encoding/gob"
	"lab4/labrpc"
	"lab4/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type  string
	Key   string
	Value string

	// To ensure exactly-once semantics
	SerialNr uint64
	ClientId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	kvStore map[string]string
	applied map[int]chan Op
	lstProc map[int64]uint64 // map[ClientId]SerialNr

	isKilled int32 // set by Kill()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if args.SerialNr <= kv.lstProc[args.ClientId] {
		reply.Value = kv.kvStore[args.Key]
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(
		Op{
			Type:     "GET",
			Key:      args.Key,
			SerialNr: args.SerialNr,
			ClientId: args.ClientId,
		})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan Op)
	kv.mu.Lock()
	kv.applied[index] = ch
	kv.mu.Unlock()
	select {
	case op := <-ch:
		if op.ClientId != args.ClientId ||
			op.SerialNr != args.SerialNr {
			reply.Err = ErrOpFailed
		} else {
			kv.mu.Lock()
			reply.Value = kv.kvStore[args.Key]
			kv.mu.Unlock()
			reply.Err = OK
		}
	case <-time.After(1 * time.Second):
		reply.Err = ErrOpFailed
	}
	kv.mu.Lock()
	close(ch)
	delete(kv.applied, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if args.SerialNr <= kv.lstProc[args.ClientId] {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(
		Op{
			Type:     args.Op,
			Key:      args.Key,
			Value:    args.Value,
			SerialNr: args.SerialNr,
			ClientId: args.ClientId,
		})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan Op)
	kv.mu.Lock()
	kv.applied[index] = ch
	kv.mu.Unlock()
	select {
	case op := <-ch:
		if op.ClientId != args.ClientId ||
			op.SerialNr != args.SerialNr {
			reply.Err = ErrOpFailed
		} else {
			reply.Err = OK
		}
	case <-time.After(1 * time.Second):
		reply.Err = ErrOpFailed
	}
	kv.mu.Lock()
	close(ch)
	delete(kv.applied, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.isKilled, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	flag := atomic.LoadInt32(&kv.isKilled)
	return flag == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// The k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. If maxraftstate is -1,
// there will be no snapshotting.
// StartKVServer() must return quickly, so it starts goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	gob.Register(Op{})

	kv := new(KVServer)
	kv.me = me

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.maxraftstate = maxraftstate

	kv.kvStore = make(map[string]string)
	kv.applied = make(map[int]chan Op)
	kv.lstProc = make(map[int64]uint64)

	//
	// The applier loop.
	// This is the only part that touches the state, i.e, the key-value map.
	//
	go func() {
		for kv.killed() == false {
			m := <-kv.applyCh
			if m.SnapshotValid {
				if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
					r := bytes.NewBuffer(m.Snapshot)
					d := gob.NewDecoder(r)
					var lastIncludedIndex int
					d.Decode(&lastIncludedIndex)
					kv.mu.Lock()
					kv.kvStore = make(map[string]string)
					kv.lstProc = make(map[int64]uint64)
					d.Decode(&kv.kvStore)
					d.Decode(&kv.lstProc)
					kv.mu.Unlock()
				}
			} else if m.CommandValid {
				op := m.Command.(Op)
				kv.mu.Lock()
				if op.SerialNr <= kv.lstProc[op.ClientId] {
					kv.mu.Unlock()
					continue
				}
				switch op.Type {
				case "PUT":
					kv.kvStore[op.Key] = op.Value
				case "APPEND":
					kv.kvStore[op.Key] += op.Value
				}
				kv.lstProc[op.ClientId] = op.SerialNr
				if ch, ok := kv.applied[m.CommandIndex]; ok {
					ch <- op
				}
				kv.mu.Unlock()
				if kv.maxraftstate != -1 &&
					persister.RaftStateSize() > kv.maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(m.CommandIndex)
					kv.mu.Lock()
					e.Encode(kv.kvStore)
					e.Encode(kv.lstProc)
					kv.mu.Unlock()
					kv.rf.Snapshot(m.CommandIndex, w.Bytes())
				}
			}
		}
	}()

	return kv
}
