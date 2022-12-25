package shardkv

import (
	"bytes"
	"encoding/gob"
	"lab5/labrpc"
	"lab5/raft"
	"lab5/shardctrler"
	"sync"
	"time"
)

const (
	fetchCfgIntv   = 50
	pullShardsIntv = 50
	garbColIntv    = 100
)

type Op struct {
	Type  string
	Key   string
	Value string

	// To ensure exactly-once semantics
	SerialNr uint64
	ClientId int64
}

type Res struct {
	Err Err

	// For normal operations
	Value    string
	SerialNr uint64
	ClientId int64

	// For garbage collection
	CfgNr int
	Shard int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	mck          *shardctrler.Clerk
	cfg          shardctrler.Config
	maxraftstate int // snapshot if log grows this big

	kvStore map[string]string
	applied map[int]chan Res
	lstProc map[int64]uint64

	srvShards   map[int]bool
	incomShards map[int]int
	outgoShards map[int]map[int]map[string]string // cfgNr -> (shard -> kvData)
	garbColShds map[int]map[int]bool              // cfgNr -> (shard -> bool)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
	ch := make(chan Res)
	kv.mu.Lock()
	kv.applied[index] = ch
	kv.mu.Unlock()
	select {
	case res := <-ch:
		if res.ClientId != args.ClientId ||
			res.SerialNr != args.SerialNr {
			reply.Err = ErrOpFailed
		} else {
			reply.Value = res.Value
			reply.Err = res.Err
		}
	case <-time.After(1 * time.Second):
		reply.Err = ErrOpFailed
	}
	close(ch)
	kv.mu.Lock()
	delete(kv.applied, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	ch := make(chan Res)
	kv.mu.Lock()
	kv.applied[index] = ch
	kv.mu.Unlock()
	select {
	case res := <-ch:
		if res.ClientId != args.ClientId ||
			res.SerialNr != args.SerialNr {
			reply.Err = ErrOpFailed
		} else {
			reply.Err = res.Err
		}
	case <-time.After(1 * time.Second):
		reply.Err = ErrOpFailed
	}
	close(ch)
	kv.mu.Lock()
	delete(kv.applied, index)
	kv.mu.Unlock()
}

type MigrateShardArgs struct {
	CfgNr int
	Shard int
}

type MigrateShardReply struct {
	Err    Err
	CfgNr  int
	Shard  int
	KVData map[string]string
	LstPrc map[int64]uint64
}

type Shard MigrateShardReply

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.CfgNr >= kv.cfg.Num {
		reply.Err = ErrOpFailed
		return
	}
	reply.KVData = map[string]string{}
	for k, v := range kv.outgoShards[args.CfgNr][args.Shard] {
		reply.KVData[k] = v
	}
	reply.LstPrc = map[int64]uint64{}
	for k, v := range kv.lstProc {
		reply.LstPrc[k] = v
	}
	reply.CfgNr = args.CfgNr
	reply.Shard = args.Shard
	reply.Err = OK
}

type GarbageCollectionArgs struct {
	CfgNr int
	Shard int
}

type GarbageCollectionReply struct {
	Err Err
}

func (kv *ShardKV) GarbageCollection(args *GarbageCollectionArgs, reply *GarbageCollectionReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if _, ok := kv.outgoShards[args.CfgNr]; !ok {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	if _, ok := kv.outgoShards[args.CfgNr][args.Shard]; !ok {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan Res)
	kv.mu.Lock()
	kv.applied[index] = ch
	kv.mu.Unlock()
	select {
	case res := <-ch:
		if res.CfgNr != args.CfgNr ||
			res.Shard != args.Shard {
			reply.Err = ErrOpFailed
		} else {
			reply.Err = res.Err
		}
	case <-time.After(1 * time.Second):
		reply.Err = ErrOpFailed
	}
	close(ch)
	kv.mu.Lock()
	delete(kv.applied, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// The k/v server stores snapshots through the underlying Raft
// implementation, which calls persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// The k/v server snapshots when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. If maxraftstate is -1, no snapshot will be taken.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// ctrlers[] is passed to shardctrler.MakeClerk() so RPCs can be
// sent to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which RPCs
// are sent.
//
// StartServer() must return quickly, so it starts goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	gob.Register(Op{})
	gob.Register(GarbageCollectionArgs{})
	gob.Register(MigrateShardReply{})
	gob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.kvStore = make(map[string]string)
	kv.applied = make(map[int]chan Res)
	kv.lstProc = make(map[int64]uint64)

	kv.mck = shardctrler.MakeClerk(ctrlers)
	kv.cfg = shardctrler.Config{Num: 0}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.srvShards = make(map[int]bool)
	kv.incomShards = make(map[int]int)
	kv.outgoShards = make(map[int]map[int]map[string]string)
	kv.garbColShds = make(map[int]map[int]bool)

	//
	// The applier loop.
	// This is the only part that touches the state, i.e, the key-value map.
	//
	go func() {
		for {
			m := <-kv.applyCh
			if m.SnapshotValid {
				if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
					r := bytes.NewBuffer(m.Snapshot)
					d := gob.NewDecoder(r)
					var lastIncludedIndex int
					d.Decode(&lastIncludedIndex)
					kv.mu.Lock()
					d.Decode(&kv.cfg)
					kv.kvStore = map[string]string{}
					kv.lstProc = map[int64]uint64{}
					kv.srvShards = map[int]bool{}
					kv.incomShards = map[int]int{}
					kv.outgoShards =
						map[int]map[int]map[string]string{}
					kv.garbColShds = map[int]map[int]bool{}
					d.Decode(&kv.kvStore)
					d.Decode(&kv.lstProc)
					d.Decode(&kv.srvShards)
					d.Decode(&kv.incomShards)
					d.Decode(&kv.outgoShards)
					d.Decode(&kv.garbColShds)
					kv.mu.Unlock()
				}
			} else if m.CommandValid {
				switch ent := m.Command.(type) {
				// There is a config change happening
				case shardctrler.Config:
					kv.mu.Lock()
					if ent.Num > kv.cfg.Num {
						if kv.cfg.Num == 0 {
							for shard, gid := range ent.Shards {
								if gid == kv.gid {
									kv.srvShards[shard] = true
								}
							}
						} else {
							// Based on the new config, figure out the next
							// servering shards, and determine the shards to
							// migrate to the replica groups taking over
							// ownership (outgoShards) and the shards to request
							// from the replica groups currently owning them
							// (incomShards)
							curSrvShards := kv.srvShards
							kv.srvShards = map[int]bool{}
							for shard, gid := range ent.Shards {
								if gid != kv.gid {
									continue
								}
								if !curSrvShards[shard] {
									kv.incomShards[shard] = kv.cfg.Num
									continue
								}
								delete(curSrvShards, shard)
								kv.srvShards[shard] = true
							}
							if len(curSrvShards) > 0 {
								kv.outgoShards[kv.cfg.Num] =
									map[int]map[string]string{}
								for shard := range curSrvShards {
									outgoData := map[string]string{}
									for k, v := range kv.kvStore {
										if key2shard(k) == shard {
											outgoData[k] = v
											delete(kv.kvStore, k)
										}
									}
									kv.outgoShards[kv.cfg.Num][shard] = outgoData
								}
							}
						}
						kv.cfg = ent
					}
					kv.mu.Unlock()
				// There is a shard being handed over
				case MigrateShardReply:
					kv.mu.Lock()
					if ent.CfgNr == kv.cfg.Num-1 &&
						!kv.srvShards[ent.Shard] {
						for k, v := range ent.KVData {
							kv.kvStore[k] = v
						}
						for k, v := range ent.LstPrc {
							if v > kv.lstProc[k] {
								kv.lstProc[k] = v
							}
						}
						kv.srvShards[ent.Shard] = true
						delete(kv.incomShards, ent.Shard)

						// Mark the gained shard as one that needed to be GCed
						if _, ok := kv.garbColShds[ent.CfgNr]; !ok {
							kv.garbColShds[ent.CfgNr] = map[int]bool{}
						}
						kv.garbColShds[ent.CfgNr][ent.Shard] = true
					}
					kv.mu.Unlock()
				// There is a shard already handed off to be garbage collected
				case GarbageCollectionArgs:
					kv.mu.Lock()
					if _, ok := kv.outgoShards[ent.CfgNr]; ok {
						delete(kv.outgoShards[ent.CfgNr], ent.Shard)
						if len(kv.outgoShards[ent.CfgNr]) == 0 {
							delete(kv.outgoShards, ent.CfgNr)
						}
					}
					if ch, ok := kv.applied[m.CommandIndex]; ok {
						ch <- Res{Err: OK, CfgNr: ent.CfgNr, Shard: ent.Shard}
					}
					kv.mu.Unlock()
				case Op:
					kv.mu.Lock()
					if ent.SerialNr <= kv.lstProc[ent.ClientId] {
						kv.mu.Unlock()
						continue
					}
					res := Res{SerialNr: ent.SerialNr, ClientId: ent.ClientId}
					if !kv.srvShards[key2shard(ent.Key)] {
						res.Err = ErrWrongGroup
					} else {
						switch ent.Type {
						case "GET":
							res.Value = kv.kvStore[ent.Key]
							res.Err = OK
						case "PUT":
							kv.kvStore[ent.Key] = ent.Value
							res.Err = OK
						case "APPEND":
							kv.kvStore[ent.Key] += ent.Value
							res.Err = OK
						}
						kv.lstProc[ent.ClientId] = ent.SerialNr
					}
					if ch, ok := kv.applied[m.CommandIndex]; ok {
						ch <- res
					}
					kv.mu.Unlock()
				}
				if kv.maxraftstate != -1 &&
					persister.RaftStateSize() > kv.maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(m.CommandIndex)
					kv.mu.Lock()
					e.Encode(kv.cfg)
					e.Encode(kv.kvStore)
					e.Encode(kv.lstProc)
					e.Encode(kv.srvShards)
					e.Encode(kv.incomShards)
					e.Encode(kv.outgoShards)
					e.Encode(kv.garbColShds)
					kv.mu.Unlock()
					kv.rf.Snapshot(m.CommandIndex, w.Bytes())
				}
			}
		}
	}()

	// Periodically poll the shardctrler to learn about new configs
	go func() {
		for {
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			// Check len(kv.incomShards) == 0 to make sure that
			// the server polls the shardctrler for new configs (if any)
			// after shard migration for current config is done
			if isLeader && len(kv.incomShards) == 0 {
				cfgNr := kv.cfg.Num + 1
				kv.mu.Unlock()
				cfg := kv.mck.Query(cfgNr)
				kv.mu.Lock()
				if cfg.Num > kv.cfg.Num {
					kv.mu.Unlock()
					kv.rf.Start(cfg)
					kv.mu.Lock()
				}
			}
			kv.mu.Unlock()
			time.Sleep(time.Duration(fetchCfgIntv) * time.Millisecond)
		}
	}()

	// Request shards needed from the replica groups that are
	// currently holding the shards during config changes, if any.
	go func() {
		for {
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if isLeader && len(kv.incomShards) > 0 {
				var wg sync.WaitGroup
				for shard, cfgNr := range kv.incomShards {
					cfg := kv.mck.Query(cfgNr)
					wg.Add(1)
					go func(cfgNr, shard int) {
						args := MigrateShardArgs{
							CfgNr: cfgNr,
							Shard: shard,
						}
						gid := cfg.Shards[shard]
						for _, server := range cfg.Groups[gid] {
							srv := kv.make_end(server)
							var reply MigrateShardReply
							ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
							if ok && reply.Err == OK {
								kv.rf.Start(reply)
								break
							}
						}
						wg.Done()
					}(cfgNr, shard)
				}
				kv.mu.Unlock()
				wg.Wait()
				kv.mu.Lock()
			}
			kv.mu.Unlock()
			time.Sleep(time.Duration(pullShardsIntv) * time.Millisecond)
		}
	}()

	// For each shard that has been migrated during config changes,
	// request the replica group from which the shard came
	// to garbage collect the shard from their outgoShards'
	go func() {
		for {
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if isLeader && len(kv.garbColShds) > 0 {
				var wg sync.WaitGroup
				for cfgNr, shards := range kv.garbColShds {
					cfg := kv.mck.Query(cfgNr)
					for shard := range shards {
						wg.Add(1)
						go func(cfgNr, shard int) {
							args := GarbageCollectionArgs{
								CfgNr: cfgNr,
								Shard: shard,
							}
							gid := cfg.Shards[shard]
							for _, server := range cfg.Groups[gid] {
								srv := kv.make_end(server)
								var reply MigrateShardReply
								ok := srv.Call("ShardKV.GarbageCollection", &args, &reply)
								if ok && reply.Err == OK {
									kv.mu.Lock()
									delete(kv.garbColShds[cfgNr], shard)
									if len(kv.garbColShds[cfgNr]) == 0 {
										delete(kv.garbColShds, cfgNr)
									}
									kv.mu.Unlock()
									break
								}
							}
							wg.Done()
						}(cfgNr, shard)
					}
				}
				kv.mu.Unlock()
				wg.Wait()
				kv.mu.Lock()
			}
			kv.mu.Unlock()
			time.Sleep(time.Duration(pullShardsIntv) * time.Millisecond)
		}
	}()

	return kv
}
