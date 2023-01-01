package shardctrler

import (
	"encoding/gob"
	"lab5/labrpc"
	"lab5/raft"
	"sort"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	applied map[int]chan Op
	lstProc map[int64]uint64
}

type Op struct {
	Args interface{}

	// To ensure exactly-once semantics
	SerialNr uint64
	ClientId int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	if args.SerialNr <= sc.lstProc[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(
		Op{
			Args:     *args,
			SerialNr: args.SerialNr,
			ClientId: args.ClientId,
		})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan Op)
	sc.mu.Lock()
	sc.applied[index] = ch
	sc.mu.Unlock()
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
	sc.mu.Lock()
	close(ch)
	delete(sc.applied, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	if args.SerialNr <= sc.lstProc[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(
		Op{
			Args:     *args,
			SerialNr: args.SerialNr,
			ClientId: args.ClientId,
		})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan Op)
	sc.mu.Lock()
	sc.applied[index] = ch
	sc.mu.Unlock()
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
	sc.mu.Lock()
	close(ch)
	delete(sc.applied, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()
	if args.SerialNr <= sc.lstProc[args.ClientId] {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(
		Op{
			Args:     *args,
			SerialNr: args.SerialNr,
			ClientId: args.ClientId,
		})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan Op)
	sc.mu.Lock()
	sc.applied[index] = ch
	sc.mu.Unlock()
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
	sc.mu.Lock()
	close(ch)
	delete(sc.applied, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	if args.SerialNr <= sc.lstProc[args.ClientId] {
		if args.Num == -1 || args.Num >= len(sc.configs) {
			args.Num = len(sc.configs) - 1
		}
		reply.Config = sc.configs[args.Num]
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(
		Op{
			Args:     *args,
			SerialNr: args.SerialNr,
			ClientId: args.ClientId,
		})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	ch := make(chan Op)
	sc.mu.Lock()
	sc.applied[index] = ch
	sc.mu.Unlock()
	select {
	case op := <-ch:
		if op.ClientId != args.ClientId ||
			op.SerialNr != args.SerialNr {
			reply.Err = ErrOpFailed
		} else {
			sc.mu.Lock()
			num := op.Args.(QueryArgs).Num
			if num == -1 || num >= len(sc.configs) {
				num = len(sc.configs) - 1
			}
			reply.Config = sc.configs[num]
			sc.mu.Unlock()
			reply.Err = OK
		}
	case <-time.After(1 * time.Second):
		reply.Err = ErrOpFailed
	}
	sc.mu.Lock()
	close(ch)
	delete(sc.applied, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// Needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.applied = make(map[int]chan Op)
	sc.lstProc = make(map[int64]uint64)

	//
	// The applier loop.
	// This is the only part that touches the state, i.e, the configuration.
	//
	go func() {
		for {
			m := <-sc.applyCh
			if m.CommandValid {
				op := m.Command.(Op)
				sc.mu.Lock()
				if op.SerialNr <= sc.lstProc[op.ClientId] {
					sc.mu.Unlock()
					continue
				}
				switch args := op.Args.(type) {
				case JoinArgs:
					// Get current configuration
					currConfig := sc.configs[len(sc.configs)-1]

					//
					// Construct next configuration with next config #.
					// Start first with the replica group set.
					//
					nextConfig := Config{Num: currConfig.Num + 1}
					nextConfig.Groups = map[int][]string{}
					for gid, srvGp := range currConfig.Groups {
						nextConfig.Groups[gid] = make([]string, len(srvGp))
						copy(nextConfig.Groups[gid], srvGp)
					}
					joinGIDs := []int{} // for later use in shard assingment

					// Add new servers to the replica groups identified by gids
					for gid, srvGp := range args.Servers {
						joinGIDs = append(joinGIDs, gid)
						nextConfig.Groups[gid] =
							append(nextConfig.Groups[gid], srvGp...)

					}

					// Re-assign shards each group of server is responsible for
					// to re-balance the load
					nextConfig.Shards = [NShards]int{}
					if len(nextConfig.Groups) > 0 &&
						len(nextConfig.Groups) > len(currConfig.Groups) {
						// Evenly assign the shards across the newly formed
						// replica groups
						if len(currConfig.Groups) == 0 {
							i := 0
							nGp := len(nextConfig.Groups)
							for shard := range currConfig.Shards {
								nextConfig.Shards[shard] = joinGIDs[i]
								i = (i + 1) % nGp
							}
						} else {
							gids := []int{}
							for gid := range nextConfig.Groups {
								gids = append(gids, gid)
							}
							sort.Ints(gids)

							minLoad := NShards / len(nextConfig.Groups)
							rShards := []int{}
							load := map[int]int{} // gid -> nShards
							for shard, gid := range currConfig.Shards {
								if load[gid] >= minLoad {
									rShards = append(rShards, shard)
									continue
								}
								nextConfig.Shards[shard] = gid
								load[gid]++
							}
							sort.Ints(rShards)

							i := 0 // to index gids
							j := 0 // to index rShards
							for _, shard := range rShards {
								if load[joinGIDs[i]] >= minLoad {
									break
								}
								nextConfig.Shards[shard] = joinGIDs[i]
								load[joinGIDs[i]]++
								i = (i + 1) % len(joinGIDs)
								j++
							}

							i = 0
							for ; j < len(rShards); j++ {
								nextConfig.Shards[rShards[j]] = gids[i]
								i = (i + 1) % len(gids)
							}
						}
					}
					sc.configs = append(sc.configs, nextConfig)
				case LeaveArgs:
					// Get current configuration
					currConfig := sc.configs[len(sc.configs)-1]

					//
					// Construct next configuration with next config #.
					// Start first with the replica group set.
					//
					nextConfig := Config{Num: currConfig.Num + 1}
					nextConfig.Groups = map[int][]string{}
					for gid, srvGp := range currConfig.Groups {
						nextConfig.Groups[gid] = make([]string, len(srvGp))
						copy(nextConfig.Groups[gid], srvGp)
					}

					// Delete the replica group identified by gid from the set
					for _, gid := range args.GIDs {
						delete(nextConfig.Groups, gid)
					}

					// Re-assign shards each group of server is responsible for
					// to rebalance the load
					nextConfig.Shards = [NShards]int{}
					if len(nextConfig.Groups) > 0 {
						// Evenly assign the shards the leaving replica groups was
						// respoinsible for to the remaining groups
						rShards := []int{} // to collect re-assigning shards
						for shard, gid := range currConfig.Shards {
							if _, ok := nextConfig.Groups[gid]; !ok {
								rShards = append(rShards, shard)
								continue
							}
							nextConfig.Shards[shard] = gid
						}
						sort.Ints(rShards)

						rmnGIDs := []int{} // to collect remaining gids
						for gid := range nextConfig.Groups {
							rmnGIDs = append(rmnGIDs, gid)
						}
						sort.Ints(rmnGIDs)

						i := 0
						nGp := len(nextConfig.Groups)
						for _, shard := range rShards {
							nextConfig.Shards[shard] = rmnGIDs[i]
							i = (i + 1) % nGp
						}
					}
					sc.configs = append(sc.configs, nextConfig)
				case MoveArgs:
					// Get current configuration
					currConfig := sc.configs[len(sc.configs)-1]

					// Construct next configuration with next config no.
					nextConfig := Config{Num: currConfig.Num + 1}
					nextConfig.Shards = [NShards]int{}
					copy(nextConfig.Shards[:], currConfig.Shards[:])

					// Update shard -> gid assignment
					nextConfig.Shards[args.Shard] = args.GID
					nextConfig.Groups = make(map[int][]string)
					for gid, srvgp := range currConfig.Groups {
						copy(nextConfig.Groups[gid], srvgp)
					}
					sc.configs = append(sc.configs, nextConfig)
				}
				sc.lstProc[op.ClientId] = op.SerialNr
				if ch, ok := sc.applied[m.CommandIndex]; ok {
					ch <- op
				}
				sc.mu.Unlock()
			}
		}
	}()

	return sc
}
