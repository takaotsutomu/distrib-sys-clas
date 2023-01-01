# Fault-Tolerant Key/Value Store *Sharded*
Fault-Tolerant key/value storage system that shards the keys over a set of replica groups. A shard is a subset of the key/value pairs. Each replica group handles puts and gets for just a few of the shards, and the groups operate in parallel. With sharding total system throughput (puts/gets per unit time) increases in proportion to the number of groups.

The system consists of two main components:
* A set of replica groups, each responsible for a subset of shards
* The shard controller 

A replica group consists of a handful of servers that use the [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) consensus protocol to replicate the group's shards. The shard controller decides which replica group shoud server each shard. This information is called the configuration and it changes over time. Clients consult the shard controller in order to find the replica group for a key, and replica groups consult the controller in order to find out what shards to serve. There is a single shard controller for the whole system, implemented as a fault-tolerant service using Raft as well.

The system is able to shift shards among replica groups to balance the load, and rebalance the load (i.e., reassign the shards) when replica groups join and leave the system--new replica group may be added to increase capacity, or existing replica groups may be taken offline. The shard controller rebalances the load and the replica groups hand over the data for shards being reassigned to each other during configuration changes.

This general architecture follows the same general pattern as Flat Datacenter Storage, BigTable, Spanner, FAWN, Apache HBase, Rosebud, Spinnaker, and many others, although these systems differ in many details from the one in this project, and are also typically more sophisticated and capable. After all, this is an individual-effort project. 

The architecture of the system (when the number of shards $N=2$) is shown as follows.
![shardkv_arch](https://github.com/takaotsutomu/distrib-sys-clas/blob/master/shardkv/shardkv_arch.png?raw=true)


## RPC Interface
The shard controller supports the following RPCs intended to allow an administrator to control through a `Clerk` with `Join`/`Leave`/`Move` methods, and allow the key/value servers to fetch the lastest configuration through their `Clerk`s with `Query` method, in which the `Clerk`s manage RPC interactions with the servers. 
* `Join(servers: map[int][]string)`: add new replica groups
* `Leave(gids: int[])`: eliminate replica groups
* `Move(shard: int, gid: int)`: move shards between replica groups
* `Query(num: int)`: get configuration # num, latest one if $num=-1$

The interface to the key/value service stays the same as the one in the previous project (key/value store w/o sharding), which exposes the following RPCs to the client and provides linearizability guarantee. 
* `Put(key: string, value: string)`
* `Append(key: string, arg: string)`
* `Get(key: string)`

Keys and values are strings. `Put(key, value)` replaces the value for a particular key in the database, `Append(key, arg)` appends args to the key's value, and `Get(key)` fetches the current value for the key. A `Get` for a non-existent key returns an empty string. An `Append` to a non-existent key act just like `Put`. Each client communicates with the service through a `Clerk` with `Put`/`Append`/`Get` methods, in which the `Clerk` manages RPC interactions with the servers. 

```
Test: Basic leave/join ...
  ... Passed
Test: Historical queries ...
  ... Passed
Test: Move ...
  ... Passed
Test: Concurrent leave/join ...
  ... Passed
Test: Minimal transfers after joins ...
  ... Passed
Test: Minimal transfers after leaves ...
  ... Passed
Test: Multi-group join/leave ...
  ... Passed
Test: Concurrent multi leave/join ...
  ... Passed
Test: Minimal transfers after multijoins ...
  ... Passed
Test: Minimal transfers after multileaves ...
  ... Passed
Test: Check Same config on servers ...
  ... Passed
PASS
ok      lab5/shardctrler        2.721s

```
```
Test: static shards ...
  ... Passed
Test: join then leave ...
  ... Passed
Test: snapshots, join, and leave ...
  ... Passed
Test: servers miss configuration changes...
  ... Passed
Test: concurrent puts and configuration changes...
  ... Passed
Test: more concurrent puts and configuration changes...
  ... Passed
Test: concurrent configuration change and restart...
  ... Passed
Test: unreliable 1...
  ... Passed
Test: unreliable 2...
  ... Passed
Test: unreliable 3...
  ... Passed
Test: shard deletion (challenge 1) ...
  ... Passed
Test: unaffected shard access (challenge 2) ...
  ... Passed
Test: partial migration shard access (challenge 2) ...
  ... Passed
PASS
ok      lab5/shardkv    110.951s
```