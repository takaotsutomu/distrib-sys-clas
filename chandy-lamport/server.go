package chandy_lamport

import (
	"log"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	Snapshots     map[int]*Snapshot
	numSnapInP    int // num of snapshots in progress
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

type Snapshot struct {
	ServerId   string
	Tokens     int
	Messages   []*SnapshotMessage
	recdMarker map[string]bool // key = link.src
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]*Snapshot),
		0,
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	switch message := message.(type) {
	case TokenMessage:
		// For all not yet completed snapshots
		if server.numSnapInP > 0 {
			for _, snap := range server.Snapshots {
				if !snap.recdMarker[src] {
					snap.Messages = append(snap.Messages,
						&SnapshotMessage{src, server.Id, message})
				}
			}
		}
		server.Tokens += message.numTokens
	case MarkerMessage:
		id := message.snapshotId
		numInb := len(server.inboundLinks)
		if server.Snapshots[id] != nil {
			server.Snapshots[id].recdMarker[src] = true
			if len(server.Snapshots[id].recdMarker) == numInb {
				server.sim.NotifySnapshotComplete(server.Id, id)
				server.numSnapInP--
			}
			return
		}
		server.StartSnapshot(id)
		server.Snapshots[id].recdMarker[src] = true

		// If there is only 1 interface to recieve a
		// marker message from, end the snapshot process
		if len(server.Snapshots[id].recdMarker) == numInb {
			server.sim.NotifySnapshotComplete(server.Id, id)
			server.numSnapInP--
		}
	default:
		log.Fatal("Error unknown message: ", message)
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// Record local state and send marker messages
	server.Snapshots[snapshotId] = &Snapshot{
		server.Id,
		server.Tokens,
		make([]*SnapshotMessage, 0),
		make(map[string]bool),
	}
	server.SendToNeighbors(MarkerMessage{snapshotId})
	server.numSnapInP++
}
