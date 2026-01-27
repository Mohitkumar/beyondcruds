package broker

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/mohitkumar/mlog/api/metadata"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type MetadataRaft struct {
	node    raft.Node
	storage *raft.MemoryStorage
	applyCh chan *metadata.MetadataEvent
}

// sendRaftRPC is a placeholder for sending raft messages to peers.
// TODO: implement transport (gRPC/http) and peer routing.
func sendRaftRPC(msg raftpb.Message) {
	_ = msg
}

func StartMetadataRaft(id uint64, peers []raft.Peer) *MetadataRaft {
	storage := raft.NewMemoryStorage()

	cfg := &raft.Config{
		ID:            id,
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       storage,
	}

	node := raft.StartNode(cfg, peers)

	mr := &MetadataRaft{
		node:    node,
		storage: storage,
		applyCh: make(chan *metadata.MetadataEvent, 1024),
	}

	go mr.loop()
	return mr
}

func (m *MetadataRaft) loop() {
	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			m.node.Tick()

		case rd := <-m.node.Ready():
			m.storage.Append(rd.Entries)

			for _, msg := range rd.Messages {
				sendRaftRPC(msg)
			}

			for _, ent := range rd.CommittedEntries {
				if ent.Type != raftpb.EntryNormal {
					continue
				}
				ev := &metadata.MetadataEvent{}
				proto.Unmarshal(ent.Data, ev)
				m.applyCh <- ev
			}
			m.node.Advance()
		}
	}
}

func (m *MetadataRaft) Propose(ev *metadata.MetadataEvent) error {
	data, _ := proto.Marshal(ev)
	return m.node.Propose(context.Background(), data)
}
