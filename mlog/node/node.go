package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/api/common"
	"github.com/mohitkumar/mlog/api/leader"
	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/log"
)

// Node represents a broker node that can be either a leader or replica for a topic
type Node struct {
	mu            sync.RWMutex
	Topic         string
	Log           *log.LogManager
	broker        *broker.Broker
	NodeID        string
	Addr          string
	ReplicaID     string // Only set for replicas
	IsLeader      bool
	brokerManager *broker.BrokerManager

	// Leader-specific fields
	followers map[string]*FollowerState

	// Replica-specific fields
	leaderAddr   string
	leaderClient leader.LeaderServiceClient
	stopChan     chan struct{}
	stopOnce     sync.Once
	cancelFn     context.CancelFunc
}

// FollowerState tracks the state of a follower/replica
type FollowerState struct {
	NodeID        string
	LastFetchTime time.Time
	LEO           uint64
	IsISR         bool
}

// NewLeaderNode creates a new node that acts as a leader
func NewLeaderNode(topic string, log *log.LogManager, broker *broker.Broker, brokerManager *broker.BrokerManager) *Node {
	return &Node{
		Topic:         topic,
		Log:           log,
		broker:        broker,
		NodeID:        broker.NodeID,
		Addr:          broker.Addr,
		IsLeader:      true,
		brokerManager: brokerManager,
		followers:     make(map[string]*FollowerState),
	}
}

// NewReplicaNode creates a new node that acts as a replica
func NewReplicaNode(topic string, replicaID string, log *log.LogManager, broker *broker.Broker, leaderAddr string, brokerManager *broker.BrokerManager) *Node {
	return &Node{
		Topic:         topic,
		Log:           log,
		broker:        broker,
		NodeID:        broker.NodeID,
		Addr:          broker.Addr,
		ReplicaID:     replicaID,
		IsLeader:      false,
		leaderAddr:    leaderAddr,
		brokerManager: brokerManager,
	}
}

func (n *Node) NewFollwerState(replicaID string, nodeId string) {
	n.mu.Lock()
	n.followers[replicaID] = &FollowerState{
		NodeID:        nodeId,
		LastFetchTime: time.Now(),
		LEO:           0,
		IsISR:         false,
	}
	n.mu.Unlock()
}

// StartReplication starts the replication process (only for replica nodes)
func (n *Node) StartReplication() error {
	if n.IsLeader {
		return fmt.Errorf("cannot start replication on leader node")
	}
	if n.leaderAddr == "" {
		return fmt.Errorf("leader address not set for replica %s", n.ReplicaID)
	}

	// Create leader client if not already created
	if n.leaderClient == nil {
		if n.brokerManager == nil {
			return fmt.Errorf("broker manager not set for replica %s", n.ReplicaID)
		}

		// Find leader broker by address
		leaderBroker := n.brokerManager.GetBrokerByAddr(n.leaderAddr)
		if leaderBroker == nil {
			return fmt.Errorf("leader broker not found at address %s", n.leaderAddr)
		}

		// Get connection from broker manager
		conn, err := leaderBroker.GetConn()
		if err != nil {
			return fmt.Errorf("failed to connect to leader at %s: %w", n.leaderAddr, err)
		}
		n.leaderClient = leader.NewLeaderServiceClient(conn)
	}

	n.mu.Lock()
	if n.stopChan != nil {
		n.mu.Unlock()
		return fmt.Errorf("replication already started for replica %s", n.ReplicaID)
	}
	n.stopChan = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	n.cancelFn = cancel
	n.mu.Unlock()
	go n.startReplication(ctx)
	return nil
}

// StopReplication stops the replication process
func (n *Node) StopReplication() {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		if n.cancelFn != nil {
			n.cancelFn()
			n.cancelFn = nil
		}
		ch := n.stopChan
		n.stopChan = nil
		n.mu.Unlock()

		if ch != nil {
			close(ch)
		}
	})
}

// startReplication is the main replication loop for replica nodes
func (n *Node) startReplication(ctx context.Context) {
	backoff := 50 * time.Millisecond
	maxBackoff := 1 * time.Second

	for {
		select {
		case <-n.stopChan:
			return
		case <-ctx.Done():
			return
		default:
		}
		// Start replicating from current LEO (next offset to apply).
		currentOffset := n.Log.LEO()

		// Create replication request
		req := &leader.ReplicateRequest{
			Topic:     n.Topic,
			Offset:    currentOffset,
			BatchSize: 1000,
		}

		// Create stream to leader
		stream, err := n.leaderClient.ReplicateStream(ctx, req)
		if err != nil {
			// Backoff on connection errors
			time.Sleep(backoff)
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}
		// Reset backoff on successful connection
		backoff = 50 * time.Millisecond

		// Process stream messages
		for {
			select {
			case <-n.stopChan:
				return
			case <-ctx.Done():
				return
			default:
			}

			resp, err := stream.Recv()
			if err != nil {
				// Stream error, reconnect
				break
			}

			if len(resp.Entries) == 0 {
				// No entries in this batch, continue waiting
				continue
			}

			// If we detect a gap or write error, we must reconnect starting from our current LEO.
			// Otherwise the leader stream (which advances its own cursor) may move past what we've applied,
			// causing permanent gaps and no further progress.
			reconnectNeeded := false
			for _, entry := range resp.Entries {
				expectedOffset := n.Log.LEO()
				// Drop duplicates/old entries.
				if entry.Offset < expectedOffset {
					continue
				}
				// Enforce contiguous apply to avoid gaps.
				if entry.Offset != expectedOffset {
					// Gap detected; reconnect starting from our current LEO.
					reconnectNeeded = true
					break
				}

				_, appendErr := n.Log.Append(&common.LogEntry{Value: entry.Value})
				if appendErr != nil {
					fmt.Printf("replica %s: failed to append log entry at expected offset %d: %v", n.ReplicaID, expectedOffset, appendErr)
					// Stop processing this batch; reconnect and retry.
					reconnectNeeded = true
					break
				}
			}

			// Report LEO once per batch. This avoids spawning goroutines per entry and
			// keeps leader-side ACK_ALL progress updates frequent enough (batch size up to 1000).
			_ = n.reportLEO(ctx, n.Log.LEO())

			if reconnectNeeded {
				// Break out of the stream loop so the outer loop can reconnect from our current LEO.
				break
			}
		}
	}
}

// reportLEO reports the current Log End Offset (LEO) to the leader
func (n *Node) reportLEO(ctx context.Context, leo uint64) error {
	if n.leaderClient == nil {
		return fmt.Errorf("leader client not initialized")
	}

	req := &leader.RecordLEORequest{
		Topic:     n.Topic,
		ReplicaId: n.ReplicaID,
		Leo:       int64(leo),
	}

	_, err := n.leaderClient.RecordLEO(ctx, req)
	return err
}
