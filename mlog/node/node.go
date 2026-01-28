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
	NodeID            string
	LastFetchTime     time.Time
	LastFetchedOffset uint64
	IsISR             bool
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
	n.mu.Unlock()

	go n.startReplication()
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
func (n *Node) startReplication() {
	ctx, cancel := context.WithCancel(context.Background())
	n.mu.Lock()
	n.cancelFn = cancel
	stopCh := n.stopChan
	n.mu.Unlock()

	reconnectDelay := time.Second

	for {
		select {
		case <-stopCh:
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
			fmt.Printf("replica %s: failed to create replication stream: %v, retrying in %v", n.ReplicaID, err, reconnectDelay)
			time.Sleep(reconnectDelay)
			reconnectDelay = min(reconnectDelay*2, 30*time.Second) // Exponential backoff, max 30s
			continue
		}

		// Reset reconnect delay on successful connection
		reconnectDelay = time.Second

		// Process streamed log entries
		for {
			select {
			case <-stopCh:
				return
			default:
			}

			resp, err := stream.Recv()
			if err != nil {
				fmt.Printf("replica %s: stream receive error: %v, reconnecting", n.ReplicaID, err)
				break // Break inner loop to reconnect
			}

			if len(resp.Entries) == 0 {
				continue
			}

			expectedOffset := n.Log.LEO()
			var appliedAny bool
			var maxApplied uint64

			for _, entry := range resp.Entries {
				// Drop duplicates/old entries.
				if entry.Offset < expectedOffset {
					continue
				}
				// Enforce contiguous apply to avoid gaps.
				if entry.Offset != expectedOffset {
					// Gap detected; reconnect starting from our current LEO.
					break
				}

				off, appendErr := n.Log.Append(&common.LogEntry{Value: entry.Value})
				if appendErr != nil {
					fmt.Printf("replica %s: failed to append log entry at expected offset %d: %v", n.ReplicaID, expectedOffset, appendErr)
					// Stop processing this batch; reconnect and retry.
					break
				}
				// Defensive check: LogManager.Append should return the offset it wrote.
				if off != expectedOffset {
					fmt.Printf("replica %s: append returned offset %d, expected %d; reconnecting", n.ReplicaID, off, expectedOffset)
					break
				}

				appliedAny = true
				maxApplied = off
				expectedOffset++
			}

			if !appliedAny {
				// Nothing applied; reconnect to resync.
				break
			}

			currentLEO := maxApplied + 1

			// Report LEO to leader
			err = n.reportLEO(ctx, currentLEO)
			if err != nil {
				fmt.Printf("replica %s: failed to report LEO %d to leader: %v", n.ReplicaID, currentLEO, err)
				// Continue replication even if report fails
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

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
