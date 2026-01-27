package replication

import (
	"context"
	"fmt"

	"time"

	"github.com/mohitkumar/mlog/api/common"
	"github.com/mohitkumar/mlog/api/leader"
	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/log"
	"google.golang.org/grpc"
)

type TopicReplica struct {
	Topic        string
	Log          *log.LogManager
	broker       *broker.Broker
	NodeID       string
	ReplicaID    string
	leaderClient leader.LeaderServiceClient
	leaderAddr   string // Leader address for connection
	stopChan     chan struct{}
}

// StartReplication starts the replication process in a goroutine
// This should only be called on replica brokers (not leader)
func (r *TopicReplica) StartReplication() error {
	if r.leaderAddr == "" {
		return fmt.Errorf("leader address not set for replica %s", r.ReplicaID)
	}

	// Create leader client if not already created
	if r.leaderClient == nil {
		conn, err := grpc.Dial(r.leaderAddr, grpc.WithInsecure())
		if err != nil {
			return fmt.Errorf("failed to connect to leader at %s: %w", r.leaderAddr, err)
		}
		r.leaderClient = leader.NewLeaderServiceClient(conn)
	}

	r.stopChan = make(chan struct{})
	go r.startReplication()
	return nil
}

// StopReplication stops the replication process
func (r *TopicReplica) StopReplication() {
	if r.stopChan != nil {
		close(r.stopChan)
	}
}

// startReplication is the main replication loop
// It streams log entries from the leader and applies them to the local log
func (r *TopicReplica) startReplication() {
	ctx := context.Background()
	reconnectDelay := time.Second

	for {
		select {
		case <-r.stopChan:
			return
		default:
		}

		// Get current LEO (Log End Offset) - start replicating from here
		currentOffset := r.Log.LEO()

		// Create replication request
		req := &leader.ReplicateRequest{
			Topic:  r.Topic,
			Offset: currentOffset,
		}

		// Create stream to leader
		stream, err := r.leaderClient.ReplicateStream(ctx, req)
		if err != nil {
			fmt.Printf("replica %s: failed to create replication stream: %v, retrying in %v", r.ReplicaID, err, reconnectDelay)
			time.Sleep(reconnectDelay)
			reconnectDelay = min(reconnectDelay*2, 30*time.Second) // Exponential backoff, max 30s
			continue
		}

		// Reset reconnect delay on successful connection
		reconnectDelay = time.Second

		// Process streamed log entries
		for {
			select {
			case <-r.stopChan:
				return
			default:
			}

			resp, err := stream.Recv()
			if err != nil {
				fmt.Printf("replica %s: stream receive error: %v, reconnecting", r.ReplicaID, err)
				break // Break inner loop to reconnect
			}

			if resp.Entry == nil {
				continue
			}

			// Apply log entry to local log
			entry := resp.Entry
			_, err = r.Log.Append(&common.LogEntry{
				Offset: entry.Offset,
				Value:  entry.Value,
			})
			if err != nil {
				fmt.Printf("replica %s: failed to append log entry at offset %d: %v", r.ReplicaID, entry.Offset, err)
				continue
			}

			// Update LEO after successful append
			r.Log.SetLEO(entry.Offset + 1)
			currentLEO := r.Log.LEO()

			// Report LEO to leader
			err = r.reportLEO(ctx, currentLEO)
			if err != nil {
				fmt.Printf("replica %s: failed to report LEO %d to leader: %v", r.ReplicaID, currentLEO, err)
				// Continue replication even if report fails
			}
		}
	}
}

// reportLEO reports the current Log End Offset (LEO) to the leader
// This allows the leader to track follower progress and update high watermark
func (r *TopicReplica) reportLEO(ctx context.Context, leo uint64) error {
	if r.leaderClient == nil {
		return fmt.Errorf("leader client not initialized")
	}

	req := &leader.RecordLEORequest{
		Topic:     r.Topic,
		ReplicaId: r.ReplicaID,
		Leo:       int64(leo),
	}

	_, err := r.leaderClient.RecordLEO(ctx, req)
	return err
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
