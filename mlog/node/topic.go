package node

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/api/common"
	"github.com/mohitkumar/mlog/api/producer"
	"github.com/mohitkumar/mlog/api/replication"
	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/log"
)

// Topic represents a topic with its leader and replicas
type Topic struct {
	mu            sync.RWMutex
	Name          string
	leader        *Node
	replicas      map[string]*Node
	brokerManager *broker.BrokerManager
	baseDir       string
}

// TopicManager manages multiple topics
type TopicManager struct {
	mu            sync.RWMutex
	topics        map[string]*Topic
	BaseDir       string
	brokerManager *broker.BrokerManager
	currentBroker *broker.Broker
}

func NewTopicManager(baseDir string, brokerManager *broker.BrokerManager, currentBroker *broker.Broker) (*TopicManager, error) {
	return &TopicManager{
		topics:        make(map[string]*Topic),
		BaseDir:       baseDir,
		brokerManager: brokerManager,
		currentBroker: currentBroker,
	}, nil
}

// CreateTopic creates a new topic with the specified replica count
func (tm *TopicManager) CreateTopic(topic string, replicaCount int) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check if topic already exists
	if _, exists := tm.topics[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}

	// Create log manager for the leader (current broker)
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic))
	if err != nil {
		return fmt.Errorf("failed to create log manager: %w", err)
	}

	// Create leader node
	leaderNode := NewLeaderNode(topic, logManager, tm.currentBroker, tm.brokerManager)

	// Create the topic
	topicObj := &Topic{
		Name:          topic,
		leader:        leaderNode,
		replicas:      make(map[string]*Node),
		brokerManager: tm.brokerManager,
		baseDir:       tm.BaseDir,
	}
	tm.topics[topic] = topicObj

	// Get all available brokers
	allBrokers := tm.brokerManager.GetAllBrokers()
	if len(allBrokers) < replicaCount+1 {
		// Cleanup on error
		delete(tm.topics, topic)
		logManager.Close()
		return fmt.Errorf("not enough brokers: need %d, have %d", replicaCount+1, len(allBrokers))
	}

	// Select brokers for replicas (excluding current broker)
	replicaBrokers := make([]*broker.Broker, 0, replicaCount)
	for _, b := range allBrokers {
		if b.NodeID != tm.currentBroker.NodeID && len(replicaBrokers) < replicaCount {
			replicaBrokers = append(replicaBrokers, b)
		}
	}

	// Create replicas on other brokers
	for i, replicaBroker := range replicaBrokers {
		replicaID := fmt.Sprintf("replica-%d", i)

		// Create replica on remote broker via gRPC
		conn, err := replicaBroker.GetConn()
		if err != nil {
			// Cleanup on error
			delete(tm.topics, topic)
			logManager.Close()
			return fmt.Errorf("failed to connect to broker %s: %w", replicaBroker.NodeID, err)
		}
		replicaClient := replication.NewReplicationServiceClient(conn)
		ctx := context.Background()
		_, err = replicaClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
			Topic:      topic,
			ReplicaId:  replicaID,
			LeaderAddr: tm.currentBroker.Addr, // Pass leader address
		})
		if err != nil {
			// Cleanup on error
			delete(tm.topics, topic)
			logManager.Close()
			return fmt.Errorf("failed to create replica %s on broker %s: %w", replicaID, replicaBroker.NodeID, err)
		}

		// Track the replica locally
		leaderNode.mu.Lock()
		leaderNode.followers[replicaID] = &FollowerState{
			NodeID:            replicaBroker.NodeID,
			LastFetchTime:     time.Now(),
			LastFetchedOffset: 0,
			IsISR:             false,
		}
		leaderNode.mu.Unlock()

		// Create local replica tracking (for leader to communicate with followers)
		// Note: The actual replica log is on the remote broker, this is just metadata
		replicaNode := &Node{
			Topic:         topic,
			Log:           nil, // Replica log is on remote broker
			broker:        replicaBroker,
			NodeID:        replicaBroker.NodeID,
			ReplicaID:     replicaID,
			IsLeader:      false,
			leaderAddr:    tm.currentBroker.Addr,
			brokerManager: tm.brokerManager,
		}
		topicObj.replicas[replicaID] = replicaNode
	}

	return nil
}

// DeleteTopic deletes a topic
func (tm *TopicManager) DeleteTopic(topic string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return fmt.Errorf("topic %s not found", topic)
	}

	// Close and delete leader log if this broker is the leader
	if topicObj.leader != nil && topicObj.leader.Log != nil {
		topicObj.leader.Log.Close()
		topicObj.leader.Log.Delete()
	}

	// Delete replica from remote broker
	for _, replica := range topicObj.replicas {
		// Create client to replica broker to call DeleteReplica
		conn, err := replica.broker.GetConn()
		if err != nil {
			return fmt.Errorf("failed to connect to broker %s: %w", replica.broker.NodeID, err)
		}
		replicaClient := replication.NewReplicationServiceClient(conn)
		_, err = replicaClient.DeleteReplica(context.Background(), &replication.DeleteReplicaRequest{
			Topic:     topic,
			ReplicaId: replica.ReplicaID,
		})
		if err != nil {
			return fmt.Errorf("failed to delete replica %s on broker %s: %w", replica.ReplicaID, replica.broker.NodeID, err)
		}
	}

	// Close replica logs
	for _, replica := range topicObj.replicas {
		if replica.Log != nil {
			replica.Log.Close()
		}
	}

	delete(tm.topics, topic)
	return nil
}

// GetLeader returns the leader node for a topic
func (tm *TopicManager) GetLeader(topic string) (*Node, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	topicObj, ok := tm.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	if topicObj.leader == nil {
		return nil, fmt.Errorf("topic %s has no leader", topic)
	}
	return topicObj.leader, nil
}

// GetReplica returns a replica node for a topic
func (tm *TopicManager) GetReplica(topic string, replicaID string) (*Node, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	topicObj, ok := tm.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	replica, ok := topicObj.replicas[replicaID]
	if !ok {
		return nil, fmt.Errorf("replica %s not found", replicaID)
	}
	return replica, nil
}

// GetTopic returns the topic object
func (tm *TopicManager) GetTopic(topic string) (*Topic, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	topicObj, ok := tm.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	return topicObj, nil
}

// CreateReplicaRemote is called by remote brokers to create a replica locally
func (tm *TopicManager) CreateReplicaRemote(topic string, replicaID string, leaderBrokerAddr string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check if topic exists (it should, created by leader)
	topicObj, ok := tm.topics[topic]
	if !ok {
		// If topic doesn't exist, create it as a replica (not leader)
		topicObj = &Topic{
			Name:          topic,
			leader:        nil, // This broker is not the leader
			replicas:      make(map[string]*Node),
			brokerManager: tm.brokerManager,
			baseDir:       tm.BaseDir,
		}
		tm.topics[topic] = topicObj
	}

	// Create log manager for this replica
	logManager, err := log.NewLogManager(filepath.Join(tm.BaseDir, topic, replicaID))
	if err != nil {
		return fmt.Errorf("failed to create log manager for replica: %w", err)
	}

	// Create replica node
	replicaNode := NewReplicaNode(topic, replicaID, logManager, tm.currentBroker, leaderBrokerAddr, tm.brokerManager)
	topicObj.replicas[replicaID] = replicaNode

	// Start replication automatically
	if err := replicaNode.StartReplication(); err != nil {
		return fmt.Errorf("failed to start replication for replica %s: %w", replicaID, err)
	}

	return nil
}

// DeleteReplicaRemote deletes a replica locally
func (tm *TopicManager) DeleteReplicaRemote(topic string, replicaID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topicObj, ok := tm.topics[topic]
	if !ok {
		return fmt.Errorf("topic %s not found", topic)
	}
	replica, ok := topicObj.replicas[replicaID]
	if !ok {
		return fmt.Errorf("replica %s not found", replicaID)
	}

	// Stop replication if running
	if replica != nil {
		replica.StopReplication()
	}

	// Delete replica log if it exists
	if replica.Log != nil {
		replica.Log.Delete()
	}

	delete(topicObj.replicas, replicaID)
	return nil
}

// HandleProduce handles produce requests (leader only)
func (t *Topic) HandleProduce(ctx context.Context, logEntry *common.LogEntry, acks producer.AckMode) (uint64, error) {
	if t.leader == nil {
		return 0, fmt.Errorf("topic %s has no leader", t.Name)
	}

	offset, err := t.leader.Log.Append(logEntry)
	if err != nil {
		return 0, err
	}

	t.leader.mu.Lock()
	defer t.leader.mu.Unlock()

	switch acks {
	case producer.AckMode_ACK_LEADER:
		return offset, nil
	case producer.AckMode_ACK_ALL:
		err := t.waitForAllFollowersToCatchUp(ctx, offset)
		if err != nil {
			return 0, fmt.Errorf("failed to wait for all followers to catch up: %w", err)
		}
	default:
		return 0, fmt.Errorf("invalid ack mode: %s", acks)
	}
	return offset, nil
}

// waitForAllFollowersToCatchUp waits for all ISR followers to catch up to the given offset
func (t *Topic) waitForAllFollowersToCatchUp(ctx context.Context, offset uint64) error {
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		allCaughtUp := true
		t.leader.mu.RLock()
		for _, follower := range t.leader.followers {
			if follower.IsISR {
				if follower.LastFetchedOffset < offset {
					allCaughtUp = false
					break
				}
			}
		}
		t.leader.mu.RUnlock()

		if allCaughtUp {
			break
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout before all followers caught up")
		}
	}
	return nil
}

// HandleFetch handles fetch requests from followers (leader only)
func (t *Topic) HandleFetch(followerID string, offset uint64) ([]*common.LogEntry, uint64, error) {
	if t.leader == nil {
		return nil, 0, fmt.Errorf("topic %s has no leader", t.Name)
	}

	t.leader.mu.Lock()
	// Track follower progress
	if _, ok := t.leader.followers[followerID]; !ok {
		t.leader.followers[followerID] = &FollowerState{}
	}
	t.leader.followers[followerID].LastFetchedOffset = offset
	t.leader.followers[followerID].LastFetchTime = time.Now()
	t.leader.mu.Unlock()

	// Try to advance High Watermark based on new follower info
	t.maybeAdvanceHW()

	// Return messages and current HW
	entry, err := t.leader.Log.Read(offset)
	if err != nil {
		return nil, 0, err
	}
	return []*common.LogEntry{entry}, t.leader.Log.HighWatermark(), nil
}

// maybeAdvanceHW advances the high watermark based on follower states
func (t *Topic) maybeAdvanceHW() {
	if t.leader == nil {
		return
	}

	t.leader.mu.Lock()
	defer t.leader.mu.Unlock()

	// HW is the highest offset replicated to a majority
	minOffset := t.leader.Log.LEO()
	for _, f := range t.leader.followers {
		if f.LastFetchedOffset < minOffset {
			minOffset = f.LastFetchedOffset
		}
	}
	t.leader.Log.SetHighWatermark(minOffset)
}

// MaybeAdvanceHW is a public wrapper for maybeAdvanceHW
func (t *Topic) MaybeAdvanceHW() {
	t.maybeAdvanceHW()
}

// RecordLEORemote records the LEO of a replica (leader only)
func (t *Topic) RecordLEORemote(replicaID string, leo uint64, leoTime time.Time) error {
	if t.leader == nil {
		return fmt.Errorf("topic %s has no leader", t.Name)
	}

	t.leader.mu.Lock()
	defer t.leader.mu.Unlock()

	if follower, ok := t.leader.followers[replicaID]; ok {
		follower.LastFetchedOffset = leo
		follower.LastFetchTime = leoTime
		// Mark as ISR if caught up (simple heuristic: within 100 offsets)
		if leo >= t.leader.Log.LEO()-100 {
			follower.IsISR = true
		}
	}

	// Try to advance HW after recording LEO
	t.leader.mu.Unlock()
	t.maybeAdvanceHW()
	t.leader.mu.Lock()

	return nil
}
