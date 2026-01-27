package log

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/api/replication"
	"github.com/mohitkumar/mlog/broker"
)

type Topic struct {
	Name     string
	leader   *TopicLeader
	replicas map[string]*TopicReplica
}

type TopicManager struct {
	mu            sync.RWMutex
	topics        map[string]*Topic
	BaseDir       string
	brokerManager *broker.BrokerManager
	currentBroker *broker.Broker // The current broker (leader broker)
}

func NewTopicManager(baseDir string, brokerManager *broker.BrokerManager, currentBroker *broker.Broker) (*TopicManager, error) {
	return &TopicManager{
		topics:        make(map[string]*Topic),
		BaseDir:       baseDir,
		brokerManager: brokerManager,
		currentBroker: currentBroker,
	}, nil
}

func (t *TopicManager) GetLeader(topic string) (*TopicLeader, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	topicObj, ok := t.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	return topicObj.leader, nil
}

func (t *TopicManager) GetReplica(topic string, replicaID string) (*TopicReplica, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	topicObj, ok := t.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	replica, ok := topicObj.replicas[replicaID]
	if !ok {
		return nil, fmt.Errorf("replica %s not found", replicaID)
	}
	return replica, nil
}

func (t *TopicManager) CreateTopic(topic string, replicaCount int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if topic already exists
	if _, exists := t.topics[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}

	// Create log manager for the leader (current broker)
	logManager, err := NewLogManager(filepath.Join(t.BaseDir, topic))
	if err != nil {
		return fmt.Errorf("failed to create log manager: %w", err)
	}

	// Create the topic with leader on current broker
	topicObj := &Topic{
		Name: topic,
		leader: &TopicLeader{
			Topic:     topic,
			Log:       logManager,
			broker:    t.currentBroker,
			NodeID:    t.currentBroker.NodeID,
			followers: make(map[string]*FollowerState),
		},
		replicas: make(map[string]*TopicReplica),
	}
	t.topics[topic] = topicObj

	// Get all available brokers
	allBrokers := t.brokerManager.GetAllBrokers()
	if len(allBrokers) < replicaCount+1 {
		return fmt.Errorf("not enough brokers: need %d, have %d", replicaCount+1, len(allBrokers))
	}

	// Select brokers for replicas (excluding current broker)
	replicaBrokers := make([]*broker.Broker, 0, replicaCount)
	for _, b := range allBrokers {
		if b.NodeID != t.currentBroker.NodeID && len(replicaBrokers) < replicaCount {
			replicaBrokers = append(replicaBrokers, b)
		}
	}

	// Create replicas on other brokers
	for i, replicaBroker := range replicaBrokers {
		replicaID := fmt.Sprintf("replica-%d", i)

		// Create replica on remote broker via gRPC
		replicaClient := replication.NewReplicationServiceClient(replicaBroker.GetConn())
		ctx := context.Background()
		_, err := replicaClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
			Topic:      topic,
			ReplicaId:  replicaID,
			LeaderAddr: t.currentBroker.Addr, // Pass leader address
		})
		if err != nil {
			// Cleanup on error
			delete(t.topics, topic)
			logManager.Close()
			return fmt.Errorf("failed to create replica %s on broker %s: %w", replicaID, replicaBroker.NodeID, err)
		}

		// Track the replica locally
		topicObj.leader.followers[replicaID] = &FollowerState{
			NodeID:            replicaBroker.NodeID,
			LastFetchTime:     time.Now(),
			LastFetchedOffset: 0,
			IsISR:             false,
		}

		// Create local replica tracking (for leader to communicate with followers)
		// Note: The actual replica log is on the remote broker, this is just metadata
		topicObj.replicas[replicaID] = &TopicReplica{
			Topic:      topic,
			Log:        nil, // Replica log is on remote broker
			broker:     replicaBroker,
			NodeID:     replicaBroker.NodeID,
			ReplicaID:  replicaID,
			leaderAddr: t.currentBroker.Addr, // Leader address for replica to connect
		}
	}

	return nil
}

func (t *TopicManager) DeleteTopic(topic string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	topicObj, ok := t.topics[topic]
	if !ok {
		return fmt.Errorf("topic %s not found", topic)
	}

	// Close leader log if this broker is the leader
	if topicObj.leader != nil && topicObj.leader.Log != nil {
		topicObj.leader.Log.Close()
	}

	// Delete leader log
	if topicObj.leader != nil && topicObj.leader.Log != nil {
		topicObj.leader.Log.Delete()
	}

	// Delete replica from remote broker
	for _, replica := range topicObj.replicas {
		// Create client to replica broker to call DeleteReplica
		replicaClient := replication.NewReplicationServiceClient(replica.broker.GetConn())
		_, err := replicaClient.DeleteReplica(context.Background(), &replication.DeleteReplicaRequest{
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

	delete(t.topics, topic)
	return nil
}

// CreateReplica is called by remote brokers to create a replica locally
// This is the server-side handler for the CreateReplica gRPC call
// leaderBrokerAddr is the address of the leader broker that this replica should replicate from
func (t *TopicManager) CreateReplicaRemote(topic string, replicaID string, leaderBrokerAddr string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if topic exists (it should, created by leader)
	topicObj, ok := t.topics[topic]
	if !ok {
		// If topic doesn't exist, create it as a replica (not leader)
		topicObj = &Topic{
			Name:     topic,
			leader:   nil, // This broker is not the leader
			replicas: make(map[string]*TopicReplica),
		}
		t.topics[topic] = topicObj
	}

	// Create log manager for this replica
	logManager, err := NewLogManager(filepath.Join(t.BaseDir, topic, replicaID))
	if err != nil {
		return fmt.Errorf("failed to create log manager for replica: %w", err)
	}

	// Create replica with leader address information
	replica := &TopicReplica{
		Topic:      topic,
		Log:        logManager,
		broker:     t.currentBroker,
		NodeID:     t.currentBroker.NodeID,
		ReplicaID:  replicaID,
		leaderAddr: leaderBrokerAddr, // Leader address for connecting
	}

	topicObj.replicas[replicaID] = replica

	// Start replication automatically
	if err := replica.StartReplication(); err != nil {
		return fmt.Errorf("failed to start replication for replica %s: %w", replicaID, err)
	}

	return nil
}

func (t *TopicManager) DeleteReplicaRemote(topic string, replicaID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	topicObj, ok := t.topics[topic]
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
