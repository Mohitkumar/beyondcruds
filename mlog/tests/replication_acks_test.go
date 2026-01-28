package tests

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/api/leader"
	"github.com/mohitkumar/mlog/api/producer"
	"github.com/mohitkumar/mlog/log"
)

// TestProduceAndReplicate tests that messages produced to leader are replicated to followers (ACK_LEADER).
func TestProduceAndReplicate(t *testing.T) {
	servers := setupTestServers(t)
	defer servers.cleanup()

	ctx := context.Background()
	leaderConn, err := servers.getLeaderConn()
	if err != nil {
		t.Fatalf("failed to get leader connection: %v", err)
	}
	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	producerClient := producer.NewProducerServiceClient(leaderConn)

	topicName := "test-replication-topic"
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	produceResp, err := producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("test-message-1"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("Produce error: %v", err)
	}
	if produceResp.Offset != 0 {
		t.Fatalf("expected first offset 0, got %d", produceResp.Offset)
	}

	produceResp2, err := producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("test-message-2"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("Produce error: %v", err)
	}
	if produceResp2.Offset != 1 {
		t.Fatalf("expected second offset 1, got %d", produceResp2.Offset)
	}

	// Give replication some time.
	time.Sleep(1 * time.Second)

	leaderLogMgr, err := log.NewLogManager(filepath.Join(servers.leaderBaseDir, topicName))
	if err != nil {
		t.Fatalf("failed to create leader log manager: %v", err)
	}
	defer leaderLogMgr.Close()

	leaderEntry1, err := leaderLogMgr.ReadUncommitted(0)
	if err != nil {
		t.Fatalf("failed to read offset 0 from leader log: %v", err)
	}
	if string(leaderEntry1.Value) != "test-message-1" {
		t.Fatalf("leader log: expected message 'test-message-1' at offset 0, got '%s'", string(leaderEntry1.Value))
	}

	leaderEntry2, err := leaderLogMgr.ReadUncommitted(1)
	if err != nil {
		t.Fatalf("failed to read offset 1 from leader log: %v", err)
	}
	if string(leaderEntry2.Value) != "test-message-2" {
		t.Fatalf("leader log: expected message 'test-message-2' at offset 1, got '%s'", string(leaderEntry2.Value))
	}

	replicaLogMgr, err := log.NewLogManager(filepath.Join(servers.followerBaseDir, topicName, "replica-0"))
	if err != nil {
		t.Fatalf("failed to create replica log manager: %v", err)
	}
	defer replicaLogMgr.Close()

	replicaEntry1, err := replicaLogMgr.ReadUncommitted(0)
	if err != nil {
		t.Fatalf("failed to read offset 0 from replica log: %v", err)
	}
	if string(replicaEntry1.Value) != "test-message-1" {
		t.Fatalf("replica log: expected message 'test-message-1' at offset 0, got '%s'", string(replicaEntry1.Value))
	}

	replicaEntry2, err := replicaLogMgr.ReadUncommitted(1)
	if err != nil {
		t.Fatalf("failed to read offset 1 from replica log: %v", err)
	}
	if string(replicaEntry2.Value) != "test-message-2" {
		t.Fatalf("replica log: expected message 'test-message-2' at offset 1, got '%s'", string(replicaEntry2.Value))
	}
}

// TestProduceWithAckAll tests that ACK_ALL waits for follower replication before returning.
func TestProduceWithAckAll(t *testing.T) {
	servers := setupTestServers(t)
	defer servers.cleanup()

	ctx := context.Background()
	leaderConn, err := servers.getLeaderConn()
	if err != nil {
		t.Fatalf("failed to get leader connection: %v", err)
	}
	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	producerClient := producer.NewProducerServiceClient(leaderConn)

	topicName := "test-ackall-topic"
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	// Give replica time to start and begin reporting LEOs.
	time.Sleep(1 * time.Second)

	// Warmup produce (ACK_LEADER) so replica starts consuming and reporting.
	_, err = producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("warmup-message"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("Warmup produce error: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	startTime := time.Now()
	produceResp, err := producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("ackall-message-1"),
		Acks:  producer.AckMode_ACK_ALL,
	})
	ackAllDuration := time.Since(startTime)
	if err != nil {
		t.Fatalf("Produce with ACK_ALL error: %v", err)
	}
	if produceResp.Offset != 0 {
		t.Fatalf("expected first offset 0, got %d", produceResp.Offset)
	}

	replicaLogMgr, err := log.NewLogManager(filepath.Join(servers.followerBaseDir, topicName, "replica-0"))
	if err != nil {
		t.Fatalf("failed to create replica log manager: %v", err)
	}
	defer replicaLogMgr.Close()

	// ACK_ALL should imply the follower has the entry already.
	replicaEntry, err := replicaLogMgr.ReadUncommitted(1)
	if err != nil {
		t.Fatalf("failed to read offset 1 from replica log: %v", err)
	}
	if string(replicaEntry.Value) != "ackall-message-1" {
		t.Fatalf("replica log: expected message 'ackall-message-1' at offset 1, got '%s'", string(replicaEntry.Value))
	}

	produceResp2, err := producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("ackall-message-2"),
		Acks:  producer.AckMode_ACK_ALL,
	})
	if err != nil {
		t.Fatalf("Produce with ACK_ALL error: %v", err)
	}
	if produceResp2.Offset != 2 {
		t.Fatalf("expected second offset 2, got %d", produceResp2.Offset)
	}

	replicaEntry2, err := replicaLogMgr.ReadUncommitted(2)
	if err != nil {
		t.Fatalf("failed to read offset 2 from replica log: %v", err)
	}
	if string(replicaEntry2.Value) != "ackall-message-2" {
		t.Fatalf("replica log: expected message 'ackall-message-2' at offset 2, got '%s'", string(replicaEntry2.Value))
	}

	leaderLogMgr, err := log.NewLogManager(filepath.Join(servers.leaderBaseDir, topicName))
	if err != nil {
		t.Fatalf("failed to create leader log manager: %v", err)
	}
	defer leaderLogMgr.Close()

	leaderEntry1, err := leaderLogMgr.ReadUncommitted(1)
	if err != nil {
		t.Fatalf("failed to read offset 1 from leader log: %v", err)
	}
	if string(leaderEntry1.Value) != "ackall-message-1" {
		t.Fatalf("leader log: expected message 'ackall-message-1' at offset 1, got '%s'", string(leaderEntry1.Value))
	}

	leaderEntry2, err := leaderLogMgr.ReadUncommitted(2)
	if err != nil {
		t.Fatalf("failed to read offset 2 from leader log: %v", err)
	}
	if string(leaderEntry2.Value) != "ackall-message-2" {
		t.Fatalf("leader log: expected message 'ackall-message-2' at offset 2, got '%s'", string(leaderEntry2.Value))
	}

	// Keep as a log line (not an assertion) since ACK_ALL can be fast on localhost.
	t.Logf("ACK_ALL produce duration: %v", ackAllDuration)
}

