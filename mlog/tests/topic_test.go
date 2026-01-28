package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/api/leader"
	"github.com/mohitkumar/mlog/api/producer"
)

func TestCreateTopic(t *testing.T) {
	servers := setupTestServers(t)
	defer servers.cleanup()

	ctx := context.Background()
	leaderConn, err := servers.getLeaderConn()
	if err != nil {
		t.Fatalf("failed to get leader connection: %v", err)
	}
	client := leader.NewLeaderServiceClient(leaderConn)

	topicName := "test-topic-1"
	resp, err := client.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}
	if resp.Topic != topicName {
		t.Fatalf("expected topic %s, got %s", topicName, resp.Topic)
	}

	if _, err := os.Stat(filepath.Join(servers.leaderBaseDir, topicName)); os.IsNotExist(err) {
		t.Fatalf("expected topic directory %s to exist on leader, got error: %v", topicName, err)
	}
	if _, err := os.Stat(filepath.Join(servers.followerBaseDir, topicName)); os.IsNotExist(err) {
		t.Fatalf("expected topic directory %s to exist on follower, got error: %v", topicName, err)
	}
}

func TestDeleteTopic(t *testing.T) {
	servers := setupTestServers(t)
	defer servers.cleanup()

	ctx := context.Background()
	leaderConn, err := servers.getLeaderConn()
	if err != nil {
		t.Fatalf("failed to get leader connection: %v", err)
	}
	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	producerClient := producer.NewProducerServiceClient(leaderConn)

	topicName := "test-delete-topic"
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	// Wait for replica to start replication.
	time.Sleep(500 * time.Millisecond)

	// Produce at least one message so files exist.
	_, err = producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("test-message"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("Produce error: %v", err)
	}

	// Wait for replication.
	time.Sleep(500 * time.Millisecond)

	leaderTopicDir := filepath.Join(servers.leaderBaseDir, topicName)
	followerTopicDir := filepath.Join(servers.followerBaseDir, topicName)
	replicaDir := filepath.Join(followerTopicDir, "replica-0")

	if _, err := os.Stat(leaderTopicDir); os.IsNotExist(err) {
		t.Fatalf("expected leader topic directory %s to exist before deletion", leaderTopicDir)
	}
	if _, err := os.Stat(replicaDir); os.IsNotExist(err) {
		t.Fatalf("expected replica directory %s to exist before deletion", replicaDir)
	}

	deleteResp, err := leaderClient.DeleteTopic(ctx, &leader.DeleteTopicRequest{Topic: topicName})
	if err != nil {
		t.Fatalf("DeleteTopic error: %v", err)
	}
	if deleteResp.Topic != topicName {
		t.Fatalf("expected deleted topic %s, got %s", topicName, deleteResp.Topic)
	}

	// Wait a bit for cleanup to complete.
	time.Sleep(500 * time.Millisecond)

	if _, err := os.Stat(leaderTopicDir); !os.IsNotExist(err) {
		t.Fatalf("expected leader topic directory %s to be deleted, but it still exists", leaderTopicDir)
	}
	if _, err := os.Stat(followerTopicDir); !os.IsNotExist(err) {
		t.Fatalf("expected follower topic directory %s to be deleted, but it still exists", followerTopicDir)
	}
	if _, err := os.Stat(replicaDir); !os.IsNotExist(err) {
		t.Fatalf("expected replica directory %s to be deleted, but it still exists", replicaDir)
	}

	// Producing to deleted topic should fail.
	_, err = producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("should-fail"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err == nil {
		t.Fatalf("expected Produce to fail for deleted topic, but it succeeded")
	}

	// Deleting a non-existent topic should return error.
	_, err = leaderClient.DeleteTopic(ctx, &leader.DeleteTopicRequest{Topic: "non-existent-topic"})
	if err == nil {
		t.Fatalf("expected DeleteTopic to fail for non-existent topic, but it succeeded")
	}
}

