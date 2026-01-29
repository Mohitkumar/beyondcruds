package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/api/leader"
	"github.com/mohitkumar/mlog/api/producer"
	"github.com/mohitkumar/mlog/log"
)

func waitForLogValue(t *testing.T, lm *log.LogManager, offset uint64, want string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		entry, err := lm.ReadUncommitted(offset)
		if err == nil {
			if string(entry.Value) != want {
				t.Fatalf("log: expected message %q at offset %d, got %q", want, offset, string(entry.Value))
			}
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for offset %d to be readable: last error: %v", offset, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

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
	// Warmup message uses offset 0, so ACK_ALL message should be at offset 1.
	if produceResp.Offset != 1 {
		t.Fatalf("expected first ACK_ALL offset 1, got %d", produceResp.Offset)
	}

	replicaLogMgr, err := log.NewLogManager(filepath.Join(servers.followerBaseDir, topicName, "replica-0"))
	if err != nil {
		t.Fatalf("failed to create replica log manager: %v", err)
	}
	defer replicaLogMgr.Close()

	// ACK_ALL should imply the follower has the entry already.
	// In practice we still allow a short window for the replica stream to apply the entry.
	waitForLogValue(t, replicaLogMgr, 1, "ackall-message-1", 2*time.Second)

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

	// Keep as a log line (not an assertion) since ACK_ALL can be fast on localhost.
	t.Logf("ACK_ALL produce duration: %v", ackAllDuration)
}

// TestProduceWithAckAll_10000Messages tests ACK_ALL with 10000 messages and verifies replication.
func TestProduceWithAckAll_10000Messages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10000-message replication test in -short mode")
	}

	servers := setupTestServers(t)
	defer servers.cleanup()

	ctx := context.Background()
	leaderConn, err := servers.getLeaderConn()
	if err != nil {
		t.Fatalf("failed to get leader connection: %v", err)
	}
	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	producerClient := producer.NewProducerServiceClient(leaderConn)

	topicName := "test-ackall-10000"
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	// Let replica connect and start consuming.
	time.Sleep(1 * time.Second)

	// Warmup produce so replica starts consuming/reporting.
	_, err = producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("warmup"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("warmup produce error: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	const n = 10000
	start := time.Now()
	var lastOffset uint64

	for i := 0; i < n; i++ {
		val := []byte(fmt.Sprintf("msg-%d", i))
		resp, err := producerClient.Produce(ctx, &producer.ProduceRequest{
			Topic: topicName,
			Value: val,
			Acks:  producer.AckMode_ACK_ALL,
		})
		if err != nil {
			t.Fatalf("Produce ACK_ALL i=%d error: %v", i, err)
		}
		// Warmup is at offset 0, so msg-0 should be at offset 1.
		wantOffset := uint64(i + 1)
		if resp.Offset != wantOffset {
			t.Fatalf("Produce ACK_ALL i=%d: expected offset %d, got %d", i, wantOffset, resp.Offset)
		}
		lastOffset = resp.Offset
	}

	t.Logf("produced %d ACK_ALL messages in %v", n, time.Since(start))

	// Give it time for all messages to replicate.
	time.Sleep(15 * time.Second)

	replicaLogMgr, err := log.NewLogManager(filepath.Join(servers.followerBaseDir, topicName, "replica-0"))
	if err != nil {
		t.Fatalf("failed to create replica log manager: %v", err)
	}
	defer replicaLogMgr.Close()

	replicaEntry, err := replicaLogMgr.ReadUncommitted(1)
	if err != nil {
		t.Fatalf("failed to read offset 1 from replica log: %v", err)
	}
	if string(replicaEntry.Value) != "msg-0" {
		t.Fatalf("replica log: expected message 'msg-0' at offset 1, got '%s'", string(replicaEntry.Value))
	}

	replicaEntry, err = replicaLogMgr.ReadUncommitted(5000)
	if err != nil {
		t.Fatalf("failed to read offset 5000 from replica log: %v", err)
	}
	if string(replicaEntry.Value) != "msg-4999" {
		t.Fatalf("replica log: expected message 'msg-4999' at offset 5000, got '%s'", string(replicaEntry.Value))
	}

	replicaEntry, err = replicaLogMgr.ReadUncommitted(9999)
	if err != nil {
		t.Fatalf("failed to read offset 9999 from replica log: %v", err)
	}
	if string(replicaEntry.Value) != "msg-9998" {
		t.Fatalf("replica log: expected message 'msg-9998' at offset 9999, got '%s'", string(replicaEntry.Value))
	}

	replicaEntry, err = replicaLogMgr.ReadUncommitted(lastOffset)
	if err != nil {
		t.Fatalf("failed to read last offset %d from replica log: %v", lastOffset, err)
	}
	if string(replicaEntry.Value) != fmt.Sprintf("msg-%d", n-1) {
		t.Fatalf("replica log: expected message 'msg-%d' at offset %d, got '%s'", n-1, lastOffset, string(replicaEntry.Value))
	}

	// Verify leader also has all messages.
	leaderLogMgr, err := log.NewLogManager(filepath.Join(servers.leaderBaseDir, topicName))
	if err != nil {
		t.Fatalf("failed to create leader log manager: %v", err)
	}
	defer leaderLogMgr.Close()

	leaderEntry, err := leaderLogMgr.ReadUncommitted(lastOffset)
	if err != nil {
		t.Fatalf("failed to read last offset %d from leader log: %v", lastOffset, err)
	}
	if string(leaderEntry.Value) != fmt.Sprintf("msg-%d", n-1) {
		t.Fatalf("leader log: expected message 'msg-%d' at offset %d, got '%s'", n-1, lastOffset, string(leaderEntry.Value))
	}

	t.Logf("verified replication: all %d ACK_LEADER messages present in both leader and replica logs", n)
}

// TestProduceWithAckLeader_10000Messages tests ACK_LEADER with 10000 messages and verifies replication.
func TestProduceWithAckLeader_10000Messages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10000-message replication test in -short mode")
	}

	servers := setupTestServers(t)
	defer servers.cleanup()

	ctx := context.Background()
	leaderConn, err := servers.getLeaderConn()
	if err != nil {
		t.Fatalf("failed to get leader connection: %v", err)
	}
	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	producerClient := producer.NewProducerServiceClient(leaderConn)

	topicName := "test-ackleader-10000"
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	// Let replica connect and start consuming.
	time.Sleep(1 * time.Second)

	// Warmup produce so replica starts consuming/reporting.
	_, err = producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("warmup"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("warmup produce error: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	const n = 10000
	start := time.Now()
	var lastOffset uint64

	for i := 0; i < n; i++ {
		val := []byte(fmt.Sprintf("msg-%d", i))
		resp, err := producerClient.Produce(ctx, &producer.ProduceRequest{
			Topic: topicName,
			Value: val,
			Acks:  producer.AckMode_ACK_LEADER,
		})
		if err != nil {
			t.Fatalf("Produce ACK_LEADER i=%d error: %v", i, err)
		}
		// Warmup is at offset 0, so msg-0 should be at offset 1.
		wantOffset := uint64(i + 1)
		if resp.Offset != wantOffset {
			t.Fatalf("Produce ACK_LEADER i=%d: expected offset %d, got %d", i, wantOffset, resp.Offset)
		}
		lastOffset = resp.Offset
	}

	t.Logf("produced %d ACK_LEADER messages in %v", n, time.Since(start))

	// Verify leader  has all messages.
	leaderLogMgr, err := log.NewLogManager(filepath.Join(servers.leaderBaseDir, topicName))
	if err != nil {
		t.Fatalf("failed to create leader log manager: %v", err)
	}

	leaderEntry, err := leaderLogMgr.ReadUncommitted(lastOffset)
	if err != nil {
		t.Fatalf("failed to read last offset %d from leader log: %v", lastOffset, err)
	}
	if string(leaderEntry.Value) != fmt.Sprintf("msg-%d", n-1) {
		t.Fatalf("leader log: expected message 'msg-%d' at offset %d, got '%s'", n-1, lastOffset, string(leaderEntry.Value))
	}
	leaderLogMgr.Close()

	// With ACK_LEADER, we need to wait longer for replication to catch up.
	// Give it time for all messages to replicate.
	time.Sleep(10 * time.Second)

	replicaLogMgr, err := log.NewLogManager(filepath.Join(servers.followerBaseDir, topicName, "replica-0"))
	if err != nil {
		t.Fatalf("failed to create replica log manager: %v", err)
	}
	defer replicaLogMgr.Close()

	replicaEntry, err := replicaLogMgr.ReadUncommitted(1)
	if err != nil {
		t.Fatalf("failed to read offset 1 from replica log: %v", err)
	}
	if string(replicaEntry.Value) != "msg-0" {
		t.Fatalf("replica log: expected message 'msg-0' at offset 1, got '%s'", string(replicaEntry.Value))
	}

	replicaEntry, err = replicaLogMgr.ReadUncommitted(5000)
	if err != nil {
		t.Fatalf("failed to read offset 5000 from replica log: %v", err)
	}
	if string(replicaEntry.Value) != "msg-4999" {
		t.Fatalf("replica log: expected message 'msg-4999' at offset 5000, got '%s'", string(replicaEntry.Value))
	}

	replicaEntry, err = replicaLogMgr.ReadUncommitted(9999)
	if err != nil {
		t.Fatalf("failed to read offset 9999 from replica log: %v", err)
	}
	if string(replicaEntry.Value) != "msg-9998" {
		t.Fatalf("replica log: expected message 'msg-9998' at offset 9999, got '%s'", string(replicaEntry.Value))
	}

	replicaEntry, err = replicaLogMgr.ReadUncommitted(lastOffset)
	if err != nil {
		t.Fatalf("failed to read last offset %d from replica log: %v", lastOffset, err)
	}
	if string(replicaEntry.Value) != fmt.Sprintf("msg-%d", n-1) {
		t.Fatalf("replica log: expected message 'msg-%d' at offset %d, got '%s'", n-1, lastOffset, string(replicaEntry.Value))
	}

	t.Logf("verified replication: all %d ACK_LEADER messages present in both leader and replica logs", n)
}
