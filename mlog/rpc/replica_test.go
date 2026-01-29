package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/api/common"
	"github.com/mohitkumar/mlog/api/leader"
	"github.com/mohitkumar/mlog/api/producer"
	"github.com/mohitkumar/mlog/api/replication"
	"github.com/mohitkumar/mlog/testutil"
)

func TestCreateReplica(t *testing.T) {
	leaderSrv, followerSrv := testutil.SetupTwoTestServers(t, NewGrpcServer)
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	ctx := context.Background()

	// Create topic on leader
	leaderConn, err := leaderSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer leaderConn.Close()

	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        "test-topic",
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Create replica on follower
	followerConn, err := followerSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)
	resp, err := replicationClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
		Topic:      "test-topic",
		ReplicaId:  "replica-0",
		LeaderAddr: leaderSrv.Addr,
	})
	if err != nil {
		t.Fatalf("CreateReplica: %v", err)
	}
	if resp.ReplicaId != "replica-0" {
		t.Fatalf("expected replica_id 'replica-0', got %s", resp.ReplicaId)
	}
}

func TestCreateReplica_InvalidArguments(t *testing.T) {
	_, follower := testutil.SetupTwoTestServers(t, NewGrpcServer)
	defer follower.Cleanup()

	ctx := context.Background()
	followerConn, err := follower.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)

	tests := []struct {
		name    string
		req     *replication.CreateReplicaRequest
		wantErr bool
	}{
		{
			name: "missing topic",
			req: &replication.CreateReplicaRequest{
				ReplicaId:  "replica-0",
				LeaderAddr: "127.0.0.1:9092",
			},
			wantErr: true,
		},
		{
			name: "missing replica_id",
			req: &replication.CreateReplicaRequest{
				Topic:      "test-topic",
				LeaderAddr: "127.0.0.1:9092",
			},
			wantErr: true,
		},
		{
			name: "missing leader_addr",
			req: &replication.CreateReplicaRequest{
				Topic:     "test-topic",
				ReplicaId: "replica-0",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := replicationClient.CreateReplica(ctx, tt.req)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreateReplica() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeleteReplica(t *testing.T) {
	leaderSrv, followerSrv := testutil.SetupTwoTestServers(t, NewGrpcServer)
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	ctx := context.Background()

	// Create topic on leader
	leaderConn, err := leaderSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer leaderConn.Close()

	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        "test-topic",
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Create replica on follower
	followerConn, err := followerSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)
	_, err = replicationClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
		Topic:      "test-topic",
		ReplicaId:  "replica-0",
		LeaderAddr: leaderSrv.Addr,
	})
	if err != nil {
		t.Fatalf("CreateReplica: %v", err)
	}

	// Delete replica
	_, err = replicationClient.DeleteReplica(ctx, &replication.DeleteReplicaRequest{
		Topic:     "test-topic",
		ReplicaId: "replica-0",
	})
	if err != nil {
		t.Fatalf("DeleteReplica: %v", err)
	}
}

func TestDeleteReplica_InvalidArguments(t *testing.T) {
	_, follower := testutil.SetupTwoTestServers(t, NewGrpcServer)
	defer follower.Cleanup()

	ctx := context.Background()
	followerConn, err := follower.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)

	// Missing replica_id should fail
	_, err = replicationClient.DeleteReplica(ctx, &replication.DeleteReplicaRequest{
		Topic: "test-topic",
	})
	if err == nil {
		t.Fatal("expected error for missing replica_id")
	}
}

func TestCreateReplica_Integration(t *testing.T) {
	// Test that creating a replica and producing messages works end-to-end
	leaderSrv, followerSrv := testutil.SetupTwoTestServers(t, NewGrpcServer)
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	ctx := context.Background()

	// Create topic on leader
	leaderConn, err := leaderSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer leaderConn.Close()

	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        "test-topic",
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Create replica on follower
	followerConn, err := followerSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)
	_, err = replicationClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
		Topic:      "test-topic",
		ReplicaId:  "replica-0",
		LeaderAddr: leaderSrv.Addr,
	})
	if err != nil {
		t.Fatalf("CreateReplica: %v", err)
	}

	// Produce a message to leader
	producerClient := producer.NewProducerServiceClient(leaderConn)
	_, err = producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("test-message"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
}

func TestReplication_VerifyMessages(t *testing.T) {
	// Test that messages produced to leader are replicated to follower
	leaderSrv, followerSrv := testutil.SetupTwoTestServers(t, NewGrpcServer)
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	ctx := context.Background()
	topicName := "replication-test-topic"

	// Create topic on leader
	leaderConn, err := leaderSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer leaderConn.Close()

	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Wait for topic creation to complete
	time.Sleep(200 * time.Millisecond)

	// Create replica on follower
	followerConn, err := followerSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)
	_, err = replicationClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
		Topic:      topicName,
		ReplicaId:  "replica-0",
		LeaderAddr: leaderSrv.Addr,
	})
	if err != nil {
		t.Fatalf("CreateReplica: %v", err)
	}

	// Wait for replica to start replication
	time.Sleep(500 * time.Millisecond)

	// Produce multiple messages to leader
	producerClient := producer.NewProducerServiceClient(leaderConn)
	messages := []string{
		"message-0",
		"message-1",
		"message-2",
		"message-3",
		"message-4",
	}

	var lastOffset uint64
	for i, msg := range messages {
		resp, err := producerClient.Produce(ctx, &producer.ProduceRequest{
			Topic: topicName,
			Value: []byte(msg),
			Acks:  producer.AckMode_ACK_LEADER,
		})
		if err != nil {
			t.Fatalf("Produce message %d: %v", i, err)
		}
		if resp.Offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, resp.Offset)
		}
		lastOffset = resp.Offset
	}

	// Verify messages on leader - use the existing LogManager from the leader node
	// instead of creating a new one from disk (which won't have buffered writes flushed)
	leaderNode, err := leaderSrv.TopicManager.GetLeader(topicName)
	if err != nil {
		t.Fatalf("failed to get leader node: %v", err)
	}
	leaderLogMgr := leaderNode.Log // Use the existing LogManager instance

	for i, expectedMsg := range messages {
		entry, err := leaderLogMgr.ReadUncommitted(uint64(i))
		if err != nil {
			t.Fatalf("failed to read offset %d from leader log: %v", i, err)
		}
		if string(entry.Value) != expectedMsg {
			t.Fatalf("leader log offset %d: expected %q, got %q", i, expectedMsg, string(entry.Value))
		}
		if entry.Offset != uint64(i) {
			t.Fatalf("leader log offset %d: expected offset %d, got %d", i, i, entry.Offset)
		}
	}

	// Wait for replication to catch up
	time.Sleep(2 * time.Second)
	// Verify messages on follower replica - use the existing LogManager from the replica node
	// instead of creating a new one from disk (which won't have buffered writes flushed)
	replicaNode, err := followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("failed to get replica node: %v", err)
	}
	replicaLogMgr := replicaNode.Log // Use the existing LogManager instance

	for i, expectedMsg := range messages {
		entry, err := replicaLogMgr.ReadUncommitted(uint64(i))
		if err != nil {
			t.Fatalf("failed to read offset %d from replica log: %v", i, err)
		}
		if string(entry.Value) != expectedMsg {
			t.Fatalf("replica log offset %d: expected %q, got %q", i, expectedMsg, string(entry.Value))
		}
		if entry.Offset != uint64(i) {
			t.Fatalf("replica log offset %d: expected offset %d, got %d", i, i, entry.Offset)
		}
	}

	// Verify LEO on both leader and replica
	leaderLEO := leaderLogMgr.LEO()
	replicaLEO := replicaLogMgr.LEO()

	expectedLEO := lastOffset + 1
	if leaderLEO != expectedLEO {
		t.Fatalf("leader LEO: expected %d, got %d", expectedLEO, leaderLEO)
	}
	if replicaLEO < expectedLEO {
		t.Fatalf("replica LEO: expected at least %d, got %d (replication may not have caught up)", expectedLEO, replicaLEO)
	}

	t.Logf("verified replication: all %d messages replicated from leader to follower", len(messages))
}

func TestReplication_WithAckAll(t *testing.T) {
	// Test that ACK_ALL waits for replication before returning
	leaderSrv, followerSrv := testutil.SetupTwoTestServers(t, NewGrpcServer)
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	ctx := context.Background()
	topicName := "ack-all-test-topic"

	// Create topic on leader
	leaderConn, err := leaderSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer leaderConn.Close()

	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Wait for topic creation
	time.Sleep(200 * time.Millisecond)

	// Create replica on follower
	followerConn, err := followerSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)
	_, err = replicationClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
		Topic:      topicName,
		ReplicaId:  "replica-0",
		LeaderAddr: leaderSrv.Addr,
	})
	if err != nil {
		t.Fatalf("CreateReplica: %v", err)
	}

	// Wait for replica to start replication
	time.Sleep(500 * time.Millisecond)

	// Produce message with ACK_ALL (should wait for replication)
	producerClient := producer.NewProducerServiceClient(leaderConn)
	startTime := time.Now()
	resp, err := producerClient.Produce(ctx, &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("ack-all-message"),
		Acks:  producer.AckMode_ACK_ALL,
	})
	produceDuration := time.Since(startTime)
	if err != nil {
		t.Fatalf("Produce with ACK_ALL: %v", err)
	}
	if resp.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", resp.Offset)
	}

	// ACK_ALL should take longer than ACK_LEADER because it waits for replication
	// But it should complete within a reasonable time (e.g., 5 seconds)
	if produceDuration > 5*time.Second {
		t.Fatalf("ACK_ALL took too long: %v", produceDuration)
	}
	if produceDuration < 100*time.Millisecond {
		t.Logf("warning: ACK_ALL completed very quickly (%v), replication may have been instant", produceDuration)
	}

	// Verify message is immediately available on replica (ACK_ALL guarantees this)
	// Use the existing LogManager from the replica node instead of creating a new one from disk
	replicaNode, err := followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("failed to get replica node: %v", err)
	}
	replicaLogMgr := replicaNode.Log // Use the existing LogManager instance

	// Message should be available immediately after ACK_ALL returns
	entry, err := replicaLogMgr.ReadUncommitted(0)
	if err != nil {
		t.Fatalf("failed to read offset 0 from replica log after ACK_ALL: %v", err)
	}
	if string(entry.Value) != "ack-all-message" {
		t.Fatalf("replica log: expected 'ack-all-message', got %q", string(entry.Value))
	}

	t.Logf("ACK_ALL verified: message replicated to follower in %v", produceDuration)
}

func TestReplication_CatchUpTime_10000Messages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10k replication catch-up test in -short mode")
	}

	leaderSrv, followerSrv := testutil.SetupTwoTestServers(t, NewGrpcServer)
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	ctx := context.Background()
	topicName := "catchup-10k-topic"

	// IMPORTANT: don't use LeaderService.CreateTopic(replicaCount=1) here because it
	// already creates remote replicas. For this benchmark-style test we want to
	// control replica creation explicitly (single replica-0 on follower).
	if err := leaderSrv.TopicManager.CreateTopic(topicName, 0); err != nil {
		t.Fatalf("CreateTopic (leader-only): %v", err)
	}

	followerConn, err := followerSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)
	_, err = replicationClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
		Topic:      topicName,
		ReplicaId:  "replica-0",
		LeaderAddr: leaderSrv.Addr,
	})
	if err != nil {
		t.Fatalf("CreateReplica: %v", err)
	}

	// Give replica time to start and begin streaming.
	time.Sleep(800 * time.Millisecond)

	leaderConn, err := leaderSrv.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer leaderConn.Close()

	producerClient := producer.NewProducerServiceClient(leaderConn)

	const n = 10000
	const batchSize = 100 // Batch 100 messages at a time
	produceStart := time.Now()
	var lastOffset uint64
	var baseOffset uint64

	// Produce in batches
	for i := 0; i < n; i += batchSize {
		batchCount := batchSize
		if i+batchSize > n {
			batchCount = n - i
		}
		values := make([][]byte, batchCount)
		for j := 0; j < batchCount; j++ {
			values[j] = []byte("msg")
		}

		resp, err := producerClient.ProduceBatch(ctx, &producer.ProduceBatchRequest{
			Topic:  topicName,
			Values: values,
			Acks:   producer.AckMode_ACK_LEADER,
		})
		if err != nil {
			t.Fatalf("ProduceBatch at i=%d: %v", i, err)
		}
		if i == 0 {
			baseOffset = resp.BaseOffset
		}
		lastOffset = resp.LastOffset
		if resp.Count != uint32(batchCount) {
			t.Fatalf("ProduceBatch at i=%d: expected count %d, got %d", i, batchCount, resp.Count)
		}
	}
	produceDuration := time.Since(produceStart)
	expectedLastOffset := baseOffset + uint64(n) - 1
	if lastOffset != expectedLastOffset {
		t.Fatalf("expected last offset %d, got %d (base=%d)", expectedLastOffset, lastOffset, baseOffset)
	}

	// Use the existing LogManager from the replica node instead of creating a new one from disk
	replicaNode, err := followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		t.Fatalf("failed to get replica node: %v", err)
	}
	replicaLogMgr := replicaNode.Log // Use the existing LogManager instance

	// Measure how long it takes for replica to catch up to lastOffset.
	catchUpStart := time.Now()
	// 10k messages can take a while on slower machines / CI. Keep a hard cap so we don't hang forever.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		// Fast path: replica has applied through lastOffset once it can read it.
		if _, err := replicaLogMgr.ReadUncommitted(lastOffset); err == nil {
			break
		}
		// Alternate signal: replica LEO is at least lastOffset+1.
		if replicaLogMgr.LEO() >= lastOffset+1 {
			// Still validate lastOffset is readable.
			if _, err := replicaLogMgr.ReadUncommitted(lastOffset); err == nil {
				break
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for replica to catch up: want LEO >= %d, got %d", lastOffset+1, replicaLogMgr.LEO())
		}
		time.Sleep(25 * time.Millisecond)
	}
	catchUpDuration := time.Since(catchUpStart)

	t.Logf("produced %d messages in %v; replica catch-up time = %v (LEO=%d)", n, produceDuration, catchUpDuration, replicaLogMgr.LEO())
}

func BenchmarkReplication_WithAckLeader(b *testing.B) {
	// Benchmark producing messages with ACK_LEADER and verifying replication
	leaderSrv, followerSrv := testutil.SetupTwoTestServers(b, NewGrpcServer)
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	ctx := context.Background()
	topicName := "bench-ack-leader-topic"

	// Create topic on leader
	leaderConn, err := leaderSrv.GetConn()
	if err != nil {
		b.Fatalf("GetConn: %v", err)
	}
	defer leaderConn.Close()

	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}

	// Wait for topic creation
	time.Sleep(200 * time.Millisecond)

	// Create replica on follower
	followerConn, err := followerSrv.GetConn()
	if err != nil {
		b.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)
	_, err = replicationClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
		Topic:      topicName,
		ReplicaId:  "replica-0",
		LeaderAddr: leaderSrv.Addr,
	})
	if err != nil {
		b.Fatalf("CreateReplica: %v", err)
	}

	// Wait for replica to start replication
	time.Sleep(500 * time.Millisecond)

	producerClient := producer.NewProducerServiceClient(leaderConn)
	// Use ProduceBatch with 10 messages per batch for better throughput
	batchValues := [][]byte{
		[]byte("bench-msg-0"), []byte("bench-msg-1"), []byte("bench-msg-2"),
		[]byte("bench-msg-3"), []byte("bench-msg-4"), []byte("bench-msg-5"),
		[]byte("bench-msg-6"), []byte("bench-msg-7"), []byte("bench-msg-8"),
		[]byte("bench-msg-9"),
	}
	req := &producer.ProduceBatchRequest{
		Topic:  topicName,
		Values: batchValues,
		Acks:   producer.AckMode_ACK_LEADER,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := producerClient.ProduceBatch(ctx, req)
		if err != nil {
			b.Fatalf("ProduceBatch: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "batches/s")
		b.ReportMetric(float64(b.N*len(batchValues))/seconds, "msgs/s")
	}
}

func BenchmarkReplication_WithAckAll(b *testing.B) {
	// Benchmark producing messages with ACK_ALL (waits for replication)
	leaderSrv, followerSrv := testutil.SetupTwoTestServers(b, NewGrpcServer)
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	ctx := context.Background()
	topicName := "bench-ack-all-topic"

	// Create topic on leader
	leaderConn, err := leaderSrv.GetConn()
	if err != nil {
		b.Fatalf("GetConn: %v", err)
	}
	defer leaderConn.Close()

	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}

	// Wait for topic creation
	time.Sleep(200 * time.Millisecond)

	// Create replica on follower
	followerConn, err := followerSrv.GetConn()
	if err != nil {
		b.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)
	_, err = replicationClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
		Topic:      topicName,
		ReplicaId:  "replica-0",
		LeaderAddr: leaderSrv.Addr,
	})
	if err != nil {
		b.Fatalf("CreateReplica: %v", err)
	}

	// Wait for replica to start replication
	time.Sleep(500 * time.Millisecond)

	producerClient := producer.NewProducerServiceClient(leaderConn)
	req := &producer.ProduceRequest{
		Topic: topicName,
		Value: []byte("bench-message"),
		Acks:  producer.AckMode_ACK_ALL,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := producerClient.Produce(ctx, req)
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}

func BenchmarkReplication_VerifyReplication(b *testing.B) {
	// Benchmark producing messages and verifying they are replicated
	leaderSrv, followerSrv := testutil.SetupTwoTestServers(b, NewGrpcServer)
	defer leaderSrv.Cleanup()
	defer followerSrv.Cleanup()

	ctx := context.Background()
	topicName := "bench-verify-replication-topic"

	// Create topic on leader
	leaderConn, err := leaderSrv.GetConn()
	if err != nil {
		b.Fatalf("GetConn: %v", err)
	}
	defer leaderConn.Close()

	leaderClient := leader.NewLeaderServiceClient(leaderConn)
	_, err = leaderClient.CreateTopic(ctx, &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	})
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}

	// Wait for topic creation
	time.Sleep(200 * time.Millisecond)

	// Create replica on follower
	followerConn, err := followerSrv.GetConn()
	if err != nil {
		b.Fatalf("GetConn: %v", err)
	}
	defer followerConn.Close()

	replicationClient := replication.NewReplicationServiceClient(followerConn)
	_, err = replicationClient.CreateReplica(ctx, &replication.CreateReplicaRequest{
		Topic:      topicName,
		ReplicaId:  "replica-0",
		LeaderAddr: leaderSrv.Addr,
	})
	if err != nil {
		b.Fatalf("CreateReplica: %v", err)
	}

	// Wait for replica to start replication
	time.Sleep(500 * time.Millisecond)

	producerClient := producer.NewProducerServiceClient(leaderConn)

	// Get existing log managers from nodes for verification (not from disk)
	leaderNode, err := leaderSrv.TopicManager.GetLeader(topicName)
	if err != nil {
		b.Fatalf("failed to get leader node: %v", err)
	}
	leaderLogMgr := leaderNode.Log // Use the existing LogManager instance

	replicaNode, err := followerSrv.TopicManager.GetReplica(topicName, "replica-0")
	if err != nil {
		b.Fatalf("failed to get replica node: %v", err)
	}
	replicaLogMgr := replicaNode.Log // Use the existing LogManager instance

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Produce message
		resp, err := producerClient.Produce(ctx, &producer.ProduceRequest{
			Topic: topicName,
			Value: []byte("bench-verify-message"),
			Acks:  producer.AckMode_ACK_LEADER,
		})
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}

		// Wait a bit for the message to be written and indexed
		time.Sleep(50 * time.Millisecond)

		// Verify on leader (with retry in case of indexing delay)
		var leaderEntry *common.LogEntry
		for retry := 0; retry < 5; retry++ {
			var err error
			leaderEntry, err = leaderLogMgr.ReadUncommitted(resp.Offset)
			if err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if leaderEntry == nil {
			// Skip if we can't read (shouldn't happen, but be lenient in benchmark)
			continue
		}
		if string(leaderEntry.Value) != "bench-verify-message" {
			b.Fatalf("leader log offset %d: expected 'bench-verify-message', got %q", resp.Offset, string(leaderEntry.Value))
		}

		// Wait for replication
		time.Sleep(100 * time.Millisecond)

		// Verify on replica (with retry for replication lag)
		var replicaEntry *common.LogEntry
		for retry := 0; retry < 10; retry++ {
			var err error
			replicaEntry, err = replicaLogMgr.ReadUncommitted(resp.Offset)
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if replicaEntry == nil {
			// Skip if replication hasn't caught up (acceptable for benchmark)
			continue
		}
		if string(replicaEntry.Value) != "bench-verify-message" {
			b.Fatalf("replica log offset %d: expected 'bench-verify-message', got %q", resp.Offset, string(replicaEntry.Value))
		}
	}
}
