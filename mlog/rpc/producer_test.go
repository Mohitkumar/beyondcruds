package rpc

import (
	"context"
	"testing"

	"github.com/mohitkumar/mlog/api/producer"
	"github.com/mohitkumar/mlog/testutil"
)

func TestProduce(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, NewGrpcServer)
	defer ts.Cleanup()

	ctx := context.Background()
	conn, err := ts.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer conn.Close()

	client := producer.NewProducerServiceClient(conn)
	resp, err := client.Produce(ctx, &producer.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("hello"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if resp.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", resp.Offset)
	}

	resp2, err := client.Produce(ctx, &producer.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("world"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("Produce second: %v", err)
	}
	if resp2.Offset != 1 {
		t.Fatalf("expected offset 1, got %d", resp2.Offset)
	}
}

func TestProduce_TopicNotFound(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, NewGrpcServer)
	defer ts.Cleanup()

	ctx := context.Background()
	conn, err := ts.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer conn.Close()

	client := producer.NewProducerServiceClient(conn)
	_, err = client.Produce(ctx, &producer.ProduceRequest{
		Topic: "nonexistent-topic",
		Value: []byte("x"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err == nil {
		t.Fatal("expected error for unknown topic")
	}
}

func TestProduce_WithAckLeader(t *testing.T) {
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, NewGrpcServer)
	defer ts.Cleanup()

	ctx := context.Background()
	conn, err := ts.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer conn.Close()

	client := producer.NewProducerServiceClient(conn)
	resp, err := client.Produce(ctx, &producer.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("ack-leader"),
		Acks:  producer.AckMode_ACK_LEADER,
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if resp.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", resp.Offset)
	}
}

func TestProduce_WithAckAll_NoReplicas(t *testing.T) {
	// With 0 replicas, ACK_ALL behaves like ACK_LEADER (returns immediately).
	ts := testutil.SetupTestServerWithTopic(t, "node-1", "producer-test", "test-topic", 0, NewGrpcServer)
	defer ts.Cleanup()

	ctx := context.Background()
	conn, err := ts.GetConn()
	if err != nil {
		t.Fatalf("GetConn: %v", err)
	}
	defer conn.Close()

	client := producer.NewProducerServiceClient(conn)
	resp, err := client.Produce(ctx, &producer.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("ack-all"),
		Acks:  producer.AckMode_ACK_ALL,
	})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if resp.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", resp.Offset)
	}
}

func BenchmarkProduce(b *testing.B) {
	ts := testutil.SetupTestServerWithTopic(b, "node-1", "producer-bench", "test-topic", 0, NewGrpcServer)
	defer ts.Cleanup()

	conn, err := ts.GetConn()
	if err != nil {
		b.Fatalf("GetConn: %v", err)
	}
	defer conn.Close()

	client := producer.NewProducerServiceClient(conn)
	ctx := context.Background()
	req := &producer.ProduceRequest{
		Topic: "test-topic",
		Value: []byte("bench-value"),
		Acks:  producer.AckMode_ACK_LEADER,
	}

	for b.Loop() {
		_, err := client.Produce(ctx, req)
		if err != nil {
			b.Fatalf("Produce: %v", err)
		}
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(b.N)/seconds, "req/s")
	}
}
