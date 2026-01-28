package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mohitkumar/mlog/api/common"
	"github.com/mohitkumar/mlog/broker"
)

// TestTopicManager_CreateTopicLeaderOnly verifies that creating a topic
// with zero replicas sets up a leader and a backing log that can be written/read.
func TestTopicManager_CreateTopicLeaderOnly(t *testing.T) {
	dir := t.TempDir()

	bm := broker.NewBrokerManager()
	self := &broker.Broker{
		NodeID: "node-1",
		Addr:   "127.0.0.1:19092", // not used when replicaCount == 0
	}
	bm.AddBroker(self)

	tm, err := NewTopicManager(dir, bm, self)
	if err != nil {
		t.Fatalf("NewTopicManager error = %v", err)
	}

	const topic = "test-topic"

	if err := tm.CreateTopic(topic, 0); err != nil {
		t.Fatalf("CreateTopic error = %v", err)
	}

	leader, err := tm.GetLeader(topic)
	if err != nil {
		t.Fatalf("GetLeader error = %v", err)
	}
	if leader == nil || leader.Log == nil {
		t.Fatalf("expected non-nil leader and log")
	}

	// Append a record and read it back.
	wantVal := []byte("hello-replication")
	off, err := leader.Log.Append(&common.LogEntry{Value: wantVal})
	if err != nil {
		t.Fatalf("Append error = %v", err)
	}
	if off != 0 {
		t.Fatalf("expected first offset 0, got %d", off)
	}

	got, err := leader.Log.Read(off)
	if err != nil {
		t.Fatalf("Read error = %v", err)
	}
	if string(got.Value) != string(wantVal) {
		t.Fatalf("Read value = %q, want %q", got.Value, wantVal)
	}
}

// TestTopicManager_DeleteTopic verifies that DeleteTopic removes the topic,
// closes and deletes the leader log directory.
func TestTopicManager_DeleteTopic(t *testing.T) {
	dir := t.TempDir()

	bm := broker.NewBrokerManager()
	self := &broker.Broker{
		NodeID: "node-1",
		Addr:   "127.0.0.1:19092",
	}
	bm.AddBroker(self)

	tm, err := NewTopicManager(dir, bm, self)
	if err != nil {
		t.Fatalf("NewTopicManager error = %v", err)
	}

	const topic = "delete-topic"

	if err := tm.CreateTopic(topic, 0); err != nil {
		t.Fatalf("CreateTopic error = %v", err)
	}

	// Ensure log directory exists.
	logDir := filepath.Join(dir, topic)
	if _, err := os.Stat(logDir); err != nil {
		t.Fatalf("expected log dir %s to exist, got error: %v", logDir, err)
	}

	if err := tm.DeleteTopic(topic); err != nil {
		t.Fatalf("DeleteTopic error = %v", err)
	}

	// Leader should no longer be found.
	if _, err := tm.GetLeader(topic); err == nil {
		t.Fatalf("expected GetLeader to fail after DeleteTopic")
	}

	// Log directory should be removed.
	if _, err := os.Stat(logDir); !os.IsNotExist(err) {
		t.Fatalf("expected log dir %s to be removed, got err = %v", logDir, err)
	}
}
