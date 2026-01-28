package tests

import (
	"context"
	"fmt"
	"net"
	"path"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/api/leader"
	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/replication"
	"github.com/mohitkumar/mlog/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testServers struct {
	leaderAddr   string
	followerAddr string
	leaderConn   *grpc.ClientConn
	followerConn *grpc.ClientConn
	cleanup      func()
}

// setupServer creates and starts a gRPC server for a broker
func setupServer(t *testing.T, bm *broker.BrokerManager, baseDir string, nodeID string, serverName string) (*grpc.Server, net.Listener, error) {
	t.Helper()
	broker := bm.GetBroker(nodeID)
	if broker == nil {
		return nil, nil, fmt.Errorf("broker %s not found", nodeID)
	}

	topicMgr, err := replication.NewTopicManager(baseDir, bm, broker)
	if err != nil {
		return nil, nil, fmt.Errorf("create topic manager: %w", err)
	}

	consumerMgr, err := consumer.NewConsumerManager(baseDir)
	if err != nil {
		return nil, nil, fmt.Errorf("create consumer manager: %w", err)
	}

	ln, err := net.Listen("tcp", broker.Addr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen %s: %w", broker.Addr, err)
	}

	gsrv, err := rpc.NewGrpcServer(topicMgr, consumerMgr)
	if err != nil {
		ln.Close()
		return nil, nil, fmt.Errorf("create grpc server: %w", err)
	}

	go func() {
		if err := gsrv.Serve(ln); err != nil {
			t.Logf("%s server error: %v", serverName, err)
		}
	}()

	return gsrv, ln, nil
}

func setupLeaderServer(t *testing.T, bm *broker.BrokerManager, baseDir string) (*grpc.Server, net.Listener, error) {
	return setupServer(t, bm, baseDir, "node-1", "leader")
}

func setupFollowerServer(t *testing.T, bm *broker.BrokerManager, baseDir string) (*grpc.Server, net.Listener, error) {
	return setupServer(t, bm, baseDir, "node-2", "follower")
}

func setupTestServers(t *testing.T) *testServers {
	t.Helper()

	leaderAddr := "localhost:9000"
	followerAddr := "localhost:9001"

	// Create initial broker manager with placeholder brokers (for server setup)
	// We'll recreate them with proper connections after servers start
	bm := broker.NewBrokerManager()

	leaderBaseDir := path.Join(t.TempDir(), "leader")
	followerBaseDir := path.Join(t.TempDir(), "follower")

	// Create brokers for topic managers (no connection yet, will be created lazily)
	leaderBrokerForTM := broker.NewBroker("node-1", leaderAddr)
	followerBrokerForTM := broker.NewBroker("node-2", followerAddr)

	bm.AddBroker(leaderBrokerForTM)
	bm.AddBroker(followerBrokerForTM)

	leaderSrv, leaderLn, err := setupLeaderServer(t, bm, leaderBaseDir)
	if err != nil {
		t.Fatalf("setupLeaderServer error: %v", err)
	}

	followerSrv, followerLn, err := setupFollowerServer(t, bm, followerBaseDir)
	if err != nil {
		leaderSrv.Stop()
		leaderLn.Close()
		t.Fatalf("setupFollowerServer error: %v", err)
	}

	// Wait for servers to be ready
	time.Sleep(300 * time.Millisecond)

	// Now establish connections to brokers (retry until servers are ready)
	maxRetries := 10
	var connectErr error
	for i := 0; i < maxRetries; i++ {
		connectErr = leaderBrokerForTM.Connect()
		if connectErr == nil {
			connectErr = followerBrokerForTM.Connect()
			if connectErr == nil {
				break
			}
		}
		if i < maxRetries-1 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if connectErr != nil {
		leaderBrokerForTM.Close()
		followerBrokerForTM.Close()
		leaderSrv.Stop()
		followerSrv.Stop()
		leaderLn.Close()
		followerLn.Close()
		t.Fatalf("failed to connect to brokers: %v", connectErr)
	}

	// Create new broker instances for test client connections
	leaderBroker := broker.NewBroker("node-1", leaderAddr)
	followerBroker := broker.NewBroker("node-2", followerAddr)
	
	// Connect them
	if err := leaderBroker.Connect(); err != nil {
		leaderBrokerForTM.Close()
		followerBrokerForTM.Close()
		leaderSrv.Stop()
		followerSrv.Stop()
		leaderLn.Close()
		followerLn.Close()
		t.Fatalf("failed to connect leader broker: %v", err)
	}
	if err := followerBroker.Connect(); err != nil {
		leaderBroker.Close()
		leaderBrokerForTM.Close()
		followerBrokerForTM.Close()
		leaderSrv.Stop()
		followerSrv.Stop()
		leaderLn.Close()
		followerLn.Close()
		t.Fatalf("failed to connect follower broker: %v", err)
	}

	// Create client connections for test use
	leaderConn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		leaderBroker.Close()
		followerBroker.Close()
		leaderSrv.Stop()
		followerSrv.Stop()
		leaderLn.Close()
		followerLn.Close()
		t.Fatalf("dial leader error: %v", err)
	}

	followerConn, err := grpc.NewClient(followerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		leaderConn.Close()
		leaderBroker.Close()
		followerBroker.Close()
		leaderSrv.Stop()
		followerSrv.Stop()
		leaderLn.Close()
		followerLn.Close()
		t.Fatalf("dial follower error: %v", err)
	}

	return &testServers{
		leaderAddr:   leaderAddr,
		followerAddr: followerAddr,
		leaderConn:   leaderConn,
		followerConn: followerConn,
		cleanup: func() {
			leaderConn.Close()
			followerConn.Close()
			leaderBroker.Close()
			followerBroker.Close()
			leaderSrv.Stop()
			followerSrv.Stop()
			leaderLn.Close()
			followerLn.Close()
		},
	}
}

func TestCreateTopic(t *testing.T) {
	servers := setupTestServers(t)
	defer servers.cleanup()

	ctx := context.Background()
	client := leader.NewLeaderServiceClient(servers.leaderConn)

	// Test creating a topic with 1 replica (leader + 1 follower)
	topicName := "test-topic-1"
	req := &leader.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 1,
	}

	resp, err := client.CreateTopic(ctx, req)
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}

	if resp.Topic != topicName {
		t.Fatalf("expected topic %s, got %s", topicName, resp.Topic)
	}

	// Verify topic was created by trying to create it again (should fail)
	_, err = client.CreateTopic(ctx, req)
	if err == nil {
		t.Fatal("expected error when creating duplicate topic, got nil")
	}
}
