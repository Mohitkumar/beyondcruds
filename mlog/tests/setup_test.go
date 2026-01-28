package tests

import (
	"fmt"
	"net"
	"path"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/rpc"
	"google.golang.org/grpc"
)

type testServers struct {
	leaderBroker    *broker.Broker
	followerBroker  *broker.Broker
	leaderBaseDir   string
	followerBaseDir string
	cleanup         func()
}

func (ts *testServers) getLeaderConn() (*grpc.ClientConn, error) {
	return ts.leaderBroker.GetConn()
}

func (ts *testServers) getFollowerConn() (*grpc.ClientConn, error) {
	return ts.followerBroker.GetConn()
}

// setupServer creates and starts a gRPC server for a broker on a pre-bound listener.
func setupServer(t *testing.T, bm *broker.BrokerManager, baseDir string, nodeID string, serverName string, ln net.Listener) (*grpc.Server, net.Listener, error) {
	t.Helper()

	b := bm.GetBroker(nodeID)
	if b == nil {
		return nil, nil, fmt.Errorf("broker %s not found", nodeID)
	}

	topicMgr, err := node.NewTopicManager(baseDir, bm, b)
	if err != nil {
		return nil, nil, fmt.Errorf("create topic manager: %w", err)
	}

	consumerMgr, err := consumer.NewConsumerManager(baseDir)
	if err != nil {
		return nil, nil, fmt.Errorf("create consumer manager: %w", err)
	}

	gsrv, err := rpc.NewGrpcServer(topicMgr, consumerMgr)
	if err != nil {
		_ = ln.Close()
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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	return setupServer(t, bm, baseDir, "node-1", "leader", ln)
}

func setupFollowerServer(t *testing.T, bm *broker.BrokerManager, baseDir string) (*grpc.Server, net.Listener, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	return setupServer(t, bm, baseDir, "node-2", "follower", ln)
}

func setupTestServers(t *testing.T) *testServers {
	t.Helper()

	// We'll allocate ports by starting servers first, then set broker addrs to match.
	leaderAddr := "127.0.0.1:0"
	followerAddr := "127.0.0.1:0"

	// Create broker manager with brokers (connections are created lazily).
	bm := broker.NewBrokerManager()
	leaderBroker := broker.NewBroker("node-1", leaderAddr)
	followerBroker := broker.NewBroker("node-2", followerAddr)
	bm.AddBroker(leaderBroker)
	bm.AddBroker(followerBroker)

	leaderBaseDir := path.Join(t.TempDir(), "leader")
	followerBaseDir := path.Join(t.TempDir(), "follower")

	leaderSrv, leaderLn, err := setupLeaderServer(t, bm, leaderBaseDir)
	if err != nil {
		t.Fatalf("setupLeaderServer error: %v", err)
	}
	leaderBroker.Addr = leaderLn.Addr().String()

	followerSrv, followerLn, err := setupFollowerServer(t, bm, followerBaseDir)
	if err != nil {
		leaderSrv.Stop()
		_ = leaderLn.Close()
		t.Fatalf("setupFollowerServer error: %v", err)
	}
	followerBroker.Addr = followerLn.Addr().String()

	// Wait for servers to be ready.
	time.Sleep(300 * time.Millisecond)

	return &testServers{
		leaderBroker:    leaderBroker,
		followerBroker:  followerBroker,
		leaderBaseDir:   leaderBaseDir,
		followerBaseDir: followerBaseDir,
		cleanup: func() {
			_ = leaderBroker.Close()
			_ = followerBroker.Close()
			leaderSrv.Stop()
			followerSrv.Stop()
			_ = leaderLn.Close()
			_ = followerLn.Close()
		},
	}
}
