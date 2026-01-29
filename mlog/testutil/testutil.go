package testutil

import (
	"net"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/mohitkumar/mlog/broker"
	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestServerComponents contains the components needed to create a test server.
// The caller is responsible for creating and starting the gRPC server.
type TestServerComponents struct {
	TopicManager    *node.TopicManager
	ConsumerManager *consumermgr.ConsumerManager
	BrokerManager   *broker.BrokerManager
	Broker          *broker.Broker
	BaseDir         string
}

// TestServer represents a test gRPC server with its components.
type TestServer struct {
	*TestServerComponents
	GrpcServer *grpc.Server
	Listener   net.Listener
	Addr       string
	conn       *grpc.ClientConn
	connMu     sync.Mutex
}

// Cleanup closes all resources associated with the test server.
func (ts *TestServer) Cleanup() {
	ts.connMu.Lock()
	if ts.conn != nil {
		_ = ts.conn.Close()
		ts.conn = nil
	}
	ts.connMu.Unlock()

	if ts.GrpcServer != nil {
		ts.GrpcServer.Stop()
	}
	if ts.Listener != nil {
		_ = ts.Listener.Close()
	}
	if ts.Broker != nil {
		_ = ts.Broker.Close()
	}
}

// GetConn returns a gRPC client connection to the test server, reusing the connection if available.
func (ts *TestServer) GetConn() (*grpc.ClientConn, error) {
	ts.connMu.Lock()
	defer ts.connMu.Unlock()

	if ts.conn != nil {
		return ts.conn, nil
	}

	conn, err := grpc.NewClient(ts.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	ts.conn = conn
	return ts.conn, nil
}

// SetupTestServerComponents creates the components needed for a test server.
// The caller must create and start the gRPC server using these components.
func SetupTestServerComponents(t testing.TB, nodeID string, baseDirSuffix string) *TestServerComponents {
	t.Helper()

	baseDir := path.Join(t.TempDir(), baseDirSuffix)
	bm := broker.NewBrokerManager()
	b := broker.NewBroker(nodeID, "127.0.0.1:0")
	bm.AddBroker(b)

	topicMgr, err := node.NewTopicManager(baseDir, bm, b)
	if err != nil {
		t.Fatalf("NewTopicManager: %v", err)
	}

	consumerMgr, err := consumermgr.NewConsumerManager(baseDir)
	if err != nil {
		t.Fatalf("NewConsumerManager: %v", err)
	}

	return &TestServerComponents{
		TopicManager:    topicMgr,
		ConsumerManager: consumerMgr,
		BrokerManager:   bm,
		Broker:          b,
		BaseDir:         baseDir,
	}
}

// StartTestServer starts a gRPC server using the provided components and server factory.
// The factory function should create and register all services on the server.
func StartTestServer(t testing.TB, comps *TestServerComponents, serverFactory func(*node.TopicManager, *consumermgr.ConsumerManager) (*grpc.Server, error)) *TestServer {
	t.Helper()

	gsrv, err := serverFactory(comps.TopicManager, comps.ConsumerManager)
	if err != nil {
		t.Fatalf("serverFactory: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	comps.Broker.Addr = ln.Addr().String()

	go func() {
		_ = gsrv.Serve(ln)
	}()

	// Wait for server to be ready
	time.Sleep(100 * time.Millisecond)

	return &TestServer{
		TestServerComponents: comps,
		GrpcServer:           gsrv,
		Listener:             ln,
		Addr:                 ln.Addr().String(),
	}
}

// SetupTestServerWithTopic creates a test server and creates a topic with the specified replica count.
func SetupTestServerWithTopic(t testing.TB, nodeID string, baseDirSuffix string, topic string, replicaCount int, serverFactory func(*node.TopicManager, *consumermgr.ConsumerManager) (*grpc.Server, error)) *TestServer {
	t.Helper()

	comps := SetupTestServerComponents(t, nodeID, baseDirSuffix)
	if err := comps.TopicManager.CreateTopic(topic, replicaCount); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	return StartTestServer(t, comps, serverFactory)
}

// SetupTwoTestServers creates two test servers (useful for replication tests).
// Returns leader and follower servers.
func SetupTwoTestServers(t testing.TB, serverFactory func(*node.TopicManager, *consumermgr.ConsumerManager) (*grpc.Server, error)) (*TestServer, *TestServer) {
	t.Helper()

	leaderComps := SetupTestServerComponents(t, "node-1", "leader")
	followerComps := SetupTestServerComponents(t, "node-2", "follower")

	// Add each broker to the other's broker manager so they can communicate
	leaderComps.BrokerManager.AddBroker(followerComps.Broker)
	followerComps.BrokerManager.AddBroker(leaderComps.Broker)

	leader := StartTestServer(t, leaderComps, serverFactory)
	follower := StartTestServer(t, followerComps, serverFactory)

	return leader, follower
}
