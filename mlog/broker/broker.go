package broker

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Broker struct {
	NodeID string
	Addr   string
	mu     sync.Mutex
	conn   *grpc.ClientConn
}

func NewBroker(nodeID string, addr string) *Broker {
	return &Broker{
		NodeID: nodeID,
		Addr:   addr,
	}
}

// GetConn returns the gRPC client connection, creating it if necessary.
// This method is thread-safe and will lazily establish the connection
// on first call.
func (b *Broker) GetConn() (*grpc.ClientConn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn != nil {
		return b.conn, nil
	}

	conn, err := grpc.NewClient(b.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	b.conn = conn
	return b.conn, nil
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn != nil {
		err := b.conn.Close()
		b.conn = nil
		return err
	}
	return nil
}

func (b *Broker) GetAddr() string {
	return b.Addr
}

type BrokerManager struct {
	brokers map[string]*Broker
}

func NewBrokerManager() *BrokerManager {
	return &BrokerManager{
		brokers: make(map[string]*Broker),
	}
}

func (bm *BrokerManager) AddBroker(broker *Broker) {
	bm.brokers[broker.NodeID] = broker
}

func (bm *BrokerManager) GetBroker(nodeID string) *Broker {
	return bm.brokers[nodeID]
}

func (bm *BrokerManager) RemoveBroker(nodeID string) {
	delete(bm.brokers, nodeID)
}

func (bm *BrokerManager) GetAllBrokers() []*Broker {
	brokers := make([]*Broker, 0, len(bm.brokers))
	for _, broker := range bm.brokers {
		brokers = append(brokers, broker)
	}
	return brokers
}

// GetBrokerByAddr finds a broker by its address
func (bm *BrokerManager) GetBrokerByAddr(addr string) *Broker {
	for _, b := range bm.brokers {
		if b.Addr == addr {
			return b
		}
	}
	return nil
}
