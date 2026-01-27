package broker

import "google.golang.org/grpc"

type Broker struct {
	NodeID string
	Addr   string
	conn   *grpc.ClientConn
}

func NewBroker(nodeID string, addr string) (*Broker, error) {
	b := &Broker{
		NodeID: nodeID,
		Addr:   addr,
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	b.conn = conn
	return b, nil
}

func (b *Broker) Close() error {
	return b.conn.Close()
}

func (b *Broker) GetConn() *grpc.ClientConn {
	return b.conn
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
