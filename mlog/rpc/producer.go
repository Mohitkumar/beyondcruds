package rpc

import (
	"context"

	"github.com/mohitkumar/mlog/api/common"
	"github.com/mohitkumar/mlog/api/producer"
)

var _ producer.ProducerServiceServer = (*grpcServer)(nil)

func (srv *grpcServer) Produce(ctx context.Context, req *producer.ProduceRequest) (*producer.ProduceResponse, error) {
	leader, err := srv.topicManager.GetLeader(req.Topic)
	if err != nil {
		return nil, err
	}
	offset, err := leader.HandleProduce(ctx, &common.LogEntry{
		Value: req.Value,
	}, req.Acks)
	if err != nil {
		return nil, err
	}
	return &producer.ProduceResponse{Offset: offset}, err
}
