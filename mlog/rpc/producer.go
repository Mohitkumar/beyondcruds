package rpc

import (
	"context"
	"fmt"

	"github.com/mohitkumar/mlog/api/common"
	"github.com/mohitkumar/mlog/api/producer"
)

var _ producer.ProducerServiceServer = (*grpcServer)(nil)

func (srv *grpcServer) Produce(ctx context.Context, req *producer.ProduceRequest) (*producer.ProduceResponse, error) {
	topicObj, err := srv.topicManager.GetTopic(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("topic %s not found: %w", req.Topic, err)
	}

	offset, err := topicObj.HandleProduce(ctx, &common.LogEntry{
		Value: req.Value,
	}, req.Acks)
	if err != nil {
		return nil, err
	}
	return &producer.ProduceResponse{Offset: offset}, err
}
