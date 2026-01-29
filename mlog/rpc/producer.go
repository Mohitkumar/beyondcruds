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

func (srv *grpcServer) ProduceBatch(ctx context.Context, req *producer.ProduceBatchRequest) (*producer.ProduceBatchResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if len(req.Values) == 0 {
		return nil, fmt.Errorf("values are required")
	}

	topicObj, err := srv.topicManager.GetTopic(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("topic %s not found: %w", req.Topic, err)
	}

	base, last, err := topicObj.HandleProduceBatch(ctx, req.Values, req.Acks)
	if err != nil {
		return nil, err
	}
	return &producer.ProduceBatchResponse{
		BaseOffset: base,
		LastOffset: last,
		Count:      uint32(len(req.Values)),
	}, nil
}
