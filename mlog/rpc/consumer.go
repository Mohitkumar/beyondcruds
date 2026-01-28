package rpc

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/api/consumer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ consumer.ConsumerServiceServer = (*grpcServer)(nil)

func (s *grpcServer) Fetch(ctx context.Context, req *consumer.FetchRequest) (*consumer.FetchResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	id := req.Id
	if id == "" {
		id = "default"
	}

	off := req.Offset
	if off == 0 {
		// try cache; if missing, default to 0
		if err := s.consumerManager.Recover(); err == nil {
			if cached, err := s.consumerManager.GetOffset(id, req.Topic); err == nil {
				off = cached
			}
		}
	}

	leaderNode, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "topic %s not found: %v", req.Topic, err)
	}

	entry, err := leaderNode.Log.Read(off)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read offset %d: %v", off, err)
	}

	return &consumer.FetchResponse{
		Entry: entry,
	}, nil
}

func (s *grpcServer) FetchStream(req *consumer.FetchRequest, stream consumer.ConsumerService_FetchStreamServer) error {
	if req.Topic == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}
	id := req.Id
	if id == "" {
		id = "default"
	}

	leaderNode, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return status.Errorf(codes.NotFound, "topic %s not found: %v", req.Topic, err)
	}

	off := req.Offset
	if off == 0 {
		if err := s.consumerManager.Recover(); err == nil {
			if cached, err := s.consumerManager.GetOffset(id, req.Topic); err == nil {
				off = cached
			}
		}
	}

	ctx := stream.Context()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			entry, err := leaderNode.Log.Read(off)
			if err != nil {
				// likely out of range; wait for more data
				continue
			}

			if err := stream.Send(&consumer.FetchResponse{Entry: entry}); err != nil {
				return status.Errorf(codes.Internal, "failed to send: %v", err)
			}

			off = entry.Offset + 1
		}
	}
}

func (s *grpcServer) CommitOffset(ctx context.Context, req *consumer.CommitOffsetRequest) (*consumer.CommitOffsetResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	id := req.Id
	if id == "" {
		id = "default"
	}

	if err := s.consumerManager.CommitOffset(id, req.Topic, req.Offset); err != nil {
		return nil, status.Errorf(codes.Internal, "commit offset failed: %v", err)
	}
	return &consumer.CommitOffsetResponse{Success: true}, nil
}

func (s *grpcServer) FetchOffset(ctx context.Context, req *consumer.FetchOffsetRequest) (*consumer.FetchOffsetResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	id := req.Id
	if id == "" {
		id = "default"
	}

	if err := s.consumerManager.Recover(); err != nil {
		return nil, status.Errorf(codes.Internal, "recover offsets failed: %v", err)
	}

	off, err := s.consumerManager.GetOffset(id, req.Topic)
	if err != nil {
		// If not found, return 0
		return &consumer.FetchOffsetResponse{Offset: 0}, nil
	}
	return &consumer.FetchOffsetResponse{Offset: off}, nil
}
