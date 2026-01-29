package rpc

import (
	"context"
	"time"

	"github.com/mohitkumar/mlog/api/common"
	"github.com/mohitkumar/mlog/api/leader"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ leader.LeaderServiceServer = (*grpcServer)(nil)

// CreateTopic creates a new topic with the specified replica count
func (s *grpcServer) CreateTopic(ctx context.Context, req *leader.CreateTopicRequest) (*leader.CreateTopicResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name is required")
	}
	if req.ReplicaCount < 1 {
		return nil, status.Error(codes.InvalidArgument, "replica count must be at least 1")
	}

	err := s.topicManager.CreateTopic(req.Topic, int(req.ReplicaCount))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
	}

	return &leader.CreateTopicResponse{
		Topic: req.Topic,
	}, nil
}

// DeleteTopic deletes a topic
func (s *grpcServer) DeleteTopic(ctx context.Context, req *leader.DeleteTopicRequest) (*leader.DeleteTopicResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name is required")
	}

	err := s.topicManager.DeleteTopic(req.Topic)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete topic: %v", err)
	}

	return &leader.DeleteTopicResponse{
		Topic: req.Topic,
	}, nil
}

// RecordLEO records the Log End Offset (LEO) of a replica
func (s *grpcServer) RecordLEO(ctx context.Context, req *leader.RecordLEORequest) (*leader.RecordLEOResponse, error) {
	if req.ReplicaId == "" {
		return nil, status.Error(codes.InvalidArgument, "replica_id is required")
	}

	topicObj, err := s.topicManager.GetTopic(req.Topic)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "topic %s not found: %v", req.Topic, err)
	}

	err = topicObj.RecordLEORemote(req.ReplicaId, uint64(req.Leo), time.Now())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to record LEO for replica %s in topic %s: %v", req.ReplicaId, req.Topic, err)
	}

	return &leader.RecordLEOResponse{}, nil
}

// ReplicateStream streams log entries to replicas for replication
func (s *grpcServer) ReplicateStream(req *leader.ReplicateRequest, stream leader.LeaderService_ReplicateStreamServer) error {
	if req.Topic == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}

	// Get the leader for this topic
	leaderNode, err := s.topicManager.GetLeader(req.Topic)
	if err != nil {
		return status.Errorf(codes.NotFound, "topic not found: %v", err)
	}

	batchSize := req.BatchSize
	if batchSize == 0 {
		batchSize = 1000
	}

	// Start reading from the requested offset
	currentOffset := req.Offset
	// Use a short idle tick; when there's backlog we stream batches back-to-back.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	ctx := stream.Context()

	for {
		// LEO is the next offset to write; readable offsets are [0, LEO).
		endExclusive := leaderNode.Log.LEO()
		if currentOffset < endExclusive {
			entries := make([]*common.LogEntry, 0, batchSize)
			for i := 0; i < int(batchSize); i++ {
				// Refresh endExclusive so large backlogs drain efficiently.
				endExclusive = leaderNode.Log.LEO()
				if currentOffset >= endExclusive {
					break
				}
				entry, err := leaderNode.Log.ReadUncommitted(currentOffset)
				if err != nil {
					// If we can't read this offset yet (might be partially written or not indexed),
					// stop this batch and retry shortly.
					break
				}
				entries = append(entries, entry)
				currentOffset++
			}
			if len(entries) > 0 {
				resp := &leader.ReplicateResponse{
					LastOffset: currentOffset - 1,
					Entries:    entries,
				}
				if err := stream.Send(resp); err != nil {
					return status.Errorf(codes.Internal, "failed to send entries: %v", err)
				}
			}
			// Continue immediately to drain backlog faster.
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			continue
		}
	}
}
