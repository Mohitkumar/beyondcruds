package rpc

import (
	"context"
	"fmt"
	"time"

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

	// Start reading from the requested offset
	currentOffset := req.Offset
	ticker := time.NewTicker(100 * time.Millisecond) // Poll for new entries
	defer ticker.Stop()

	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Try to read the next entry (use ReadUncommitted for replication)
			entry, err := leaderNode.Log.ReadUncommitted(currentOffset)
			if err != nil {
				// If offset is out of range, wait for new entries
				if err.Error() == fmt.Sprintf("offset %d out of range", currentOffset) {
					// Check if we've reached the end
					highestOffset := leaderNode.Log.HighestOffset()
					if currentOffset >= highestOffset {
						// No new entries, continue waiting
						continue
					}
					// Otherwise, there might be a gap, skip to next available
					currentOffset++
					continue
				}
				// Other errors, log and continue
				continue
			}

			// Send the entry
			resp := &leader.ReplicateResponse{
				LastOffset: entry.Offset,
				Entry:      entry,
			}

			if err := stream.Send(resp); err != nil {
				return status.Errorf(codes.Internal, "failed to send entry: %v", err)
			}

			currentOffset = entry.Offset + 1
		}
	}
}
