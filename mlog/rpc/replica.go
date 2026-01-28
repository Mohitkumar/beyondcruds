package rpc

import (
	"context"

	"github.com/mohitkumar/mlog/api/replication"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ replication.ReplicationServiceServer = (*grpcServer)(nil)

// CreateReplica creates a new replica for a topic on this broker
func (s *grpcServer) CreateReplica(ctx context.Context, req *replication.CreateReplicaRequest) (*replication.CreateReplicaResponse, error) {
	if req.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}
	if req.ReplicaId == "" {
		return nil, status.Error(codes.InvalidArgument, "replica_id is required")
	}
	if req.LeaderAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "leader_addr is required")
	}

	err := s.topicManager.CreateReplicaRemote(req.Topic, req.ReplicaId, req.LeaderAddr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create replica: %v", err)
	}

	return &replication.CreateReplicaResponse{
		ReplicaId: req.ReplicaId,
	}, nil
}

// DeleteReplica deletes a replica for a topic
func (s *grpcServer) DeleteReplica(ctx context.Context, req *replication.DeleteReplicaRequest) (*replication.DeleteReplicaResponse, error) {
	if req.ReplicaId == "" {
		return nil, status.Error(codes.InvalidArgument, "replica_id is required")
	}
	err := s.topicManager.DeleteReplicaRemote(req.Topic, req.ReplicaId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete replica: %v", err)
	}

	return &replication.DeleteReplicaResponse{}, nil
}
