package rpc

import (
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/mohitkumar/mlog/api/leader"
	"github.com/mohitkumar/mlog/api/producer"
	"github.com/mohitkumar/mlog/api/replication"
	"github.com/mohitkumar/mlog/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type grpcServer struct {
	leader.UnimplementedLeaderServiceServer
	replication.UnimplementedReplicationServiceServer
	producer.UnimplementedProducerServiceServer
	topicManager *log.TopicManager
}

func NewGrpcServer(topicManager *log.TopicManager) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}

	grpcOpts := make([]grpc.ServerOption, 0)
	grpcOpts = append(grpcOpts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
			)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
		)),
	)

	gsrv := grpc.NewServer(grpcOpts...)
	srv := &grpcServer{
		topicManager: topicManager,
	}
	leader.RegisterLeaderServiceServer(gsrv, srv)
	replication.RegisterReplicationServiceServer(gsrv, srv)
	producer.RegisterProducerServiceServer(gsrv, srv)
	return gsrv, nil
}
