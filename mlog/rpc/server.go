package rpc

import (
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	consumerapi "github.com/mohitkumar/mlog/api/consumer"
	"github.com/mohitkumar/mlog/api/leader"
	"github.com/mohitkumar/mlog/api/producer"
	"github.com/mohitkumar/mlog/api/replication"
	consumermgr "github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type grpcServer struct {
	consumerapi.UnimplementedConsumerServiceServer
	leader.UnimplementedLeaderServiceServer
	replication.UnimplementedReplicationServiceServer
	producer.UnimplementedProducerServiceServer
	topicManager    *node.TopicManager
	consumerManager *consumermgr.ConsumerManager
}

func NewGrpcServer(topicManager *node.TopicManager, consumerManager *consumermgr.ConsumerManager) (*grpc.Server, error) {
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
		topicManager:    topicManager,
		consumerManager: consumerManager,
	}
	consumerapi.RegisterConsumerServiceServer(gsrv, srv)
	leader.RegisterLeaderServiceServer(gsrv, srv)
	replication.RegisterReplicationServiceServer(gsrv, srv)
	producer.RegisterProducerServiceServer(gsrv, srv)
	return gsrv, nil
}
