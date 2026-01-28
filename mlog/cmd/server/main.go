package main

import (
	"fmt"
	"net"
	"os"

	"github.com/mohitkumar/mlog/broker"
	"github.com/mohitkumar/mlog/consumer"
	"github.com/mohitkumar/mlog/node"
	"github.com/mohitkumar/mlog/rpc"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	var (
		addr    string
		dataDir string
		nodeID  string
		peers   []string
	)

	rootCmd := &cobra.Command{
		Use:   "server",
		Short: "Run the mlog gRPC server",
		RunE: func(cmd *cobra.Command, args []string) error {
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				return fmt.Errorf("listen %s: %w", addr, err)
			}

			// Create self broker (connection will be established lazily when needed)
			selfBroker := broker.NewBroker(nodeID, addr)

			bm := broker.NewBrokerManager()
			bm.AddBroker(selfBroker)

			// Add peers from config: "nodeID=addr"
			for _, p := range peers {
				var peerNode, peerAddr string
				n, _ := fmt.Sscanf(p, "%[^=]=%s", &peerNode, &peerAddr)
				if n != 2 {
					continue
				}
				b := broker.NewBroker(peerNode, peerAddr)
				bm.AddBroker(b)
			}

			topicMgr, err := node.NewTopicManager(dataDir, bm, selfBroker)
			if err != nil {
				return fmt.Errorf("create topic manager: %w", err)
			}

			consumerMgr, err := consumer.NewConsumerManager(dataDir)
			if err != nil {
				return fmt.Errorf("create consumer manager: %w", err)
			}

			gsrv, err := rpc.NewGrpcServer(topicMgr, consumerMgr)
			if err != nil {
				return fmt.Errorf("create grpc server: %w", err)
			}

			return gsrv.Serve(ln)
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", "127.0.0.1:9092", "gRPC listen address")
	rootCmd.Flags().StringVar(&dataDir, "data-dir", "/tmp/mlog", "data directory")
	rootCmd.Flags().StringVar(&nodeID, "node-id", "node-1", "broker node ID")
	rootCmd.Flags().StringSliceVar(&peers, "peer", nil, "peer brokers (nodeID=addr), repeatable")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addr", rootCmd.Flags().Lookup("addr"))
	viper.BindPFlag("data_dir", rootCmd.Flags().Lookup("data-dir"))
	viper.BindPFlag("node_id", rootCmd.Flags().Lookup("node-id"))
	viper.BindPFlag("peers", rootCmd.Flags().Lookup("peer"))

	if viper.IsSet("addr") {
		addr = viper.GetString("addr")
	}
	if viper.IsSet("data_dir") {
		dataDir = viper.GetString("data_dir")
	}
	if viper.IsSet("node_id") {
		nodeID = viper.GetString("node_id")
	}
	if viper.IsSet("peers") {
		peers = viper.GetStringSlice("peers")
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
