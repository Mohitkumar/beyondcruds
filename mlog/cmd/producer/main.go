package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/mohitkumar/mlog/api/producer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		addr  string
		topic string
		value string
		acks  int32
	)

	rootCmd := &cobra.Command{
		Use:   "producer",
		Short: "Produce messages to a topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := producer.NewProducerServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			resp, err := client.Produce(ctx, &producer.ProduceRequest{
				Topic: topic,
				Value: []byte(value),
				Acks:  producer.AckMode(acks),
			})
			if err != nil {
				return err
			}

			fmt.Printf("offset=%d\n", resp.Offset)
			return nil
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", "127.0.0.1:9092", "gRPC server address")
	rootCmd.Flags().StringVar(&topic, "topic", "", "topic name (required)")
	rootCmd.Flags().StringVar(&value, "value", "", "message value (required)")
	rootCmd.Flags().Int32Var(&acks, "acks", int32(producer.AckMode_ACK_LEADER), "acks: 0=none,1=leader,2=all")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addr", rootCmd.Flags().Lookup("addr"))
	if viper.IsSet("addr") {
		addr = viper.GetString("addr")
	}

	rootCmd.MarkFlagRequired("topic")
	rootCmd.MarkFlagRequired("value")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
