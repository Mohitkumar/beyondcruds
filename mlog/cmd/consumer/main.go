package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mohitkumar/mlog/api/consumer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		addr          string
		id            string
		topic         string
		offset        uint64
		fromBeginning bool
	)

	rootCmd := &cobra.Command{
		Use:   "consumer",
		Short: "Consume messages from a topic (streaming)",
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			client := consumer.NewConsumerServiceClient(conn)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Determine starting offset
			offsetExplicitlySet := cmd.Flags().Changed("offset")
			startOffset := offset

			if fromBeginning {
				// --from-beginning flag takes precedence
				fmt.Fprintf(os.Stderr, "Starting from beginning (offset 0)\n")
				startOffset = 0
			} else if offsetExplicitlySet {
				// User explicitly set an offset, use it
				fmt.Fprintf(os.Stderr, "Starting from offset %d (explicitly specified)\n", startOffset)
			} else {
				// Default behavior: fetch the last committed offset to resume from where we left off
				fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
				resp, err := client.FetchOffset(fetchCtx, &consumer.FetchOffsetRequest{
					Id:    id,
					Topic: topic,
				})
				fetchCancel()
				if err == nil && resp != nil && resp.Offset > 0 {
					startOffset = resp.Offset
					fmt.Fprintf(os.Stderr, "Resuming from offset %d (last committed)\n", startOffset)
				} else {
					fmt.Fprintf(os.Stderr, "No previous offset found, starting from beginning\n")
					startOffset = 0
				}
			}

			stream, err := client.FetchStream(ctx, &consumer.FetchRequest{
				Id:     id,
				Topic:  topic,
				Offset: startOffset,
			})
			if err != nil {
				return err
			}

			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if resp.Entry == nil {
					continue
				}

				fmt.Printf("%d\t%s\n", resp.Entry.Offset, string(resp.Entry.Value))

				// Commit the next offset
				commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, _ = client.CommitOffset(commitCtx, &consumer.CommitOffsetRequest{
					Id:     id,
					Topic:  topic,
					Offset: resp.Entry.Offset + 1,
				})
				cancel()
			}
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", "127.0.0.1:9092", "gRPC server address")
	rootCmd.Flags().StringVar(&id, "id", "default", "consumer id")
	rootCmd.Flags().StringVar(&topic, "topic", "", "topic name (required)")
	rootCmd.Flags().Uint64Var(&offset, "offset", 0, "start from specific offset (default: resume from last committed)")
	rootCmd.Flags().BoolVar(&fromBeginning, "from-beginning", false, "start from offset 0 instead of last committed offset")

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
	viper.BindPFlag("addr", rootCmd.Flags().Lookup("addr"))
	if viper.IsSet("addr") {
		addr = viper.GetString("addr")
	}

	rootCmd.MarkFlagRequired("topic")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
