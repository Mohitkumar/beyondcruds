package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string

	rootCmd = &cobra.Command{
		Use:   "mlog",
		Short: "mlog - a minimal Kafka-like log broker",
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (optional)")
	rootCmd.PersistentFlags().String("data-dir", "/tmp/mlog", "data directory")
	rootCmd.PersistentFlags().String("addr", "127.0.0.1:9092", "gRPC listen address")
	rootCmd.PersistentFlags().String("node-id", "node-1", "broker node ID")
	rootCmd.PersistentFlags().StringSlice("peer", nil, "peer brokers (nodeID=addr), repeatable")

	_ = viper.BindPFlag("data_dir", rootCmd.PersistentFlags().Lookup("data-dir"))
	_ = viper.BindPFlag("addr", rootCmd.PersistentFlags().Lookup("addr"))
	_ = viper.BindPFlag("node_id", rootCmd.PersistentFlags().Lookup("node-id"))
	_ = viper.BindPFlag("peers", rootCmd.PersistentFlags().Lookup("peer"))

	viper.SetEnvPrefix("mlog")
	viper.AutomaticEnv()
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("mlog")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.config/mlog")
	}

	if err := viper.ReadInConfig(); err != nil {
		// Ignore missing config file; error out on other issues
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			fmt.Fprintln(os.Stderr, "config error:", err)
			os.Exit(1)
		}
	}
}
