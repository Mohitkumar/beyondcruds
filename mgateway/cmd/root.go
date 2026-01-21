package cmd

import (
	"fmt"
	"net"
	"net/http"
	"os"

	"gitbub.com/mohitkumar/mgateway/config"
	"gitbub.com/mohitkumar/mgateway/router"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.yaml.in/yaml/v3"
)

var gatewayConfig GatewayConfig

func setupConfig(cmd *cobra.Command, args []string) error {
	gatewayConfig.Port = viper.GetInt("port")
	gatewayConfig.ConfigFilePath = viper.GetString("config-path")
	return nil
}

func setupFlags(cmd *cobra.Command) error {
	cmd.Flags().Int("port", 8081, "gateway port")
	cmd.Flags().String("config-path", "./config.yaml", "route config file path")
	return viper.BindPFlags(cmd.Flags())
}

func run(cmd *cobra.Command, args []string) error {
	content, err := os.ReadFile(gatewayConfig.ConfigFilePath)
	if err != nil {
		return fmt.Errorf("file %s: %w", gatewayConfig.ConfigFilePath, err)
	}
	var cfg config.Config
	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return err
	}
	router, err := router.NewRouter(cfg)
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", gatewayConfig.Port))
	if err != nil {
		return err
	}
	fmt.Printf("serving api gateway on :%d\n", gatewayConfig.Port)
	if err = http.Serve(l, router); err != nil {
		return err
	}
	return nil
}

var rootCmd = &cobra.Command{
	Use:     "ApiGateway",
	RunE:    run,
	PreRunE: setupConfig,
}

func init() {
	if err := setupFlags(rootCmd); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
