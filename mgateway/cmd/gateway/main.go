package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"gitbub.com/mohitkumar/mgateway/config"
	"gitbub.com/mohitkumar/mgateway/router"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.yaml.in/yaml/v3"
)

var gatewayConfig GatewayConfig

func setupConfig(cmd *cobra.Command, args []string) error {
	gatewayConfig.Host = viper.GetString("host")
	gatewayConfig.Port = viper.GetInt("port")
	gatewayConfig.ConfigFilePath = viper.GetString("config-path")
	return nil
}

func setupFlags(cmd *cobra.Command) error {
	cmd.Flags().String("host", "0.0.0.0", "http host name")
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
	if err = http.ListenAndServe(gatewayConfig.Host+":"+strconv.Itoa(gatewayConfig.Port), router); err != nil {
		return err
	}
	return nil
}

func main() {
	cmd := &cobra.Command{
		Use:     "ApiGateway",
		RunE:    run,
		PreRunE: setupConfig,
	}
	if err := setupFlags(cmd); err != nil {
		log.Fatal(err)
	}

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
