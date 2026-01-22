package main

import (
	"context"

	"github.com/mohitkumar/mgateway/cmd"
	"github.com/mohitkumar/mgateway/observe"
)

func main() {
	ctx := context.Background()

	shutdown, err := observe.InitOpenTelemetry(ctx)
	if err != nil {
		panic(err)
	}
	defer shutdown(ctx)

	if err := observe.InitMetrics(); err != nil {
		panic(err)
	}
	cmd.Execute()
}
