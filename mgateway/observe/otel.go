package observe

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
)

func InitOpenTelemetry(ctx context.Context) (func(context.Context) error, error) {
	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	reader := metric.NewPeriodicReader(exporter, metric.WithInterval(10*time.Second))
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(provider)
	return provider.Shutdown, nil
}
