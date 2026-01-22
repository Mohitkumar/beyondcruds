package observe

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	RequestsTotal   metric.Int64Counter
	RequestsFailed  metric.Int64Counter
	RequestDuration metric.Float64Histogram
)

func InitMetrics() error {
	var err error
	meter := otel.Meter("mgateway")
	RequestsTotal, err = meter.Int64Counter("requests_total")
	if err != nil {
		return err
	}
	RequestsFailed, err = meter.Int64Counter("requests_failed")
	if err != nil {
		return err
	}
	RequestDuration, err = meter.Float64Histogram("request_duration_ms", metric.WithUnit("ms"))
	if err != nil {
		return err
	}
	return nil
}
