package middleware

import (
	"net/http"
	"time"

	"github.com/mohitkumar/mgateway/observe"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func MetricsMiddleware(route string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: 200}

			next.ServeHTTP(rec, r)

			duration := float64(time.Since(start).Milliseconds())

			attrs := []attribute.KeyValue{
				attribute.String("method", r.Method),
				attribute.String("route", route),
				attribute.Int("status", rec.status),
			}

			observe.RequestsTotal.Add(r.Context(), 1, metric.WithAttributes(attrs...))
			observe.RequestDuration.Record(r.Context(), duration, metric.WithAttributes(attrs...))

			if rec.status >= 400 {
				observe.RequestsFailed.Add(
					r.Context(),
					1,
					metric.WithAttributes(
						append(attrs,
							attribute.String("reason", statusClass(rec.status)),
						)...,
					),
				)
			}
		})
	}
}

func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	default:
		return "ok"
	}
}
