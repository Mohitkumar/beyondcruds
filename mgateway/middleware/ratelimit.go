package middleware

import (
	"net/http"

	"github.com/mohitkumar/mgateway/ratelimit"
)

var _ MiddlewareFactory = (*RatelimitMiddlewareFactory)(nil)

type RatelimitMiddlewareFactory struct{}

func (m *RatelimitMiddlewareFactory) Name() string {
	return "rate_limit"
}

func (m *RatelimitMiddlewareFactory) Create(cfg map[string]any) (Middleware, error) {
	// Get rate and capacity with defaults
	rate := 2
	capacity := 10

	if r, ok := cfg["rate"]; ok {
		rate = toInt(r)
	}
	if c, ok := cfg["capacity"]; ok {
		capacity = toInt(c)
	}

	// Create the rate limiter
	limiter := ratelimit.NewInMemoryRateLimiter(rate, capacity)

	// Get key extraction strategy (default: ip)
	keyType := "ip"
	if k, ok := cfg["key"].(string); ok {
		keyType = k
	}
	keyExtractor := ratelimit.GetKeyExtractor(keyType)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract key based on configured strategy
			key := keyExtractor(r)

			if !limiter.Allow(key) {
				http.Error(w, "rate limited", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

// toInt converts interface{} to int, handling both int and float64
func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	default:
		return 0
	}
}
