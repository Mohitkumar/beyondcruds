package middleware

import (
	"net/http"

	"gitbub.com/mohitkumar/mgateway/ratelimit"
)

var _ MiddlewareFactory = (*RatelimitMiddlewareFactory)(nil)

type RatelimitMiddlewareFactory struct{}

func (m *RatelimitMiddlewareFactory) Name() string {
	return "rate_limit"
}

func (m *RatelimitMiddlewareFactory) Create(cfg map[string]any) (Middleware, error) {
	var limiter ratelimit.RateLimiter
	limiterType := "in_memory"
	typeStr, ok := cfg["type"]
	if ok {
		limiterType = typeStr.(string)
	}
	switch limiterType {
	case "in_memory":
		limiter = ratelimit.NewinMemoryRateLimiterDefault()
		capacityStr, capOk := cfg["capacity"]
		rateStr, rateOk := cfg["rate"]
		if capOk && rateOk {
			capacity := capacityStr.(int)
			rate := rateStr.(int)
			limiter = ratelimit.NewinMemoryRateLimiter(rate, capacity)
		}
	default:
		limiter = ratelimit.NewinMemoryRateLimiterDefault()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.Allow() {
				http.Error(w, "rate limited", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}
