package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gitbub.com/mohitkumar/mgateway/config"
	"gitbub.com/mohitkumar/mgateway/ratelimit"
	"gitbub.com/mohitkumar/mgateway/router"
)

func TestRateLimiter_Allow(t *testing.T) {
	limiter := ratelimit.NewinMemoryRateLimiter(2, 5)

	// Should allow first 5 requests (capacity)
	for i := 0; i < 5; i++ {
		if !limiter.Allow() {
			t.Errorf("request %d should be allowed", i+1)
		}
	}

	// 6th request should be denied
	if limiter.Allow() {
		t.Error("6th request should be denied")
	}
}

func TestRateLimiter_Refill(t *testing.T) {
	limiter := ratelimit.NewinMemoryRateLimiter(2, 3)

	// Consume all tokens
	for i := 0; i < 3; i++ {
		limiter.Allow()
	}

	// Should be denied
	if limiter.Allow() {
		t.Error("should be denied after exhausting tokens")
	}

	// Wait for refill (1 second adds 2 tokens)
	time.Sleep(1100 * time.Millisecond)

	// Should now allow 2 requests
	if !limiter.Allow() {
		t.Error("should allow after refill")
	}
	if !limiter.Allow() {
		t.Error("should allow second request after refill")
	}

	// Third should be denied
	if limiter.Allow() {
		t.Error("should deny after using refilled tokens")
	}
}

func TestRateLimitMiddleware_BlocksExcessRequests(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	cfg := config.RouteConfig{
		ID:     "ratelimit-test",
		Method: "GET",
		Path:   "/users",
		Upstream: config.UpstreamConfig{
			Strategy: "round_robin",
			Targets:  []string{mockServer.URL},
		},
		Middlewares: []config.MiddlewareConfig{
			{
				Name: "rate_limit",
				Config: map[string]any{
					"type":     "in_memory",
					"capacity": 3,
					"rate":     1,
				},
			},
		},
	}

	router, err := router.NewRouter(config.Config{
		Routes: []config.RouteConfig{cfg},
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	// First 3 requests should succeed
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/users", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("request %d: expected status 200, got %v", i+1, status)
		}
	}

	// 4th request should be rate limited
	req := httptest.NewRequest("GET", "/users", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %v", status)
	}
}

func TestRateLimitMiddleware_AllowsAfterRefill(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	cfg := config.RouteConfig{
		ID:     "ratelimit-refill-test",
		Method: "GET",
		Path:   "/users",
		Upstream: config.UpstreamConfig{
			Strategy: "round_robin",
			Targets:  []string{mockServer.URL},
		},
		Middlewares: []config.MiddlewareConfig{
			{
				Name: "rate_limit",
				Config: map[string]any{
					"type":     "in_memory",
					"capacity": 2,
					"rate":     2,
				},
			},
		},
	}

	router, err := router.NewRouter(config.Config{
		Routes: []config.RouteConfig{cfg},
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	// Exhaust the capacity
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/users", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}

	// Should be rate limited
	req := httptest.NewRequest("GET", "/users", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %v", status)
	}

	// Wait for refill
	time.Sleep(1100 * time.Millisecond)

	// Should succeed now
	req = httptest.NewRequest("GET", "/users", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("expected status 200 after refill, got %v", status)
	}
}
