package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mohitkumar/mgateway/config"
	"github.com/mohitkumar/mgateway/ratelimit"
	"github.com/mohitkumar/mgateway/router"
)

func TestRateLimiter_Allow(t *testing.T) {
	limiter := ratelimit.NewInMemoryRateLimiter(2, 5)
	key := "test-client"

	// Should allow first 5 requests (capacity)
	for i := 0; i < 5; i++ {
		if !limiter.Allow(key) {
			t.Errorf("request %d should be allowed", i+1)
		}
	}

	// 6th request should be denied
	if limiter.Allow(key) {
		t.Error("6th request should be denied")
	}
}

func TestRateLimiter_PerKeyIsolation(t *testing.T) {
	limiter := ratelimit.NewInMemoryRateLimiter(2, 3)

	// Exhaust tokens for client1
	for i := 0; i < 3; i++ {
		limiter.Allow("client1")
	}

	// client1 should be denied
	if limiter.Allow("client1") {
		t.Error("client1 should be denied after exhausting tokens")
	}

	// client2 should still be allowed (separate bucket)
	if !limiter.Allow("client2") {
		t.Error("client2 should be allowed (has its own bucket)")
	}
}

func TestRateLimiter_Refill(t *testing.T) {
	limiter := ratelimit.NewInMemoryRateLimiter(2, 3)
	key := "test-client"

	// Consume all tokens
	for i := 0; i < 3; i++ {
		limiter.Allow(key)
	}

	// Should be denied
	if limiter.Allow(key) {
		t.Error("should be denied after exhausting tokens")
	}

	// Wait for refill (1 second adds 2 tokens)
	time.Sleep(1100 * time.Millisecond)

	// Should now allow 2 requests
	if !limiter.Allow(key) {
		t.Error("should allow after refill")
	}
	if !limiter.Allow(key) {
		t.Error("should allow second request after refill")
	}

	// Third should be denied
	if limiter.Allow(key) {
		t.Error("should deny after using refilled tokens")
	}
}

func TestKeyExtractor_IP(t *testing.T) {
	extractor := ratelimit.GetKeyExtractor("ip")

	t.Run("RemoteAddr", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.RemoteAddr = "192.168.1.100:12345"

		key := extractor(req)
		if key != "192.168.1.100" {
			t.Errorf("expected 192.168.1.100, got %s", key)
		}
	})

	t.Run("X-Forwarded-For", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Forwarded-For", "10.0.0.1, 10.0.0.2")

		key := extractor(req)
		if key != "10.0.0.1" {
			t.Errorf("expected 10.0.0.1, got %s", key)
		}
	})

	t.Run("X-Real-IP", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Real-IP", "172.16.0.1")

		key := extractor(req)
		if key != "172.16.0.1" {
			t.Errorf("expected 172.16.0.1, got %s", key)
		}
	})
}

func TestKeyExtractor_UserID(t *testing.T) {
	extractor := ratelimit.GetKeyExtractor("user_id")

	t.Run("With X-User-ID header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-User-ID", "user-123")

		key := extractor(req)
		if key != "user-123" {
			t.Errorf("expected user-123, got %s", key)
		}
	})

	t.Run("Falls back to IP without header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.RemoteAddr = "192.168.1.50:8080"

		key := extractor(req)
		if key != "192.168.1.50" {
			t.Errorf("expected 192.168.1.50, got %s", key)
		}
	})
}

func TestKeyExtractor_Global(t *testing.T) {
	extractor := ratelimit.GetKeyExtractor("global")

	req := httptest.NewRequest("GET", "/test", nil)
	key := extractor(req)

	if key != "global" {
		t.Errorf("expected 'global', got %s", key)
	}
}

func TestRateLimitMiddleware_BlocksExcessRequests(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	cfg := config.Config{
		Routes: []config.RouteConfig{
			{
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
							"key":      "ip",
							"capacity": 3,
							"rate":     1,
						},
					},
				},
			},
		},
	}

	r, err := router.NewRouter(cfg)
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	// First 3 requests should succeed
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/users", nil)
		req.RemoteAddr = "192.168.1.1:1234"
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("request %d: expected 200, got %v", i+1, status)
		}
	}

	// 4th request should be rate limited
	req := httptest.NewRequest("GET", "/users", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %v", status)
	}
}

func TestRateLimitMiddleware_PerIPIsolation(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	cfg := config.Config{
		Routes: []config.RouteConfig{
			{
				ID:     "ratelimit-ip-test",
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
							"key":      "ip",
							"capacity": 2,
							"rate":     1,
						},
					},
				},
			},
		},
	}

	r, err := router.NewRouter(cfg)
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	// Exhaust rate limit for IP1
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/users", nil)
		req.RemoteAddr = "192.168.1.1:1234"
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
	}

	// IP1 should be rate limited
	req := httptest.NewRequest("GET", "/users", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusTooManyRequests {
		t.Errorf("IP1 should be rate limited, got %v", status)
	}

	// IP2 should still work (separate bucket)
	req = httptest.NewRequest("GET", "/users", nil)
	req.RemoteAddr = "192.168.1.2:1234"
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("IP2 should succeed, got %v", status)
	}
}

func TestRateLimitMiddleware_AllowsAfterRefill(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	cfg := config.Config{
		Routes: []config.RouteConfig{
			{
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
							"key":      "ip",
							"capacity": 2,
							"rate":     2,
						},
					},
				},
			},
		},
	}

	r, err := router.NewRouter(cfg)
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	// Exhaust the capacity
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/users", nil)
		req.RemoteAddr = "192.168.1.1:1234"
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
	}

	// Should be rate limited
	req := httptest.NewRequest("GET", "/users", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %v", status)
	}

	// Wait for refill
	time.Sleep(1100 * time.Millisecond)

	// Should succeed now
	req = httptest.NewRequest("GET", "/users", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("expected 200 after refill, got %v", status)
	}
}
