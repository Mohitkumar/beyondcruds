package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gitbub.com/mohitkumar/mgateway/config"
	routerpkg "gitbub.com/mohitkumar/mgateway/router"
	"github.com/golang-jwt/jwt/v5"
)

func generateToken(secret string, expiry time.Duration) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":  "user123",
		"name": "Test User",
		"exp":  time.Now().Add(expiry).Unix(),
	})
	tokenString, _ := token.SignedString([]byte(secret))
	return tokenString
}

// TestIntegration_FullPipeline tests the complete request flow through
// auth -> rate limit -> transform -> load balancer -> proxy
func TestIntegration_FullPipeline(t *testing.T) {
	// Create two mock backend servers
	mockServer1 := runMockUpstreamServerLB(t, `{"server": "backend-1"}`)
	mockServer2 := runMockUpstreamServerLB(t, `{"server": "backend-2"}`)
	defer mockServer1.Close()
	defer mockServer2.Close()

	secret := "integration-test-secret"

	cfg := config.Config{
		Routes: []config.RouteConfig{
			{
				ID:     "integration-test",
				Method: "GET",
				Path:   "/api/data",
				Upstream: config.UpstreamConfig{
					Strategy: "round_robin",
					Targets:  []string{mockServer1.URL, mockServer2.URL},
				},
				Middlewares: []config.MiddlewareConfig{
					{
						Name: "auth",
						Config: map[string]any{
							"type":   "jwt",
							"secret": secret,
						},
					},
					{
						Name: "rate_limit",
						Config: map[string]any{
							"type":     "in_memory",
							"capacity": 5,
							"rate":     2,
						},
					},
					{
						Name: "transform",
						Config: map[string]any{
							"request": map[string]any{
								"header": map[string]any{
									"add": []map[string]string{
										{"name": "X-Gateway-Request", "value": "true"},
									},
								},
							},
							"response": map[string]any{
								"header": map[string]any{
									"add": []map[string]string{
										{"name": "X-Gateway-Response", "value": "processed"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	r, err := routerpkg.NewRouter(cfg)
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	token := generateToken(secret, time.Hour)

	t.Run("Authenticated requests with load balancing", func(t *testing.T) {
		// Make requests and verify load balancing
		responses := make([]string, 4)
		for i := 0; i < 4; i++ {
			req := httptest.NewRequest("GET", "/api/data", nil)
			req.Header.Set("Authorization", token)

			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if status := rr.Code; status != http.StatusOK {
				t.Errorf("request %d: expected status 200, got %v", i+1, status)
			}
			responses[i] = rr.Body.String()
		}

		// Verify round-robin: alternating between backend-1 and backend-2
		if responses[0] != `{"server": "backend-1"}` {
			t.Errorf("expected backend-1 first, got %s", responses[0])
		}
		if responses[1] != `{"server": "backend-2"}` {
			t.Errorf("expected backend-2 second, got %s", responses[1])
		}
		if responses[2] != `{"server": "backend-1"}` {
			t.Errorf("expected backend-1 third, got %s", responses[2])
		}
		if responses[3] != `{"server": "backend-2"}` {
			t.Errorf("expected backend-2 fourth, got %s", responses[3])
		}
	})

	t.Run("Unauthenticated request rejected", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/data", nil)
		// No Authorization header

		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %v", status)
		}
	})

	t.Run("Rate limiting kicks in after capacity exceeded", func(t *testing.T) {
		// Create a fresh router to reset rate limiter
		router2, _ := routerpkg.NewRouter(cfg)

		// Exhaust the rate limit capacity (5 requests)
		for i := 0; i < 5; i++ {
			req := httptest.NewRequest("GET", "/api/data", nil)
			req.Header.Set("Authorization", token)

			rr := httptest.NewRecorder()
			router2.ServeHTTP(rr, req)

			if status := rr.Code; status != http.StatusOK {
				t.Errorf("request %d: expected status 200, got %v", i+1, status)
			}
		}

		// 6th request should be rate limited
		req := httptest.NewRequest("GET", "/api/data", nil)
		req.Header.Set("Authorization", token)

		rr := httptest.NewRecorder()
		router2.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusTooManyRequests {
			t.Errorf("expected status 429, got %v", status)
		}
	})
}

// TestIntegration_MultipleRoutes tests multiple routes with different configurations
func TestIntegration_MultipleRoutes(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	secret := "multi-route-secret"

	cfg := config.Config{
		Routes: []config.RouteConfig{
			{
				ID:     "protected-route",
				Method: "GET",
				Path:   "/protected",
				Upstream: config.UpstreamConfig{
					Strategy: "round_robin",
					Targets:  []string{mockServer.URL + "/users"},
				},
				Middlewares: []config.MiddlewareConfig{
					{
						Name: "auth",
						Config: map[string]any{
							"type":   "jwt",
							"secret": secret,
						},
					},
				},
			},
			{
				ID:     "public-route",
				Method: "GET",
				Path:   "/public",
				Upstream: config.UpstreamConfig{
					Strategy: "round_robin",
					Targets:  []string{mockServer.URL + "/users"},
				},
				Middlewares: []config.MiddlewareConfig{},
			},
		},
	}

	r, err := routerpkg.NewRouter(cfg)
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	t.Run("Protected route requires auth", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/protected", nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %v", status)
		}
	})

	t.Run("Protected route with valid token succeeds", func(t *testing.T) {
		token := generateToken(secret, time.Hour)
		req := httptest.NewRequest("GET", "/protected", nil)
		req.Header.Set("Authorization", token)

		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("expected status 200, got %v", status)
		}
	})

	t.Run("Public route works without auth", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/public", nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("expected status 200, got %v", status)
		}
	})

	t.Run("Non-existent route returns 404", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/unknown", nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusNotFound {
			t.Errorf("expected status 404, got %v", status)
		}
	})

	t.Run("Wrong method returns 404", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/public", nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusNotFound {
			t.Errorf("expected status 404, got %v", status)
		}
	})
}

// TestIntegration_CombinedMiddleware verifies multiple middleware work together correctly
func TestIntegration_CombinedMiddleware(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	secret := "combined-test-secret"

	t.Run("Auth checked - unauthenticated rejected", func(t *testing.T) {
		cfg := config.Config{
			Routes: []config.RouteConfig{
				{
					ID:     "combined-test-auth",
					Method: "GET",
					Path:   "/combined",
					Upstream: config.UpstreamConfig{
						Strategy: "round_robin",
						Targets:  []string{mockServer.URL + "/users"},
					},
					Middlewares: []config.MiddlewareConfig{
						{
							Name: "auth",
							Config: map[string]any{
								"type":   "jwt",
								"secret": secret,
							},
						},
					},
				},
			},
		}

		r, err := routerpkg.NewRouter(cfg)
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}

		req := httptest.NewRequest("GET", "/combined", nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusUnauthorized {
			t.Errorf("expected 401, got %v", status)
		}
	})

	t.Run("Authenticated requests succeed until rate limited", func(t *testing.T) {
		cfg := config.Config{
			Routes: []config.RouteConfig{
				{
					ID:     "combined-test-ratelimit",
					Method: "GET",
					Path:   "/combined",
					Upstream: config.UpstreamConfig{
						Strategy: "round_robin",
						Targets:  []string{mockServer.URL + "/users"},
					},
					Middlewares: []config.MiddlewareConfig{
						{
							Name: "auth",
							Config: map[string]any{
								"type":   "jwt",
								"secret": secret,
							},
						},
						{
							Name: "rate_limit",
							Config: map[string]any{
								"type":     "in_memory",
								"capacity": 3,
								"rate":     1,
							},
						},
					},
				},
			},
		}

		r, err := routerpkg.NewRouter(cfg)
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}

		token := generateToken(secret, time.Hour)

		// Make requests up to rate limit
		for i := 0; i < 3; i++ {
			req := httptest.NewRequest("GET", "/combined", nil)
			req.Header.Set("Authorization", token)

			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if status := rr.Code; status != http.StatusOK {
				t.Errorf("request %d: expected 200, got %v", i+1, status)
			}
		}

		// Next request should be rate limited
		req := httptest.NewRequest("GET", "/combined", nil)
		req.Header.Set("Authorization", token)

		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusTooManyRequests {
			t.Errorf("expected 429, got %v", status)
		}
	})
}
