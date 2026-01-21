package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gitbub.com/mohitkumar/mgateway/config"
	"gitbub.com/mohitkumar/mgateway/router"
	"github.com/golang-jwt/jwt/v5"
)

func generateTestToken(secret string, expiry time.Duration) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":  "user123",
		"name": "Test User",
		"exp":  time.Now().Add(expiry).Unix(),
	})
	tokenString, _ := token.SignedString([]byte(secret))
	return tokenString
}

func TestAuthMiddleware_ValidToken(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	secret := "test-secret-key"
	cfg := config.RouteConfig{
		ID:     "auth-test",
		Method: "GET",
		Path:   "/users",
		Upstream: config.UpstreamConfig{
			Strategy: "round_robin",
			Targets:  []string{mockServer.URL},
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
	}

	router, err := router.NewRouter(config.Config{
		Routes: []config.RouteConfig{cfg},
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	token := generateTestToken(secret, time.Hour)
	req := httptest.NewRequest("GET", "/users", nil)
	req.Header.Set("Authorization", token)

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("expected status 200, got %v", status)
	}
}

func TestAuthMiddleware_MissingToken(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	cfg := config.RouteConfig{
		ID:     "auth-test-missing",
		Method: "GET",
		Path:   "/users",
		Upstream: config.UpstreamConfig{
			Strategy: "round_robin",
			Targets:  []string{mockServer.URL},
		},
		Middlewares: []config.MiddlewareConfig{
			{
				Name: "auth",
				Config: map[string]any{
					"type":   "jwt",
					"secret": "test-secret",
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

	req := httptest.NewRequest("GET", "/users", nil)
	// No Authorization header

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %v", status)
	}
}

func TestAuthMiddleware_InvalidToken(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	cfg := config.RouteConfig{
		ID:     "auth-test-invalid",
		Method: "GET",
		Path:   "/users",
		Upstream: config.UpstreamConfig{
			Strategy: "round_robin",
			Targets:  []string{mockServer.URL},
		},
		Middlewares: []config.MiddlewareConfig{
			{
				Name: "auth",
				Config: map[string]any{
					"type":   "jwt",
					"secret": "correct-secret",
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

	// Generate token with wrong secret
	wrongToken := generateTestToken("wrong-secret", time.Hour)
	req := httptest.NewRequest("GET", "/users", nil)
	req.Header.Set("Authorization", wrongToken)

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %v", status)
	}
}

func TestAuthMiddleware_MalformedToken(t *testing.T) {
	mockServer := runMockUpstreamServer(t)
	defer mockServer.Close()

	cfg := config.RouteConfig{
		ID:     "auth-test-malformed",
		Method: "GET",
		Path:   "/users",
		Upstream: config.UpstreamConfig{
			Strategy: "round_robin",
			Targets:  []string{mockServer.URL},
		},
		Middlewares: []config.MiddlewareConfig{
			{
				Name: "auth",
				Config: map[string]any{
					"type":   "jwt",
					"secret": "test-secret",
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

	req := httptest.NewRequest("GET", "/users", nil)
	req.Header.Set("Authorization", "not-a-valid-jwt-token")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %v", status)
	}
}
