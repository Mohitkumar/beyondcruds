package tests

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mohitkumar/mgateway/config"
	"github.com/mohitkumar/mgateway/router"
)

func TestRoute(t *testing.T) {
	mockServer1 := runMockUpstreamServer(t)
	mockServer2 := runMockUpstreamServer(t)
	defer mockServer1.Close()
	defer mockServer2.Close()
	t.Run("TestGetUsers", func(t *testing.T) {
		cfg := config.RouteConfig{
			ID:     "get-users",
			Method: "GET",
			Path:   "/users",
			Upstream: config.UpstreamConfig{
				Strategy: "round_robin",
				Targets:  []string{mockServer1.URL, mockServer2.URL},
			},
		}
		router, err := router.NewRouter(config.Config{
			Routes: []config.RouteConfig{cfg},
		})
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}
		req := httptest.NewRequest(cfg.Method, cfg.Path, nil)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusOK)
		}
		expected := `{"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}`
		if rr.Body.String() != expected {
			t.Errorf("handler returned unexpected body: got %v want %v",
				rr.Body.String(), expected)
		}
	})
	t.Run("TestPostUsers", func(t *testing.T) {
		cfg := config.RouteConfig{
			ID:     "post-users",
			Method: "POST",
			Path:   "/users",
			Upstream: config.UpstreamConfig{
				Strategy: "round_robin",
				Targets:  []string{mockServer1.URL, mockServer2.URL},
			},
		}
		router, err := router.NewRouter(config.Config{
			Routes: []config.RouteConfig{cfg},
		})
		if err != nil {
			t.Fatalf("failed to create router: %v", err)
		}
		bodyStr := `{"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}`
		req := httptest.NewRequest(cfg.Method, cfg.Path, strings.NewReader(bodyStr))

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusOK)
		}
		if rr.Body.String() != "Created" {
			t.Errorf("handler returned unexpected body: got %v want %v",
				rr.Body.String(), "Created")
		}
	})
}
