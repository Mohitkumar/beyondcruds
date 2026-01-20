package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"gitbub.com/mohitkumar/mgateway/config"
	"gitbub.com/mohitkumar/mgateway/router"
)

func TestLoadBalancer(t *testing.T) {
	mockServer1 := runMockUpstreamServerLB(t, "lb1")
	mockServer2 := runMockUpstreamServerLB(t, "lb2")
	defer mockServer1.Close()
	defer mockServer2.Close()
	t.Run("TestLoadBalancer", func(t *testing.T) {
		cfg := config.RouteConfig{
			ID:     "lb-test",
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

		for i := 0; i < 10; i++ {
			req := httptest.NewRequest(cfg.Method, cfg.Path, nil)

			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)
			if status := rr.Code; status != http.StatusOK {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, http.StatusOK)
			}
			expected := "lb1"
			if i%2 == 1 {
				expected = "lb2"
			}
			t.Logf("Expected: %s, Got: %s", expected, rr.Body.String())
			if rr.Body.String() != expected {
				t.Errorf("handler returned unexpected body: got %v want %v",
					rr.Body.String(), expected)
			}
		}
	})
}
