package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mohitkumar/mgateway/config"
	"github.com/mohitkumar/mgateway/router"
)

func TestTransformRequest(t *testing.T) {
	mockServer := startMockUpstream(t)
	defer mockServer.Close()
	t.Run("TestHeaderModification", func(t *testing.T) {
		cfg := config.RouteConfig{
			ID:     "get-users",
			Method: "GET",
			Path:   "/users",
			Upstream: config.UpstreamConfig{
				Targets: []string{mockServer.URL},
			},
			Middlewares: []config.MiddlewareConfig{
				{
					Name: "transform",
					Config: map[string]any{
						"request": map[string]any{
							"header": map[string]any{
								"add": []map[string]any{
									{
										"name":  "X-Test-Header",
										"value": "TestValue",
									},
								},
								"remove": []string{"X-Remove-Header"},
							},
							"query_param": map[string]any{
								"add": []map[string]any{
									{
										"name":  "version",
										"value": "1.0.0",
									},
								},
								"remove": []string{"test"},
							},
						},
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

		req := httptest.NewRequest(cfg.Method, cfg.Path, nil)
		req.Header.Add("X-Remove-Header", "ToBeRemoved")

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if status := rr.Code; status != 200 {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, 200)
		}
	})
}

func startMockUpstream(t *testing.T) *httptest.Server {
	t.Helper()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Header") != "TestValue" {
			t.Fatalf("expected request header X-Test-Header=TestValue")
		}

		if r.Header.Get("X-Remove-Header") != "" {
			t.Fatalf("expected request header X-Remove-Header to be removed")
		}
		if r.URL.Query().Get("version") != "1.0.0" {
			t.Fatalf("expected query param version=1.0.0")
		}

		if r.URL.Query().Get("test") != "" {
			t.Fatalf("expected query param test to be removed")
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"user_id": 42,
			"name":    "john",
		})
	})
	server := httptest.NewServer(handler)
	return server
}
