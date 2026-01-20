package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func runMockUpstreamServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		path := r.URL.Path
		method := r.Method
		if path == "/users" && method == "GET" {
			w.Write([]byte(`{"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}`))
		} else if path == "/users" && method == "POST" {
			w.Write([]byte("Created"))
		}
	}))
}

func runMockUpstreamServerLB(t *testing.T, response string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(response))
	}))
}
