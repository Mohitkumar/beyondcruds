package middleware

import (
	"encoding/json"
	"net/http"

	"gitbub.com/mohitkumar/mgateway/transform"
)

type TransformMiddlewareFactory struct{}

var _ MiddlewareFactory = (*TransformMiddlewareFactory)(nil)

func (m *TransformMiddlewareFactory) Name() string {
	return "transform"
}

func (m *TransformMiddlewareFactory) Create(cfg map[string]any) (Middleware, error) {
	var transform transform.Transform
	raw, _ := json.Marshal(cfg)
	if err := json.Unmarshal(raw, &transform); err != nil {
		return nil, err
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			transform.ApplyRequest(r)
			next.ServeHTTP(w, r)
			transform.ApplyResponse(w)
		})
	}, nil
}
