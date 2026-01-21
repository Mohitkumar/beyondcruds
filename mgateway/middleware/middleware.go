package middleware

import (
	"fmt"
	"net/http"

	"github.com/mohitkumar/mgateway/config"
)

type Middleware func(http.Handler) http.Handler

type MiddlewareFactory interface {
	Name() string
	Create(cfg map[string]any) (Middleware, error)
}

var middlewareRegistry = make(map[string]MiddlewareFactory)

func init() {
	authMiddlewareFacgtory := &AuthMiddlewareFactory{}
	rateLimitMiddlewareFactory := &RatelimitMiddlewareFactory{}
	transformMiddlewareFactory := &TransformMiddlewareFactory{}
	middlewareRegistry[authMiddlewareFacgtory.Name()] = authMiddlewareFacgtory
	middlewareRegistry[rateLimitMiddlewareFactory.Name()] = rateLimitMiddlewareFactory
	middlewareRegistry[transformMiddlewareFactory.Name()] = transformMiddlewareFactory
}

func Build(last http.Handler, config []config.MiddlewareConfig) (http.Handler, error) {
	h := last

	for _, m := range config {
		factory, exists := middlewareRegistry[m.Name]
		if !exists {
			return nil, fmt.Errorf("middleware %s not found", m.Name)
		}
		middleware, err := factory.Create(m.Config)
		if err != nil {
			return nil, err
		}
		h = middleware(h)
	}
	return h, nil
}
