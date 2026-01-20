package router

import (
	"fmt"
	"net/http"

	"gitbub.com/mohitkumar/mgateway/config"
	"gitbub.com/mohitkumar/mgateway/lb"
	"gitbub.com/mohitkumar/mgateway/middleware"
)

var _ http.Handler = (*Router)(nil)

type Route struct {
	Method  string
	Path    string
	Targets []string
	handler http.Handler
}

type Router struct {
	routes map[string]*Route
}

func NewRouter(cfg config.Config) (*Router, error) {
	router := &Router{
		routes: make(map[string]*Route),
	}
	for _, routeConfig := range cfg.Routes {
		route := Route{
			Method:  routeConfig.Method,
			Path:    routeConfig.Path,
			Targets: routeConfig.Upstream.Targets,
		}
		handler, err := buildHandler(routeConfig.Upstream, routeConfig.Middlewares)
		if err != nil {
			return nil, err
		}
		route.handler = handler
		err = router.registerRoute(route)
		if err != nil {
			return nil, err
		}
	}
	return router, nil
}

func buildHandler(upstream config.UpstreamConfig, middlewares []config.MiddlewareConfig) (http.Handler, error) {
	loadBalancer := lb.NewLoadBalancer(upstream)
	handler, err := middleware.Build(loadBalancer, middlewares)
	if err != nil {
		return nil, err
	}
	return handler, nil
}

func (router *Router) registerRoute(r Route) error {
	key := r.Method + ":" + r.Path
	_, ok := router.routes[key]
	if ok {
		return fmt.Errorf("path already registered")
	}
	router.routes[key] = &r
	return nil
}

func (router *Router) getRoute(method string, path string) *Route {
	key := method + ":" + path
	return router.routes[key]
}

func (router *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	fmt.Printf("handling %s, %s \n", req.Method, req.URL.Path)
	route := router.getRoute(req.Method, req.URL.Path)
	if route != nil {
		route.handler.ServeHTTP(w, req)
		return
	}
	http.NotFound(w, req)
}
