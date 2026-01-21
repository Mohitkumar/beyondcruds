package lb

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/mohitkumar/mgateway/config"
	"github.com/mohitkumar/mgateway/proxy"
)

var _ http.Handler = (*LoadBalancer)(nil)

type LoadBalancer struct {
	mutex    sync.Mutex
	Targets  []string
	Strategy string
	prxy     http.Handler
	counter  atomic.Uint32
	proxies  []http.Handler
}

func NewLoadBalancer(cfg config.UpstreamConfig) *LoadBalancer {
	proxies := make([]http.Handler, len(cfg.Targets))
	for i, target := range cfg.Targets {
		proxies[i] = proxy.NewProxy(target)
	}
	return &LoadBalancer{
		Targets:  cfg.Targets,
		Strategy: cfg.Strategy,
		proxies:  proxies,
		prxy:     proxies[0],
	}
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch lb.Strategy {
	case "round_robin":
		lb.mutex.Lock()
		defer lb.mutex.Unlock()
		lb.prxy = lb.proxies[lb.counter.Load()]
		lb.counter.Add(1)
		if int(lb.counter.Load()) >= len(lb.proxies) {
			lb.counter.Store(0)
		}
	default:
		lb.prxy = lb.proxies[0]
	}
	lb.prxy.ServeHTTP(w, r)
}
