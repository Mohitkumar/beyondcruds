package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

func NewProxy(urlStr string) http.Handler {
	u, _ := url.Parse(urlStr)
	proxy := httputil.NewSingleHostReverseProxy(u)

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	proxy.Transport = transport

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		status := http.StatusBadGateway
		msg := err.Error()
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		var netErr net.Error
		if errors.As(err, &netErr) {
			if netErr.Timeout() {
				status = http.StatusGatewayTimeout
			}
		}
		w.WriteHeader(status)
		w.Write([]byte(msg))
	}
	return proxy
}
