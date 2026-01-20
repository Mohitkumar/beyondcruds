package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

type RateLimiter interface {
	Allow() bool
}

// InMemoryRateLimiter implements a simple token bucket rate limiter
type InMemoryRateLimiter struct {
	mutex    sync.Mutex
	tokens   atomic.Uint64
	Rate     int
	Capacity int
}

func NewinMemoryRateLimiter(rate int, capacity int) *InMemoryRateLimiter {
	limiter := &InMemoryRateLimiter{
		Rate:     rate,
		Capacity: capacity,
	}
	limiter.Initialize()
	return limiter
}

func NewinMemoryRateLimiterDefault() *InMemoryRateLimiter {
	limiter := &InMemoryRateLimiter{
		Rate:     2,
		Capacity: 10,
	}
	limiter.Initialize()
	return limiter
}

func (r *InMemoryRateLimiter) Initialize() {
	r.tokens.Store(uint64(r.Capacity))
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			r.mutex.Lock()
			currentTokens := r.tokens.Load()
			if currentTokens < uint64(r.Capacity) {
				r.tokens.Store(currentTokens + uint64(r.Rate))
				if r.tokens.Load() > uint64(r.Capacity) {
					r.tokens.Store(uint64(r.Capacity))
				}
			}
			r.mutex.Unlock()
		}
	}()
}

func (r *InMemoryRateLimiter) Allow() bool {
	r.mutex.Lock()
	currentTokens := r.tokens.Load()
	if currentTokens > 0 {
		r.tokens.Store(currentTokens - 1)
		r.mutex.Unlock()
		return true
	}
	r.mutex.Unlock()
	return false
}
