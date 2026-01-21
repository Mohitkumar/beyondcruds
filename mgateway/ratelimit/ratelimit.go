package ratelimit

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RateLimiter controls the rate of operations
type RateLimiter interface {
	Allow(key string) bool
}

// KeyExtractor defines how to extract the rate limit key from a request
type KeyExtractor func(r *http.Request) string

// IPKeyExtractor extracts the client IP address as the rate limit key
func IPKeyExtractor(r *http.Request) string {
	// Check X-Forwarded-For header first (for proxied requests)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in the chain
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	// Fall back to RemoteAddr
	// RemoteAddr is in the form "IP:port", extract just the IP
	addr := r.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// UserIDKeyExtractor extracts user ID from a header (set by auth middleware)
func UserIDKeyExtractor(r *http.Request) string {
	// Check for user ID header (typically set by auth middleware)
	if userID := r.Header.Get("X-User-ID"); userID != "" {
		return userID
	}
	// Fall back to IP if no user ID
	return IPKeyExtractor(r)
}

// GlobalKeyExtractor returns a constant key for global rate limiting
func GlobalKeyExtractor(r *http.Request) string {
	return "global"
}

// GetKeyExtractor returns the appropriate key extractor based on config
func GetKeyExtractor(keyType string) KeyExtractor {
	switch keyType {
	case "ip":
		return IPKeyExtractor
	case "user_id":
		return UserIDKeyExtractor
	case "global":
		return GlobalKeyExtractor
	default:
		return IPKeyExtractor // Default to IP-based
	}
}

// InMemoryRateLimiter implements a per-key token bucket rate limiter
type InMemoryRateLimiter struct {
	mutex    sync.RWMutex
	limiters map[string]*tokenBucket
	rate     int
	capacity int
}

// tokenBucket represents a single client's token bucket
type tokenBucket struct {
	mutex    sync.Mutex
	tokens   atomic.Uint64
	rate     int
	capacity int
}

func newTokenBucket(rate, capacity int) *tokenBucket {
	tb := &tokenBucket{
		rate:     rate,
		capacity: capacity,
	}
	tb.tokens.Store(uint64(capacity))
	return tb
}

func (tb *tokenBucket) allow() bool {
	tb.mutex.Lock()
	defer tb.mutex.Unlock()

	currentTokens := tb.tokens.Load()
	if currentTokens > 0 {
		tb.tokens.Store(currentTokens - 1)
		return true
	}
	return false
}

func (tb *tokenBucket) refill() {
	tb.mutex.Lock()
	defer tb.mutex.Unlock()

	currentTokens := tb.tokens.Load()
	if currentTokens < uint64(tb.capacity) {
		newTokens := currentTokens + uint64(tb.rate)
		if newTokens > uint64(tb.capacity) {
			newTokens = uint64(tb.capacity)
		}
		tb.tokens.Store(newTokens)
	}
}

// NewInMemoryRateLimiter creates a per-key rate limiter
func NewInMemoryRateLimiter(rate int, capacity int) *InMemoryRateLimiter {
	limiter := &InMemoryRateLimiter{
		limiters: make(map[string]*tokenBucket),
		rate:     rate,
		capacity: capacity,
	}
	limiter.startRefillLoop()
	return limiter
}

// NewInMemoryRateLimiterDefault creates a rate limiter with default settings
func NewInMemoryRateLimiterDefault() *InMemoryRateLimiter {
	return NewInMemoryRateLimiter(2, 10)
}

// startRefillLoop starts a background goroutine that refills all token buckets
func (r *InMemoryRateLimiter) startRefillLoop() {
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			r.mutex.RLock()
			for _, bucket := range r.limiters {
				bucket.refill()
			}
			r.mutex.RUnlock()
		}
	}()
}

// Allow checks if a request with the given key should be allowed
func (r *InMemoryRateLimiter) Allow(key string) bool {
	// Try to get existing bucket with read lock
	r.mutex.RLock()
	bucket, exists := r.limiters[key]
	r.mutex.RUnlock()

	if !exists {
		// Create new bucket with write lock
		r.mutex.Lock()
		// Double-check after acquiring write lock
		bucket, exists = r.limiters[key]
		if !exists {
			bucket = newTokenBucket(r.rate, r.capacity)
			r.limiters[key] = bucket
		}
		r.mutex.Unlock()
	}

	return bucket.allow()
}

// Deprecated: Use NewInMemoryRateLimiter instead
// These are kept for backward compatibility
func NewinMemoryRateLimiter(rate int, capacity int) *InMemoryRateLimiter {
	return NewInMemoryRateLimiter(rate, capacity)
}

func NewinMemoryRateLimiterDefault() *InMemoryRateLimiter {
	return NewInMemoryRateLimiterDefault()
}
