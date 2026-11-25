package httpapi

import (
	"net/http"
	"sync"
	"time"

	rate "golang.org/x/time/rate"
)

const (
	// Global rate limit: 2 requests per second, burst of 5.
	rateLimit   = rate.Limit(2)
	rateBurst   = 5
	cleanupTick = 5 * time.Minute
	entryTTL    = 5 * time.Minute
)

type rateEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// rateLimiter stores per-IP rate limiters with periodic cleanup.
type rateLimiter struct {
	mu     sync.Mutex
	entries map[string]*rateEntry
}

var globalRateLimiter = &rateLimiter{
	entries: make(map[string]*rateEntry),
}

func init() {
	go func() {
		ticker := time.NewTicker(cleanupTick)
		defer ticker.Stop()
		for range ticker.C {
			globalRateLimiter.cleanup()
		}
	}()
}

func (rl *rateLimiter) getLimiter(ip string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if entry, ok := rl.entries[ip]; ok {
		entry.lastSeen = time.Now()
		return entry.limiter
	}
	lim := rate.NewLimiter(rateLimit, rateBurst)
	rl.entries[ip] = &rateEntry{limiter: lim, lastSeen: time.Now()}
	return lim
}

func (rl *rateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	for ip, entry := range rl.entries {
		if now.Sub(entry.lastSeen) > entryTTL {
			delete(rl.entries, ip)
		}
	}
}

func (s *Server) withRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := r.RemoteAddr
		if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
			ip = fwd
		}
		lim := globalRateLimiter.getLimiter(ip)
		if !lim.Allow() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}
