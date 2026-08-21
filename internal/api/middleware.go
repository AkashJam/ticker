package api

import (
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

// RequestLogger emits one structured log line per request via slog,
// replacing Gin's default text logger for consistency with the rest of the
// service's logging.
func RequestLogger(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration", time.Since(start),
		)
	}
}

// rateLimitPerSecond / rateLimitBurst are generous — this is defense in
// depth for an internal-only API (§13's public-facing "IP-rate-limited" is
// primarily Next.js/Caddy's job at the real edge), not the primary abuse
// control, so it shouldn't interfere with normal SSR traffic from the one
// caller (the portfolio container) that actually reaches this API.
const (
	rateLimitPerSecond = 50
	rateLimitBurst     = 100
)

// RateLimit applies a per-client-IP token bucket.
func RateLimit() gin.HandlerFunc {
	var (
		mu       sync.Mutex
		limiters = make(map[string]*rate.Limiter)
	)

	limiterFor := func(ip string) *rate.Limiter {
		mu.Lock()
		defer mu.Unlock()
		l, ok := limiters[ip]
		if !ok {
			l = rate.NewLimiter(rate.Limit(rateLimitPerSecond), rateLimitBurst)
			limiters[ip] = l
		}
		return l
	}

	return func(c *gin.Context) {
		ip, _, err := net.SplitHostPort(c.Request.RemoteAddr)
		if err != nil {
			ip = c.Request.RemoteAddr
		}
		if !limiterFor(ip).Allow() {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}
