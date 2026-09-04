// Package flags holds the service's typed Config and its fail-fast
// validation (portfolio.md §5). The actual CLI flag definitions live in
// cmd/main.go (urfave/cli v3's Sources: cli.EnvVars(...) already
// implements "CLI flag > .env/environment > built-in default" natively) —
// this package is what they get parsed into and validated against.
package flags

import (
	"fmt"
	"time"

	"github.com/joho/godotenv"
)

// LoadDotenv loads a .env file into the process environment if one exists.
// Silently a no-op if it doesn't — real deployments (production compose)
// inject environment variables directly rather than shipping a .env file.
func LoadDotenv() {
	_ = godotenv.Load()
}

// Config is every flag/env-sourced setting the service needs to start.
type Config struct {
	Source          string // "sim" | "finnhub" (§6.2 — only "sim" is implemented in v1)
	Env             string // "dev" | "prod" — informational, shapes log formatting
	Addr            string // HTTP listen address, e.g. ":8080"
	AggWindow       time.Duration
	LogLevel        string
	TimescaleDSN    string
	RedisAddr       string
	HealthchecksURL string // optional dead-man switch ping URL (§13) — empty disables it
	// Consumer names this instance in the ticks:raw consumer group. Empty
	// falls back to aggregate.New's hostname-pid default, correct for
	// today's single-aggregator deployment; ADR-003's revisit trigger (a
	// second aggregator replica) is what would need this set explicitly.
	Consumer string
}

// Validate fails fast on anything that would otherwise surface as a
// confusing runtime error deep in startup.
func (c Config) Validate() error {
	switch c.Source {
	case "sim", "finnhub":
	default:
		return fmt.Errorf("flags: --source must be \"sim\" or \"finnhub\", got %q", c.Source)
	}
	if c.AggWindow <= 0 {
		return fmt.Errorf("flags: --agg-window must be positive, got %s", c.AggWindow)
	}
	if c.Addr == "" {
		return fmt.Errorf("flags: --addr must not be empty")
	}
	if c.TimescaleDSN == "" {
		return fmt.Errorf("flags: TIMESCALE_DSN (or --db-dsn) is required")
	}
	if c.RedisAddr == "" {
		return fmt.Errorf("flags: REDIS_ADDR (or --redis-addr) is required")
	}
	return nil
}
