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
	// FinnhubAPIKey authenticates Finnhub REST calls (the post-close /quote
	// sweep, and the adapter later). Required with --source finnhub; the
	// sweep enforces its own requirement. Never log it.
	FinnhubAPIKey string
	// HealthchecksSweepURL is the sweep's own healthchecks.io check, separate
	// from the ingest dead-man so a missed sweep pages on its own. Empty
	// disables the ping.
	HealthchecksSweepURL string
	// HealthchecksReferenceURL is the Atlas reference fetchers' healthchecks.io
	// check (FRED today; ECB and World Bank join it). Empty disables the ping.
	HealthchecksReferenceURL string
	// TriggerThreshold is the |z| a sensor's residual move must pass on the
	// close to be recorded as an event (portfolio.md §15 Phase 15). It is stored
	// on every event, so changing it never rewrites what an old event says fired it.
	TriggerThreshold float64
	// Consumer names this instance in the ticks:raw consumer group. Empty
	// falls back to aggregate.New's hostname-pid default, correct for
	// today's single-aggregator deployment; ADR-003's revisit trigger (a
	// second aggregator replica) is what would need this set explicitly.
	Consumer string
}

// ValidateSweep is Validate plus what the daily-bar sweep needs, for the
// `sweep` subcommand: the key, because without it every call is a 401.
func (c Config) ValidateSweep() error {
	if c.TimescaleDSN == "" {
		return fmt.Errorf("flags: TIMESCALE_DSN (or --db-dsn) is required")
	}
	if c.FinnhubAPIKey == "" {
		return fmt.Errorf("flags: FINNHUB_API_KEY (or --finnhub-api-key) is required to sweep")
	}
	return nil
}

// MaxTriggerThreshold bounds --trigger-threshold. A threshold above it would
// fire only on a move the sixty-session history cannot even produce (a z over
// about 7.8 is arithmetically out of reach for 61 points), so it is a typo.
const MaxTriggerThreshold = 10

// ValidateTrigger is what the `trigger` subcommand needs: the database and a
// sane threshold.
func (c Config) ValidateTrigger() error {
	if c.TimescaleDSN == "" {
		return fmt.Errorf("flags: TIMESCALE_DSN (or --db-dsn) is required")
	}
	return c.validateThreshold()
}

func (c Config) validateThreshold() error {
	if !(c.TriggerThreshold > 0 && c.TriggerThreshold <= MaxTriggerThreshold) {
		return fmt.Errorf("flags: --trigger-threshold must be in (0, %d], got %v", MaxTriggerThreshold, c.TriggerThreshold)
	}
	return nil
}

// Validate fails fast on anything that would otherwise surface as a
// confusing runtime error deep in startup.
func (c Config) Validate() error {
	switch c.Source {
	case "sim", "finnhub":
	default:
		return fmt.Errorf("flags: --source must be \"sim\" or \"finnhub\", got %q", c.Source)
	}
	if c.Source == "finnhub" && c.FinnhubAPIKey == "" {
		return fmt.Errorf("flags: FINNHUB_API_KEY (or --finnhub-api-key) is required with --source finnhub")
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
	return c.validateThreshold()
}
