package flags

import (
	"math"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		Source:       "sim",
		Addr:         ":8080",
		AggWindow:    time.Minute,
		TimescaleDSN: "postgres://ticker:ticker@localhost:5432/ticker",
		RedisAddr:    "localhost:6379",

		TriggerThreshold: 2.5,
		SplitThreshold:   2,
	}
}

func TestValidateSweep(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"key and DSN present", func(c *Config) { c.FinnhubAPIKey = "k" }, false},
		{"no key", func(*Config) {}, true},
		{"no DSN", func(c *Config) { c.FinnhubAPIKey = "k"; c.TimescaleDSN = "" }, true},
		{"Redis is not needed", func(c *Config) { c.FinnhubAPIKey = "k"; c.RedisAddr = "" }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			if err := c.ValidateSweep(); (err != nil) != tt.wantErr {
				t.Fatalf("ValidateSweep() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"sim without a Finnhub key is fine", func(*Config) {}, false},
		{"sim ignores the sweep URL being empty", func(c *Config) { c.HealthchecksSweepURL = "" }, false},
		{"finnhub without a key fails fast", func(c *Config) { c.Source = "finnhub" }, true},
		{"finnhub with a key passes", func(c *Config) { c.Source = "finnhub"; c.FinnhubAPIKey = "k" }, false},
		{"unknown source", func(c *Config) { c.Source = "polygon" }, true},
		{"non-positive window", func(c *Config) { c.AggWindow = 0 }, true},
		{"empty DSN", func(c *Config) { c.TimescaleDSN = "" }, true},
		{"empty Redis address", func(c *Config) { c.RedisAddr = "" }, true},
		{"zero threshold", func(c *Config) { c.TriggerThreshold = 0 }, true},
		{"negative threshold", func(c *Config) { c.TriggerThreshold = -2.5 }, true},
		{"absurd threshold", func(c *Config) { c.TriggerThreshold = 25 }, true},
		{"NaN threshold", func(c *Config) { c.TriggerThreshold = math.NaN() }, true},
		{"zero split threshold", func(c *Config) { c.SplitThreshold = 0 }, true},
		{"absurd split threshold", func(c *Config) { c.SplitThreshold = 11 }, true},
		{"the bound itself is allowed", func(c *Config) { c.TriggerThreshold = MaxTriggerThreshold }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			if err := c.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTrigger(t *testing.T) {
	c := validConfig()
	if err := c.ValidateTrigger(); err != nil {
		t.Fatalf("a valid config: %v", err)
	}
	c.TimescaleDSN = ""
	if c.ValidateTrigger() == nil {
		t.Error("no DSN must fail")
	}
	c = validConfig()
	c.TriggerThreshold = 0
	if c.ValidateTrigger() == nil {
		t.Error("a zero threshold must fail")
	}
	c = validConfig()
	c.RedisAddr, c.Addr = "", ""
	if err := c.ValidateTrigger(); err != nil {
		t.Errorf("Redis and the listen address are not needed to judge a session: %v", err)
	}
}
