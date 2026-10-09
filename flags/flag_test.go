package flags

import (
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
