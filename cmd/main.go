// Command ticker is the entry point: a thin urfave/cli v3 command tree
// (portfolio.md §5) that loads config and hands off to server.Run. OS
// signal handling lives only here — server.Server.Run/shutdown are plain
// ctx-driven functions, directly callable from a test without delivering a
// real signal (this is what makes the SIGTERM-flush requirement testable).
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/AkashJam/ticker/flags"
	"github.com/AkashJam/ticker/migrations"
	"github.com/AkashJam/ticker/server"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	flags.LoadDotenv()

	sourceFlag := &cli.StringFlag{Name: "source", Value: "sim", Sources: cli.EnvVars("SOURCE"), Usage: "sim | finnhub"}
	envFlag := &cli.StringFlag{Name: "env", Value: "dev", Sources: cli.EnvVars("ENV")}
	addrFlag := &cli.StringFlag{Name: "addr", Value: ":8080", Sources: cli.EnvVars("ADDR")}
	aggWindowFlag := &cli.DurationFlag{Name: "agg-window", Value: time.Minute, Sources: cli.EnvVars("AGG_WINDOW")}
	logLevelFlag := &cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("LOG_LEVEL")}
	dbDSNFlag := &cli.StringFlag{Name: "db-dsn", Sources: cli.EnvVars("TIMESCALE_DSN")}
	redisAddrFlag := &cli.StringFlag{Name: "redis-addr", Sources: cli.EnvVars("REDIS_ADDR")}
	healthchecksFlag := &cli.StringFlag{Name: "healthchecks-url", Sources: cli.EnvVars("HEALTHCHECKS_URL")}
	finnhubKeyFlag := &cli.StringFlag{Name: "finnhub-api-key", Sources: cli.EnvVars("FINNHUB_API_KEY")}
	sweepHealthchecksFlag := &cli.StringFlag{Name: "healthchecks-sweep-url", Sources: cli.EnvVars("HEALTHCHECKS_SWEEP_URL")}
	consumerFlag := &cli.StringFlag{Name: "consumer", Sources: cli.EnvVars("CONSUMER"), Usage: "ticks:raw consumer group member name; defaults to hostname-pid"}

	sharedFlags := []cli.Flag{sourceFlag, envFlag, addrFlag, aggWindowFlag, logLevelFlag, dbDSNFlag, redisAddrFlag, healthchecksFlag, finnhubKeyFlag, sweepHealthchecksFlag, consumerFlag}

	configFromCmd := func(cmd *cli.Command) flags.Config {
		return flags.Config{
			Source: cmd.String("source"), Env: cmd.String("env"), Addr: cmd.String("addr"),
			AggWindow: cmd.Duration("agg-window"), LogLevel: cmd.String("log-level"),
			TimescaleDSN: cmd.String("db-dsn"), RedisAddr: cmd.String("redis-addr"),
			HealthchecksURL: cmd.String("healthchecks-url"), Consumer: cmd.String("consumer"),
			FinnhubAPIKey: cmd.String("finnhub-api-key"), HealthchecksSweepURL: cmd.String("healthchecks-sweep-url"),
		}
	}

	root := &cli.Command{
		Name:           "ticker",
		Usage:          "portfolio.md's market-data backend",
		Flags:          sharedFlags,
		DefaultCommand: "serve",
		Commands: []*cli.Command{
			{
				Name:  "serve",
				Usage: "run the ticker service (default command)",
				Flags: sharedFlags,
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return runServe(ctx, configFromCmd(cmd))
				},
			},
			{
				Name:  "migrate",
				Usage: "apply pending database migrations, then exit",
				Flags: sharedFlags,
				Action: func(_ context.Context, cmd *cli.Command) error {
					return runMigrate(configFromCmd(cmd))
				},
			},
			{
				Name:  "version",
				Usage: "print the build version",
				Action: func(_ context.Context, _ *cli.Command) error {
					fmt.Println(version)
					return nil
				},
			},
		},
	}

	if err := root.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "ticker:", err)
		os.Exit(1)
	}
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func runServe(ctx context.Context, cfg flags.Config) error {
	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv, err := server.New(ctx, cfg, log)
	if err != nil {
		return err
	}
	return srv.Run(ctx)
}

// runMigrate applies the embedded migrations (migrations/embed.go) against
// TimescaleDSN, then exits — the `ticker-migrate` one-shot Compose service
// gates the main `ticker` service's startup on this succeeding, since the
// distroless runtime image has no shell to run a migration script in.
func runMigrate(cfg flags.Config) error {
	if cfg.TimescaleDSN == "" {
		return fmt.Errorf("migrate: TIMESCALE_DSN (or --db-dsn) is required")
	}

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("migrate: embedded source: %w", err)
	}

	db, err := sql.Open("pgx", cfg.TimescaleDSN)
	if err != nil {
		return fmt.Errorf("migrate: open: %w", err)
	}
	defer func() { _ = db.Close() }()

	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		return fmt.Errorf("migrate: driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("migrate: init: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate: up: %w", err)
	}

	fmt.Println("migrate: up to date")
	return nil
}
