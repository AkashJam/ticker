// Command ticker is the entry point: a thin urfave/cli v3 command tree
// (portfolio.md §5) that loads config and hands off to server.Run. OS
// signal handling lives only here — server.Server.Run/shutdown are plain
// ctx-driven functions, directly callable from a test without delivering a
// real signal (this is what makes the SIGTERM-flush requirement testable).
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata" // the runtime image has no zoneinfo; the sweep schedules in America/New_York

	"github.com/urfave/cli/v3"

	"github.com/AkashJam/ticker/flags"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
	"github.com/AkashJam/ticker/internal/sweep"
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
	referenceHealthchecksFlag := &cli.StringFlag{Name: "healthchecks-reference-url", Sources: cli.EnvVars("HEALTHCHECKS_REFERENCE_URL")}
	consumerFlag := &cli.StringFlag{Name: "consumer", Sources: cli.EnvVars("CONSUMER"), Usage: "ticks:raw consumer group member name; defaults to hostname-pid"}

	sharedFlags := []cli.Flag{sourceFlag, envFlag, addrFlag, aggWindowFlag, logLevelFlag, dbDSNFlag, redisAddrFlag, healthchecksFlag, finnhubKeyFlag, sweepHealthchecksFlag, referenceHealthchecksFlag, consumerFlag}

	configFromCmd := func(cmd *cli.Command) flags.Config {
		return flags.Config{
			Source: cmd.String("source"), Env: cmd.String("env"), Addr: cmd.String("addr"),
			AggWindow: cmd.Duration("agg-window"), LogLevel: cmd.String("log-level"),
			TimescaleDSN: cmd.String("db-dsn"), RedisAddr: cmd.String("redis-addr"),
			HealthchecksURL: cmd.String("healthchecks-url"), Consumer: cmd.String("consumer"),
			FinnhubAPIKey: cmd.String("finnhub-api-key"), HealthchecksSweepURL: cmd.String("healthchecks-sweep-url"),
			HealthchecksReferenceURL: cmd.String("healthchecks-reference-url"),
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
				Name:  "sweep",
				Usage: "write the last completed session's daily bars for the 28 sweep symbols, then exit",
				Flags: sharedFlags,
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return runSweep(ctx, configFromCmd(cmd))
				},
			},
			{
				Name:  "fred",
				Usage: "fetch the FRED index series and write the dataset to --out, or to Redis without it, then exit",
				Flags: append([]cli.Flag{&cli.StringFlag{Name: "out", Usage: "write JSON here instead of Redis; this is how embedded/fred.json is made"}}, sharedFlags...),
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return runFred(ctx, configFromCmd(cmd), cmd.String("out"))
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

// runSweep is a manual run of the daily-bar sweep: the last completed
// session, written whether or not a bar already exists (the upsert is
// idempotent). It does not ping healthchecks.io — that check watches the
// scheduled run.
func runSweep(ctx context.Context, cfg flags.Config) error {
	if err := cfg.ValidateSweep(); err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		return fmt.Errorf("sweep: load America/New_York: %w", err)
	}
	pool, err := store.NewPool(ctx, cfg.TimescaleDSN)
	if err != nil {
		return fmt.Errorf("sweep: %w", err)
	}
	defer pool.Close()

	runner := sweep.NewRunner(sweep.NewFinnhubClient(cfg.FinnhubAPIKey), store.NewTimescale(pool), source.SweepSymbolCodes(), loc, log)
	res, err := runner.Run(ctx, sweep.ModeManual)
	if err != nil {
		return err
	}
	fmt.Printf("sweep: %s session=%s written=%d failed=%v\n", res.State, res.Session.Format(time.DateOnly), res.Written, res.Failed)
	if res.State == sweep.StateFailed {
		return fmt.Errorf("sweep: %d symbol(s) not written", len(res.Failed))
	}
	return nil
}

// runFred is a manual run of the FRED fetcher. With --out it writes the
// dataset to a file, which is how the embedded snapshot is generated and
// refreshed; without it the dataset goes to Redis. It does not ping
// healthchecks.io: that check watches the scheduled run.
func runFred(ctx context.Context, cfg flags.Config, out string) error {
	log := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var cache reference.Cache
	if out == "" {
		if cfg.RedisAddr == "" {
			return fmt.Errorf("fred: REDIS_ADDR (or --redis-addr) is required without --out")
		}
		cache = store.NewRedis(cfg.RedisAddr)
	}
	refresher := fred.NewRefresher(fred.NewClient(), reference.New(cache, reference.Embedded(), log), log)

	if out == "" {
		return refresher.Refresh(ctx)
	}
	ds, err := refresher.Build(ctx)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(ds, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("fred: wrote %s end=%s countries=%d\n", out, ds.End, len(ds.Rows))
	return nil
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
