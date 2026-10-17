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
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata" // the runtime image has no zoneinfo; the sweep schedules in America/New_York

	"github.com/urfave/cli/v3"

	"github.com/AkashJam/ticker/flags"
	"github.com/AkashJam/ticker/internal/coverage"
	"github.com/AkashJam/ticker/internal/ecb"
	"github.com/AkashJam/ticker/internal/edgar"
	"github.com/AkashJam/ticker/internal/fred"
	"github.com/AkashJam/ticker/internal/news"
	"github.com/AkashJam/ticker/internal/reference"
	"github.com/AkashJam/ticker/internal/source"
	"github.com/AkashJam/ticker/internal/store"
	"github.com/AkashJam/ticker/internal/sweep"
	"github.com/AkashJam/ticker/internal/trigger"
	"github.com/AkashJam/ticker/internal/worldbank"
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
	triggerThresholdFlag := &cli.FloatFlag{Name: "trigger-threshold", Value: 2.5, Sources: cli.EnvVars("TRIGGER_THRESHOLD"), Usage: "|z| a sensor's residual move must pass on the close to be recorded as an event"}
	splitThresholdFlag := &cli.FloatFlag{Name: "split-threshold", Value: 2, Sources: cli.EnvVars("SPLIT_THRESHOLD"), Usage: "|z| a currency or stock part must pass on its own to count in a trip's currency-split label"}
	fetchContactFlag := &cli.StringFlag{Name: "fetch-contact", Sources: cli.EnvVars("FETCH_CONTACT"), Usage: "name and address SEC EDGAR may contact about this client, sent in the User-Agent; empty disables the EDGAR job"}
	consumerFlag := &cli.StringFlag{Name: "consumer", Sources: cli.EnvVars("CONSUMER"), Usage: "ticks:raw consumer group member name; defaults to hostname-pid"}

	sharedFlags := []cli.Flag{sourceFlag, envFlag, addrFlag, aggWindowFlag, logLevelFlag, dbDSNFlag, redisAddrFlag, healthchecksFlag, finnhubKeyFlag, sweepHealthchecksFlag, referenceHealthchecksFlag, triggerThresholdFlag, splitThresholdFlag, fetchContactFlag, consumerFlag}

	configFromCmd := func(cmd *cli.Command) flags.Config {
		return flags.Config{
			Source: cmd.String("source"), Env: cmd.String("env"), Addr: cmd.String("addr"),
			AggWindow: cmd.Duration("agg-window"), LogLevel: cmd.String("log-level"),
			TimescaleDSN: cmd.String("db-dsn"), RedisAddr: cmd.String("redis-addr"),
			HealthchecksURL: cmd.String("healthchecks-url"), Consumer: cmd.String("consumer"),
			FinnhubAPIKey: cmd.String("finnhub-api-key"), HealthchecksSweepURL: cmd.String("healthchecks-sweep-url"),
			HealthchecksReferenceURL: cmd.String("healthchecks-reference-url"),
			TriggerThreshold:         cmd.Float("trigger-threshold"),
			SplitThreshold:           cmd.Float("split-threshold"),
			FetchContact:             cmd.String("fetch-contact"),
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
				Flags: append(append([]cli.Flag{}, sharedFlags...),
					&cli.BoolFlag{Name: "dry-run", Usage: "fetch real quotes and decide, but write nothing"},
					&cli.StringFlag{Name: "mode", Value: "manual", Usage: "manual (default) | scheduled (the 16:15 decision, so holidays show) | catch-up (the start-up check)"},
				),
				Action: func(ctx context.Context, cmd *cli.Command) error {
					mode, err := sweep.ParseMode(cmd.String("mode"))
					if err != nil {
						return err
					}
					return runSweep(ctx, configFromCmd(cmd), mode, cmd.Bool("dry-run"))
				},
			},
			{
				Name:  "edgar",
				Usage: "fetch the sensors' EDGAR identities, run the drift check, and write the dataset to --out, or to Redis without it, then exit",
				Flags: append([]cli.Flag{&cli.StringFlag{Name: "out", Usage: "write JSON here instead of Redis; this is how embedded/edgar.json is made"}}, sharedFlags...),
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return runEdgar(ctx, configFromCmd(cmd), cmd.String("out"))
				},
			},
			{
				Name:   "newscheck",
				Usage:  "run every news phrase against Google News RSS and print items and distinct outlets, read-only, then exit; run it once from the production container",
				Flags:  sharedFlags,
				Action: func(ctx context.Context, cmd *cli.Command) error { return runNewsCheck(ctx, configFromCmd(cmd)) },
			},
			{
				Name:  "trigger",
				Usage: "judge a session's closes against the threshold and record every trip, then exit; repairs a missed run",
				Flags: append(append([]cli.Flag{}, sharedFlags...),
					&cli.StringFlag{Name: "session", Usage: "the New York session date, YYYY-MM-DD; default the newest session in the database"},
					&cli.BoolFlag{Name: "dry-run", Usage: "decide and print, but write nothing"},
				),
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return runTrigger(ctx, configFromCmd(cmd), cmd.String("session"), cmd.Bool("dry-run"))
				},
			},
			{
				Name:   "sigma",
				Usage:  "print each sensor's residual return, sigma and z on the newest daily close, read-only, then exit",
				Flags:  sharedFlags,
				Action: func(ctx context.Context, cmd *cli.Command) error { return runSigma(ctx, configFromCmd(cmd)) },
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
				Name:  "ecb",
				Usage: "fetch the ECB reference rates and write the dataset to --out, or to Redis without it, then exit",
				Flags: append([]cli.Flag{&cli.StringFlag{Name: "out", Usage: "write JSON here instead of Redis; this is how embedded/ecb.json is made"}}, sharedFlags...),
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return runECB(ctx, configFromCmd(cmd), cmd.String("out"))
				},
			},
			{
				Name:  "worldbank",
				Usage: "fetch the World Bank series and write the dataset to --out, or to Redis without it, then exit",
				Flags: append([]cli.Flag{&cli.StringFlag{Name: "out", Usage: "write JSON here instead of Redis; this is how embedded/worldbank.json is made"}}, sharedFlags...),
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return runWorldBank(ctx, configFromCmd(cmd), cmd.String("out"))
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

// runSweep runs the daily-bar sweep once. In the default manual mode it
// writes the last completed session whether or not a bar already exists (the
// upsert is idempotent). It never pings healthchecks.io: that check watches
// the scheduled run. With dryRun it makes the same quote calls and decisions
// but cannot write (sweep.ReadOnly), so --mode scheduled shows what the 16:15
// run would do right now, including "holiday" on a weekend.
func runSweep(ctx context.Context, cfg flags.Config, mode sweep.Mode, dryRun bool) error {
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

	var st sweep.Store = store.NewTimescale(pool)
	label := "sweep"
	if dryRun {
		st = sweep.ReadOnly(st)
		label = fmt.Sprintf("sweep (dry run, mode=%s)", mode)
	}
	runner := sweep.NewRunner(sweep.NewFinnhubClient(cfg.FinnhubAPIKey), st, source.SweepSymbolCodes(), loc, log)
	res, err := runner.Run(ctx, mode)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s session=%s written=%d failed=%v\n", label, res.State, res.Session.Format(time.DateOnly), res.Written, res.Failed)
	fmt.Println(sweep.Explain(res, dryRun))
	if res.State == sweep.StateFailed {
		return fmt.Errorf("sweep: %d symbol(s) not written", len(res.Failed))
	}
	return nil
}

// runTrigger judges one session, as the scheduled run does after the sweep. It
// is idempotent, so it is safe to repeat; with --dry-run it records nothing and
// says which trips it would write. The server does the same thing itself after
// every sweep and at start-up, so this is for repair and for looking.
func runTrigger(ctx context.Context, cfg flags.Config, sessionArg string, dryRun bool) error {
	if err := cfg.ValidateTrigger(); err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		return fmt.Errorf("trigger: load America/New_York: %w", err)
	}
	pool, err := store.NewPool(ctx, cfg.TimescaleDSN)
	if err != nil {
		return fmt.Errorf("trigger: %w", err)
	}
	defer pool.Close()
	ts := store.NewTimescale(pool)

	var session time.Time
	if sessionArg != "" {
		if session, err = time.Parse(time.DateOnly, sessionArg); err != nil {
			return fmt.Errorf("trigger: --session %q: %w", sessionArg, err)
		}
	} else {
		latest, ok, err := ts.LatestCandle(ctx, source.SweepUniverse[0].Symbol, trigger.DailyInterval)
		if err != nil {
			return fmt.Errorf("trigger: %w", err)
		}
		if !ok {
			return fmt.Errorf("trigger: no daily bars yet")
		}
		session = latest.Time
	}

	// The reference table needs Redis only for a refreshed copy; without an
	// address the embedded one answers.
	var cache reference.Cache
	if cfg.RedisAddr != "" {
		cache = store.NewRedis(cfg.RedisAddr)
	}
	refStore := reference.New(cache, reference.Embedded(), log)
	var events trigger.EventStore = ts
	if dryRun {
		events = trigger.ReadOnly(ts)
	}
	// EDGAR needs a contact; without one the filings source is an error, frozen.
	var filer coverage.FilingSource
	if cfg.FetchContact != "" {
		c, err := edgar.NewClient(cfg.FetchContact)
		if err != nil {
			return fmt.Errorf("trigger: %w", err)
		}
		filer = c
	}
	judge := &trigger.Runner{
		Coverage: coverage.NewProduction(refStore, filer, log),
		Candles:  ts, Events: events, Loc: loc, Threshold: cfg.TriggerThreshold, Log: log,
		Rates: &ecb.RateSource{Store: refStore, Feed: ecb.NewClient()}, SplitThreshold: cfg.SplitThreshold,
		Thin: func(ctx context.Context) (map[string]bool, error) {
			sensors, err := refStore.Sensors(ctx)
			if err != nil {
				return nil, err
			}
			return server.ThinSensors(sensors), nil
		},
	}
	sum, err := judge.Run(ctx, session)
	if err != nil {
		return err
	}
	verb := "recorded"
	if dryRun {
		verb = "would record"
	}
	fmt.Printf("session %s: %d evaluated, %d skipped, %d past %.2f sigma\n", sum.Session.Format(time.DateOnly), sum.Evaluated, len(sum.Skipped), len(sum.Trips), cfg.TriggerThreshold)
	for _, t := range sum.Trips {
		bench := "no benchmark"
		if t.Benchmark != nil {
			bench = "vs " + *t.Benchmark
		}
		fmt.Printf("  %-5s z %+6.2f  residual %+7.3f%%  sigma %6.3f%%  %s  caveats %v\n", t.Symbol, t.Z, t.Residual*100, t.Sigma*100, bench, t.Caveats)
		if sp, ok := sum.Splits[t.Symbol]; ok {
			if sp.Status == "none" {
				fmt.Printf("        no split: %s\n", sp.Reason)
			} else {
				fmt.Printf("        %s (%s, fix %s, %s): currency %+.3f%% z %+.2f, stock %+.3f%% z %+.2f\n", sp.Label, sp.Currency, sp.FixDate, sp.Source,
					sp.CurrencyPart.Value*100, sp.CurrencyPart.Z, sp.StockPart.Value*100, sp.StockPart.Z)
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(sum.Coverages)) {
		fmt.Printf("  coverage %s\n%s", key, coverageSummary(sum.Coverages[key]))
	}
	fmt.Printf("%s %d, already recorded %d", verb, sum.Inserted, sum.Existing)
	if dryRun {
		fmt.Printf(" (nothing written)")
	}
	fmt.Println()
	for _, sym := range slices.Sorted(maps.Keys(sum.Skipped)) {
		fmt.Printf("  %-5s not evaluated: %s\n", sym, sum.Skipped[sym])
	}
	if len(sum.Mismatched) > 0 {
		fmt.Printf("WARNING: recorded events differ from a fresh evaluation (kept): %v\n", sum.Mismatched)
	}
	return nil
}

// runSigma prints the figures Phase 15's threshold is made of, for every
// sensor, on the newest daily close in the database. It reads candles and
// writes nothing, so it is the way to check a z against a spreadsheet before
// anything fires (phase15.md step 2).
func runSigma(ctx context.Context, cfg flags.Config) error {
	if cfg.TimescaleDSN == "" {
		return fmt.Errorf("sigma: TIMESCALE_DSN (or --db-dsn) is required")
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := store.NewPool(ctx, cfg.TimescaleDSN)
	if err != nil {
		return fmt.Errorf("sigma: %w", err)
	}
	defer pool.Close()

	var sensors []string
	for _, s := range source.StreamUniverse() {
		sensors = append(sensors, s.Symbol)
	}
	all, err := trigger.Load(ctx, store.NewTimescale(pool), source.SweepSymbolCodes())
	if err != nil {
		return err
	}
	results, errs := trigger.EvaluateLatest(all, sensors)
	if len(results) > 0 {
		fmt.Printf("session %s, residual = symbol - benchmark, sigma over the %d sessions before it\n", results[0].Date.Format(time.DateOnly), trigger.Window)
		fmt.Printf("%-6s %-6s %9s %9s %9s %8s %8s  %s\n", "symbol", "bench", "return%", "bench%", "resid%", "sigma%", "z", "window from")
	}
	for _, r := range results {
		bench, bret := "-", "-"
		if r.Point.Benchmark != nil {
			bench, bret = r.Benchmark, fmt.Sprintf("%+.3f", *r.Point.Benchmark*100)
		}
		fmt.Printf("%-6s %-6s %+9.3f %9s %+9.3f %8.3f %+8.2f  %s\n", r.Symbol, bench, r.Return*100, bret, r.Residual*100, r.Sigma*100, r.Z, r.WindowStart.Format(time.DateOnly))
	}
	for _, sym := range sensors {
		if err, ok := errs[sym]; ok {
			fmt.Printf("%-6s not evaluated: %v\n", sym, err)
		}
	}
	return nil
}

// runEdgar is a manual run of the EDGAR identity job. With --out it writes the
// dataset to a file, which is how the embedded snapshot is generated; without it
// the dataset goes to Redis. It reports the drift it found and does not ping
// healthchecks.io: that check watches the scheduled run. The contact is sent to
// SEC and is never printed.
func runEdgar(ctx context.Context, cfg flags.Config, out string) error {
	log := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := edgar.NewClient(cfg.FetchContact)
	if err != nil {
		return fmt.Errorf("edgar: %w (set it in the environment; it is sent to SEC in the User-Agent)", err)
	}
	var cache reference.Cache
	if out == "" {
		if cfg.RedisAddr == "" {
			return fmt.Errorf("edgar: REDIS_ADDR (or --redis-addr) is required without --out")
		}
		cache = store.NewRedis(cfg.RedisAddr)
	}
	refresher := edgar.NewRefresher(client, reference.New(cache, reference.Embedded(), log), log)

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
	fmt.Printf("edgar: wrote %s sensors=%d drift=%d\n", out, len(ds.Sensors), len(ds.Drift))
	for _, d := range ds.Drift {
		fmt.Printf("  %-5s %-17s %s\n", d.Symbol, d.Kind, d.Detail)
	}
	return nil
}

// runNewsCheck runs every news phrase in the reference tables against Google
// News RSS and prints items and distinct outlets over two days, as
// research/news-queries.md recorded them. It writes nothing. Run it once from
// the production container: the spec asks that the phrases be tested from the
// address that will use them.
func runNewsCheck(ctx context.Context, cfg flags.Config) error {
	log := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	ref := reference.New(nil, reference.Embedded(), log)
	countries, err := ref.Countries(ctx)
	if err != nil {
		return err
	}
	sensors, err := ref.Sensors(ctx)
	if err != nil {
		return err
	}
	type phrase struct{ kind, symbol, text string }
	var phrases []phrase
	for _, c := range countries {
		if c.Sensor == nil {
			continue
		}
		phrases = append(phrases, phrase{"economy", *c.Sensor, *c.EconomyQuery}, phrase{"sector", *c.Sensor, *c.SectorQuery})
	}
	for _, s := range sensors {
		if s.NewsTopic != nil {
			label := "topic"
			if s.NewsTopicProvisional {
				label = "topic*"
			}
			phrases = append(phrases, phrase{label, s.Symbol, *s.NewsTopic})
		}
	}

	client := news.NewClient()
	failed := 0
	fmt.Printf("%-8s %-5s %6s %8s  %s\n", "kind", "sym", "items", "outlets", "phrase")
	for i, p := range phrases {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
		today := time.Now().UTC().Truncate(24 * time.Hour)
		items, err := client.Search(ctx, p.text, today.AddDate(0, 0, -2), today.AddDate(0, 0, 1))
		if err != nil {
			failed++
			fmt.Printf("%-8s %-5s %6s %8s  %s  (%v)\n", p.kind, p.symbol, "-", "-", p.text, err)
			continue
		}
		fmt.Printf("%-8s %-5s %6d %8d  %s\n", p.kind, p.symbol, len(items), news.DistinctOutlets(items), p.text)
	}
	fmt.Println("(* provisional wording)")
	if failed > 0 {
		return fmt.Errorf("newscheck: %d of %d phrases failed", failed, len(phrases))
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
	fmt.Printf("fred: wrote %s countries=%d\n", out, len(ds.Rows))
	return nil
}

// runECB is a manual run of the ECB fetcher. With --out it seeds from the full
// history zip and writes the dataset to a file, which is how the embedded
// snapshot is generated and refreshed; without it the dataset goes to Redis.
// It does not ping healthchecks.io: that check watches the scheduled run.
func runECB(ctx context.Context, cfg flags.Config, out string) error {
	log := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var cache reference.Cache
	if out == "" {
		if cfg.RedisAddr == "" {
			return fmt.Errorf("ecb: REDIS_ADDR (or --redis-addr) is required without --out")
		}
		cache = store.NewRedis(cfg.RedisAddr)
	}
	refresher := ecb.NewRefresher(ecb.NewClient(), fred.NewClient(), reference.New(cache, reference.Embedded(), log), log)

	if out == "" {
		return refresher.Refresh(ctx)
	}
	st, err := refresher.Build(ctx)
	if err != nil {
		return err
	}
	ds, err := refresher.Compute(ctx, st)
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
	fmt.Printf("ecb: wrote %s asOf=%s currencies=%d\n", out, ds.AsOf, len(ds.Currencies))
	return nil
}

// runWorldBank is a manual run of the World Bank fetcher. With --out it writes
// the dataset to a file, which is how the embedded snapshot is generated and
// refreshed; without it the dataset goes to Redis. Everyday spending reads the
// ECB's annual averages from the store, so run `ticker ecb` first. It does not
// ping healthchecks.io: that check watches the scheduled run.
func runWorldBank(ctx context.Context, cfg flags.Config, out string) error {
	log := newLogger(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var cache reference.Cache
	if out == "" {
		if cfg.RedisAddr == "" {
			return fmt.Errorf("worldbank: REDIS_ADDR (or --redis-addr) is required without --out")
		}
		cache = store.NewRedis(cfg.RedisAddr)
	}
	refresher := worldbank.NewRefresher(worldbank.NewClient(), reference.New(cache, reference.Embedded(), log), log)

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
	fmt.Printf("worldbank: wrote %s countries=%d\n", out, len(ds.Rows))
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

// coverageSummary renders a stored coverage for the terminal: one line per
// source, with the counts a reader checks first.
func coverageSummary(b []byte) string {
	cov, err := coverage.Decode(b)
	if err != nil || cov == nil {
		return fmt.Sprintf("    (unreadable: %v)\n", err)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "    stage %s, complete %v, routing %s, window %s to %s\n", cov.Stage, cov.Complete, cov.Routing, cov.Window.From, cov.Window.To)
	for _, name := range slices.Sorted(maps.Keys(cov.Sources)) {
		s := cov.Sources[name]
		line := fmt.Sprintf("    %-12s %-5s attempts %d", name, s.Status, s.Attempts)
		switch {
		case s.Error != "":
			line += "  error: " + s.Error
		case s.Absent != nil:
			line += "  absent: " + s.Absent.Reason
		case len(s.Outlets) > 0:
			var before, after int
			for _, o := range s.Outlets {
				for _, it := range o.Items {
					if it.Side == "before" {
						before++
					} else {
						after++
					}
				}
			}
			line += fmt.Sprintf("  %d outlets, %d headlines (%d before, %d after the close); query returned %d, %d excluded, %d not English", len(s.Outlets), before+after, before, after, s.Total, s.Excluded, s.NonEnglish)
		default:
			line += fmt.Sprintf("  %d items", len(s.Items))
		}
		out.WriteString(line + "\n")
	}
	return out.String()
}
