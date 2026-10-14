// Command backfill generates migration 000006 (portfolio.md ADR-009, Phase 14
// step 0): daily bars for the sweep universe from Yahoo's chart endpoint,
// written as literal SQL so dev, CI and production load identical rows. It is
// a developer tool, not part of the ticker binary or its image.
//
//	go run ./cmd/backfill                      # writes migrations/000006_backfill_daily.{up,down}.sql
//	go run ./cmd/backfill -stored sweep.csv    # also diffs Yahoo against the sweep's bars
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	_ "time/tzdata"

	"github.com/AkashJam/ticker/internal/backfill"
	"github.com/AkashJam/ticker/internal/source"
)

func main() {
	from := flag.String("from", "2026-07-01", "first session to write")
	out := flag.String("out", "migrations", "directory for the migration files")
	stored := flag.String("stored", "", "CSV of the sweep's bars (symbol,date,open,high,low,close) to compare Yahoo against; fetches through today")
	gap := flag.Duration("gap", time.Second, "pause between Yahoo calls")
	tolerance := flag.Float64("tolerance-bp", 0.5, "comparison tolerance in basis points")
	flag.Parse()

	if err := run(*from, *out, *stored, *gap, *tolerance); err != nil {
		log.Fatal(err)
	}
}

func run(fromArg, outDir, storedPath string, gap time.Duration, toleranceBP float64) error {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		return err
	}
	from, err := time.Parse(time.DateOnly, fromArg)
	if err != nil {
		return err
	}
	var storedBars map[string]backfill.Bar
	// Past the cutoff the bars are fetched only to be compared, never written.
	fetchTo := backfill.Cutoff
	if storedPath != "" {
		f, err := os.Open(storedPath)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		if storedBars, err = backfill.ReadStored(f); err != nil {
			return err
		}
		fetchTo = time.Now().UTC().Add(48 * time.Hour)
	}

	ctx := context.Background()
	client := backfill.NewClient()
	var rows []backfill.Bar
	var symbols []string
	var compareAll []backfill.Bar
	for i, s := range source.SweepUniverse {
		if i > 0 {
			time.Sleep(gap)
		}
		chart, err := client.Fetch(ctx, s.Symbol, from, fetchTo)
		if err != nil {
			return err
		}
		if err := backfill.CheckCorporateActions(chart, loc); err != nil {
			return err
		}
		bars, err := backfill.Bars(chart, loc)
		if err != nil {
			return err
		}
		history := backfill.Before(bars, backfill.Cutoff)
		if err := backfill.CheckCount(s.Symbol, history); err != nil {
			return err
		}
		rows = append(rows, history...)
		compareAll = append(compareAll, bars...)
		symbols = append(symbols, s.Symbol)
		log.Printf("%-5s %d closes, %s to %s", s.Symbol, len(history), history[0].Session.Format(time.DateOnly), history[len(history)-1].Session.Format(time.DateOnly))
	}

	up := backfill.RenderUp(rows, from, backfill.Cutoff, time.Now().UTC())
	if err := os.WriteFile(filepath.Join(outDir, "000006_backfill_daily.up.sql"), []byte(up), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, "000006_backfill_daily.down.sql"), []byte(backfill.RenderDown(symbols)), 0o644); err != nil {
		return err
	}
	log.Printf("wrote %d bars for %d symbols", len(rows), len(symbols))

	if storedBars != nil {
		diffs, compared := backfill.Compare(compareAll, storedBars, toleranceBP)
		fmt.Printf("compared %d symbol-days against the sweep: %d field differences above %.2f bp\n", compared, len(diffs), toleranceBP)
		for _, d := range diffs {
			fmt.Printf("  %-5s %s %-5s yahoo %v stored %v (%.2f bp)\n", d.Symbol, d.Session.Format(time.DateOnly), d.Field, d.Yahoo, d.Stored, d.BP)
		}
	}
	return nil
}
