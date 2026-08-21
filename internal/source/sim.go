package source

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Regime distinguishes the 4 SIM: symbols' volatility characters
// (portfolio.md §6.2) so the live demo visibly shows different price
// behavior per symbol rather than 4 copies of the same random walk.
type Regime string

const (
	RegimeSteady        Regime = "steady"
	RegimeHighVol       Regime = "high_vol"
	RegimeMeanReverting Regime = "mean_reverting"
	RegimeGappy         Regime = "gappy"
)

// SymbolInfo is the static metadata for a tracked symbol — the shape
// GET /symbols returns and what migrations seed into the `symbols` table.
type SymbolInfo struct {
	Symbol   string
	Name     string
	Type     AssetType
	Exchange string
	Regime   Regime
}

// Symbols is the fixed v1 universe: 4 SIM: regime symbols. Exported so
// migrations seeding and API fallbacks share one source of truth instead of
// duplicating the symbol list as string literals.
var Symbols = []SymbolInfo{
	{Symbol: "SIM:NOVA", Name: "Nova (simulated)", Type: AssetTypeStock, Exchange: "SIM", Regime: RegimeSteady},
	{Symbol: "SIM:HELIX", Name: "Helix (simulated)", Type: AssetTypeStock, Exchange: "SIM", Regime: RegimeHighVol},
	{Symbol: "SIM:ORBIT", Name: "Orbit (simulated)", Type: AssetTypeStock, Exchange: "SIM", Regime: RegimeMeanReverting},
	{Symbol: "SIM:PULSE", Name: "Pulse (simulated)", Type: AssetTypeStock, Exchange: "SIM", Regime: RegimeGappy},
}

var basePrices = map[string]float64{
	"SIM:NOVA":  180.00,
	"SIM:HELIX": 120.00,
	"SIM:ORBIT": 220.00,
	"SIM:PULSE": 75.00,
}

// tickInterval jitter — not specified anywhere in portfolio.md (flagged and
// resolved with the user during planning): ~1-3s per symbol, independently
// jittered, so the 4 regimes visibly desynchronize rather than ticking in
// lockstep.
const (
	minTickInterval = 1 * time.Second
	maxTickInterval = 3 * time.Second
)

// Sim is the v1 MarketSource — SIM: regime symbols (this file) plus the
// Cost-of-Living family (cost_of_living.go). It is the sole source in v1
// (ADR-005); Finnhub (§16.1) is a future second implementation of the same
// interface, not a change to this one.
type Sim struct{}

func NewSim() *Sim { return &Sim{} }

func (s *Sim) Name() string { return "sim" }

// Backfill is a no-op for v1: the sim source never has a real outage to
// backfill from (that concept belongs to a live feed's WS-reconnect path,
// §16.1) — it always returns an empty slice.
func (s *Sim) Backfill(_ context.Context, _ string, _, _ time.Time) ([]Candle, error) {
	return nil, nil
}

// Subscribe spawns one independently-jittered generator goroutine per
// requested symbol, fanning all of them into a single shared channel that
// closes once every generator has exited (on ctx cancellation).
func (s *Sim) Subscribe(ctx context.Context, symbols []string) (<-chan NormalizedTick, error) {
	out := make(chan NormalizedTick, 64)

	var wg sync.WaitGroup
	for _, symbol := range symbols {
		base, ok := basePrices[symbol]
		if !ok {
			continue // not a SIM: symbol this source knows about — silently skip
		}
		regime := regimeOf(symbol)

		wg.Add(1)
		go func(symbol string, base float64, regime Regime) {
			defer wg.Done()
			runGenerator(ctx, symbol, base, regime, out)
		}(symbol, base, regime)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out, nil
}

// SymbolByCode looks up a tracked symbol's static metadata by its code
// (e.g. "SIM:NOVA") — used by API handlers that need the display name
// alongside dynamic price data without a DB round trip for fixed metadata.
func SymbolByCode(symbol string) (SymbolInfo, bool) {
	for _, s := range Symbols {
		if s.Symbol == symbol {
			return s, true
		}
	}
	return SymbolInfo{}, false
}

func regimeOf(symbol string) Regime {
	for _, s := range Symbols {
		if s.Symbol == symbol {
			return s.Regime
		}
	}
	return RegimeSteady
}

// runGenerator drives one symbol's price forward tick by tick until ctx is
// canceled. Each symbol owns an independent *rand.Rand so the 4 goroutines
// never contend on shared random state.
func runGenerator(ctx context.Context, symbol string, base float64, regime Regime, out chan<- NormalizedTick) {
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	price := base

	for {
		wait := minTickInterval + time.Duration(rng.Float64()*float64(maxTickInterval-minTickInterval))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		price = nextPrice(rng, price, base, regime)
		volume := nextVolume(rng)

		select {
		case out <- NormalizedTick{
			Symbol:    symbol,
			Price:     roundToCents(price),
			Volume:    volume,
			Timestamp: time.Now().UTC(),
			AssetType: AssetTypeStock,
			Simulated: true,
		}:
		case <-ctx.Done():
			return
		}
	}
}

// nextPrice advances price by one step, shaped per regime. All sigmas are
// expressed as a fraction of the current price so behavior scales sensibly
// regardless of a symbol's base price.
func nextPrice(rng *rand.Rand, price, anchor float64, regime Regime) float64 {
	switch regime {
	case RegimeHighVol:
		sigma := price * 0.004
		price += rng.NormFloat64() * sigma

	case RegimeMeanReverting:
		const kappa = 0.02 // pull-back strength per tick toward anchor
		sigma := price * 0.0015
		price += kappa*(anchor-price) + rng.NormFloat64()*sigma

	case RegimeGappy:
		sigma := price * 0.0003
		price += rng.NormFloat64() * sigma
		const gapProbability = 0.03
		if rng.Float64() < gapProbability {
			gapPct := 0.01 + rng.Float64()*0.02 // 1-3% jump
			if rng.Float64() < 0.5 {
				gapPct = -gapPct
			}
			price += price * gapPct
		}

	case RegimeSteady:
		fallthrough
	default:
		sigma := price * 0.0003
		price += rng.NormFloat64() * sigma
	}

	if price < 0.01 {
		price = 0.01 // price can't go non-positive — floor it
	}
	return price
}

func nextVolume(rng *rand.Rand) float64 {
	const baseVolume = 500.0
	v := baseVolume * (0.4 + math.Abs(rng.NormFloat64()))
	return math.Round(v)
}

func roundToCents(v float64) float64 {
	return math.Round(v*100) / 100
}
