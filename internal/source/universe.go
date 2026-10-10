package source

// SweepUniverse is the daily-bar sweep's 28 real symbols (portfolio.md "The
// symbol universe"): 13 country sensors, 5 global sensors and 10 sector ETFs.
// It is deliberately separate from Symbols, which stays the simulator's
// SIM: list — a sim walk under a real ticker would be fabrication with a
// real name on it, so the two lists are disjoint by construction.
//
// The sweep reads this list; the symbols table carries the same rows with
// tracked = false (migration 000003) until Phase 14 promotes them. The first
// entry doubles as the sweep's canary: its quote decides whether a session
// exists and is complete.
//
// Exchange is empty for the ETFs: not yet verified.
var SweepUniverse = []SymbolInfo{
	// Country sensors (13).
	{Symbol: "RY", Name: "Royal Bank of Canada", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "ITUB", Name: "Itaú Unibanco", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "AMX", Name: "América Móvil", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "BCH", Name: "Banco de Chile", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "CIB", Name: "Bancolombia", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "BAP", Name: "Credicorp", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "LYG", Name: "Lloyds Banking Group", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "MUFG", Name: "Mitsubishi UFJ Financial Group", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "KB", Name: "KB Financial Group", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "HDB", Name: "HDFC Bank", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "TLK", Name: "Telkom Indonesia", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "TKC", Name: "Turkcell", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "ING", Name: "ING Groep", Type: AssetTypeStock, Exchange: "NYSE"},
	// Global sensors (5).
	{Symbol: "NVDA", Name: "NVIDIA", Type: AssetTypeStock, Exchange: "NASDAQ"},
	{Symbol: "AAPL", Name: "Apple", Type: AssetTypeStock, Exchange: "NASDAQ"},
	{Symbol: "XOM", Name: "Exxon Mobil", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "JPM", Name: "JPMorgan Chase", Type: AssetTypeStock, Exchange: "NYSE"},
	{Symbol: "CAT", Name: "Caterpillar", Type: AssetTypeStock, Exchange: "NYSE"},
	// Sector ETFs (10), daily close only.
	{Symbol: "IXN", Name: "iShares Global Tech ETF", Type: AssetTypeETF},
	{Symbol: "IXP", Name: "iShares Global Comm Services ETF", Type: AssetTypeETF},
	{Symbol: "RXI", Name: "iShares Global Consumer Discretionary ETF", Type: AssetTypeETF},
	{Symbol: "IXG", Name: "iShares Global Financials ETF", Type: AssetTypeETF},
	{Symbol: "IXJ", Name: "iShares Global Healthcare ETF", Type: AssetTypeETF},
	{Symbol: "IXC", Name: "iShares Global Energy ETF", Type: AssetTypeETF},
	{Symbol: "KXI", Name: "iShares Global Consumer Staples ETF", Type: AssetTypeETF},
	{Symbol: "EXI", Name: "iShares Global Industrials ETF", Type: AssetTypeETF},
	{Symbol: "MXI", Name: "iShares Global Materials ETF", Type: AssetTypeETF},
	{Symbol: "JXI", Name: "iShares Global Utilities ETF", Type: AssetTypeETF},
}

// SweepSymbolCodes returns SweepUniverse's tickers in order.
func SweepSymbolCodes() []string {
	codes := make([]string, len(SweepUniverse))
	for i, s := range SweepUniverse {
		codes[i] = s.Symbol
	}
	return codes
}
