package source

// SweepUniverse is the daily-bar sweep's 28 real symbols (portfolio.md "The
// symbol universe"): 13 country sensors, 5 global sensors and 10 sector ETFs.
// It is deliberately separate from Symbols, which stays the simulator's
// SIM: list — a sim walk under a real ticker would be fabrication with a
// real name on it, so the two lists are disjoint by construction.
//
// The sweep reads this list; the symbols table carries the same rows
// (migration 000003), so their pages and daily bars are served. GET /sensors
// lists the sensors (phase14.md decision 2). The first
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

// StreamUniverse is the symbols the Finnhub WebSocket streams: the 18 stocks
// (13 country sensors and 5 global), not the ten sector ETFs, which are
// daily-close only (portfolio.md §15 Phase 13). Eighteen sits well under
// Finnhub's 50-symbol WebSocket cap.
func StreamUniverse() []SymbolInfo {
	var out []SymbolInfo
	for _, s := range SweepUniverse {
		if s.Type == AssetTypeStock {
			out = append(out, s)
		}
	}
	return out
}

// StreamSymbolCodes returns StreamUniverse's tickers in order.
func StreamSymbolCodes() []string {
	u := StreamUniverse()
	codes := make([]string, len(u))
	for i, s := range u {
		codes[i] = s.Symbol
	}
	return codes
}
