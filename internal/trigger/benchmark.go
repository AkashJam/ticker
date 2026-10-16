package trigger

// Benchmarks maps each sensor to the sector ETF its move is measured against
// (bearing.md #110, measured over 126 sessions). AAPL is absent on purpose:
// its residual against IXN was 1.5x noisier than its raw return (beta 0.02),
// so its residual is its raw return and the event says why.
var Benchmarks = map[string]string{
	"RY": "IXG", "ITUB": "IXG", "BCH": "IXG", "CIB": "IXG", "BAP": "IXG",
	"LYG": "IXG", "MUFG": "IXG", "KB": "IXG", "HDB": "IXG", "ING": "IXG",
	"JPM":  "IXG",
	"XOM":  "IXC",
	"NVDA": "IXN",
	"CAT":  "EXI",
	"TKC":  "IXP", "TLK": "IXP", "AMX": "IXP",
}

// NoBenchmarkReason is why a sensor without a benchmark has none.
var NoBenchmarkReason = map[string]string{
	"AAPL": "against IXN its residual was 1.5x noisier than its raw return (beta 0.02), so the raw return is used",
}
