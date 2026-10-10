-- Phase 8: the daily-bar sweep's 28 real symbols (portfolio.md "The symbol
-- universe"), mirrored from internal/source/universe.go (a test keeps the two
-- in step). tracked = false keeps them out of GET /symbols, /market and the
-- home band until Phase 14 promotes the adapter and flips the flag.
INSERT INTO symbols (symbol, name, type, exchange, tracked) VALUES
    ('RY', 'Royal Bank of Canada', 'stock', 'NYSE', false),
    ('ITUB', 'Itaú Unibanco', 'stock', 'NYSE', false),
    ('AMX', 'América Móvil', 'stock', 'NYSE', false),
    ('BCH', 'Banco de Chile', 'stock', 'NYSE', false),
    ('CIB', 'Bancolombia', 'stock', 'NYSE', false),
    ('BAP', 'Credicorp', 'stock', 'NYSE', false),
    ('LYG', 'Lloyds Banking Group', 'stock', 'NYSE', false),
    ('MUFG', 'Mitsubishi UFJ Financial Group', 'stock', 'NYSE', false),
    ('KB', 'KB Financial Group', 'stock', 'NYSE', false),
    ('HDB', 'HDFC Bank', 'stock', 'NYSE', false),
    ('TLK', 'Telkom Indonesia', 'stock', 'NYSE', false),
    ('TKC', 'Turkcell', 'stock', 'NYSE', false),
    ('ING', 'ING Groep', 'stock', 'NYSE', false),
    ('NVDA', 'NVIDIA', 'stock', 'NASDAQ', false),
    ('AAPL', 'Apple', 'stock', 'NASDAQ', false),
    ('XOM', 'Exxon Mobil', 'stock', 'NYSE', false),
    ('JPM', 'JPMorgan Chase', 'stock', 'NYSE', false),
    ('CAT', 'Caterpillar', 'stock', 'NYSE', false),
    ('IXN', 'iShares Global Tech ETF', 'etf', NULL, false),
    ('IXP', 'iShares Global Comm Services ETF', 'etf', NULL, false),
    ('RXI', 'iShares Global Consumer Discretionary ETF', 'etf', NULL, false),
    ('IXG', 'iShares Global Financials ETF', 'etf', NULL, false),
    ('IXJ', 'iShares Global Healthcare ETF', 'etf', NULL, false),
    ('IXC', 'iShares Global Energy ETF', 'etf', NULL, false),
    ('KXI', 'iShares Global Consumer Staples ETF', 'etf', NULL, false),
    ('EXI', 'iShares Global Industrials ETF', 'etf', NULL, false),
    ('MXI', 'iShares Global Materials ETF', 'etf', NULL, false),
    ('JXI', 'iShares Global Utilities ETF', 'etf', NULL, false)
ON CONFLICT (symbol) DO NOTHING;
