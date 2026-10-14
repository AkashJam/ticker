-- Phase 14 step 0: remove the Yahoo backfill. Bounded on both sides so a swept
-- bar (from 2026-10-09) is never touched.
DELETE FROM candles
WHERE interval = '1d'
  AND time < '2026-10-09 00:00:00+00'
  AND symbol IN ('RY', 'ITUB', 'AMX', 'BCH', 'CIB', 'BAP', 'LYG', 'MUFG', 'KB', 'HDB', 'TLK', 'TKC', 'ING', 'NVDA', 'AAPL', 'XOM', 'JPM', 'CAT', 'IXN', 'IXP', 'RXI', 'IXG', 'IXJ', 'IXC', 'KXI', 'EXI', 'MXI', 'JXI');
