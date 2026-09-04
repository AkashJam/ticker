# ticker

[![CI](https://github.com/AkashJam/ticker/actions/workflows/ci.yml/badge.svg)](https://github.com/AkashJam/ticker/actions/workflows/ci.yml)

A self-hosted, production-style market-data platform — one multiplexed feed,
Redis-coordinated ingestion, TimescaleDB candles, computed indicators, and a
live SSE dashboard.

`ticker` is the Go backend behind the live Market dashboard at
[akjames.dev/market](https://akjames.dev/market). It has no public HTTP route
of its own — [akjames.dev](https://akjames.dev) ([`portfolio`](https://github.com/AkashJam/portfolio))
is the only public ingress and reaches this service over a private Docker
network. See the [case study](https://akjames.dev/projects/market-ticker) for
the write-up, or [`infra`](https://github.com/AkashJam/infra) for how the two
are deployed together.

![Market dashboard](docs/screenshot.png)
<!-- TODO: capture https://akjames.dev/market and commit it as docs/screenshot.png -->

## Architecture

```mermaid
flowchart LR
  SIM["sim source<br/>internal/source"] -->|NormalizedTick| PROD["producer<br/>internal/ingest"]
  LOCK[["Redis SET NX PX<br/>lock:ingest-leader"]] -->|gates| PROD
  PROD -->|"XADD ticks:raw"| STREAM[("Redis Stream")]
  PROD -->|"PUBLISH quotes:sym"| PS[("Redis pub/sub")]
  STREAM -->|consumer group| AGG["aggregator<br/>internal/aggregate"]
  AGG -->|idempotent upsert| TS[("TimescaleDB<br/>candles")]
  AGG -->|candle close| INV["invalidate cache:ind:*"]
  AGG -->|"PUBLISH quotes:sym"| PS
  TS --> API["REST API (Gin)<br/>internal/api"]
  API --> INDENG["EMA / Wilder-RSI 14<br/>internal/indicators"]
  INDENG --> RCACHE[("Redis cache")]
  PS --> HUB["SSE hub<br/>internal/sse"]
  API --> PROXY["Next.js same-origin proxy"]
  HUB --> PROXY
  PROXY --> BROWSER["Browser"]
```

| Stage | Package | Does |
|---|---|---|
| Source | [`internal/source`](internal/source) | `MarketSource` interface; `sim` implementation emits ticks for 4 regime symbols (`SIM:NOVA` steady, `SIM:HELIX` high-vol, `SIM:ORBIT` mean-reverting, `SIM:PULSE` gappy) plus a 5-city cost-of-living series |
| Ingest | [`internal/ingest`](internal/ingest) | `Normalize` validates each tick; `Leader` holds a Redis `SET NX PX` lease on `lock:ingest-leader` so exactly one replica ingests; `Producer` (leader-gated) does `XADD ticks:raw` + `PUBLISH quotes:{symbol}`; `DeadMan` pings an optional dead-man-switch URL |
| Aggregate | [`internal/aggregate`](internal/aggregate) | Reads `ticks:raw` via a Redis Stream consumer group, maintains in-memory OHLCV candles for the configurable finest interval (`--agg-window`) plus a fixed `5m/15m/1h/1d` set, upserts idempotently into Timescale, invalidates the indicator cache and republishes on candle close |
| Indicators | [`internal/indicators`](internal/indicators) | EMA and Wilder-smoothed RSI(14), computed on read from candle history and cached in Redis (full series, not just the latest point) |
| Storage | [`internal/store`](internal/store) | Thin repositories over the shared Postgres pool and Redis client — `Timescale` (ticks/candles), `Meta` (symbols), `Redis` (stream/lock/pub-sub/cache primitives) — kept separate from the domain logic above so they're each swappable/mockable on their own |
| API + SSE | [`internal/api`](internal/api), [`internal/sse`](internal/sse) | Gin REST API; an SSE `Hub` relays the `quotes:{symbol}` pub/sub channel as `event: quote` / `event: candle` / `event: heartbeat`, capped at 200 connections (reject-new) |
| Wiring | [`server`](server), [`cmd`](cmd), [`flags`](flags) | `server.New` wires source → ingest → aggregate → API/SSE; on `SIGTERM` it stops the HTTP server, cancels and awaits every pipeline goroutine, flushes in-flight candles, then releases the leader lock — in that order, so nothing races the flush |

### The pipeline

- A pluggable `MarketSource` interface decouples the pipeline from where ticks
  actually come from. v1 runs a simulated source; a real feed adapter drops in
  later behind the same interface with no downstream changes.
- Ticks flow through a Redis Stream rather than an in-process channel, gated by
  a Redis `SET NX PX` leader lock. At today's scale — one instance — the lock
  is non-load-bearing, but it's built for the horizontal-scale case on
  purpose, not retrofitted later.
- The aggregator maintains OHLCV candles across multiple intervals at once and
  upserts them idempotently into TimescaleDB — required because Redis Streams
  only guarantee *at-least-once* delivery, so a redelivered tick must never
  produce a duplicate candle.
- EMA and Wilder-smoothed RSI(14) are computed on read and cached in Redis as a
  full series (not just the latest value), so the same cache entry serves both
  a live number and a chart line.
- The browser never talks to this API directly. A single Next.js route proxies
  the SSE stream same-origin, so there's no public backend, no CORS, and no
  auth surface to manage.
- On `SIGTERM`, the aggregator flushes every in-flight candle to Timescale and
  the ingestion loop releases its leader lock before the process exits — a
  deploy loses zero data, not "usually zero."

## Quickstart (`make dev`)

`ticker` needs a reachable Redis and TimescaleDB. Bring up throwaway ones with
`make services`, or point `.env` at your own:

```bash
cp .env.example .env      # defaults already match `make services` below
make services             # starts redis:7-alpine + timescaledb, published on :6379 / :5432
make migrate              # applies the schema (go run ./cmd migrate)
make dev                  # go run ./cmd serve --source sim --agg-window 10s
```

Verify it's alive:

```bash
curl localhost:8080/ready
curl -N 'localhost:8080/stream?symbols=SIM:NOVA'   # Ctrl-C to stop
```

Tear down the throwaway services with `make services-down`.

> **Inside the devcontainer:** the devcontainer and `make services`'s
> containers are sibling containers, not nested — `localhost` from inside the
> devcontainer doesn't reach their published ports. Point `TIMESCALE_DSN` /
> `REDIS_ADDR` in `.env` at `host.docker.internal` instead.

### All Makefile targets

| Target | Runs |
|---|---|
| `make dev` | `go run ./cmd serve --source sim --agg-window 10s` — fast local feedback |
| `make run` | `go run ./cmd serve --source sim --agg-window 1m` — production-shaped interval |
| `make run-finnhub` | `go run ./cmd serve --source finnhub` — not implemented yet, kept as a placeholder |
| `make migrate` | `go run ./cmd migrate` — applies pending schema migrations, then exits |
| `make services` | starts `redis:7-alpine` + `timescale/timescaledb:2.20.3-pg16` for local dev |
| `make services-down` | stops and removes them |
| `make test` | `go test ./... -v` |
| `make build` | `go build -o bin/ticker ./cmd` |
| `make lint` | `golangci-lint run` (v2.x — see `.golangci.yml`) |

## Configuration

Every setting is a CLI flag, an environment variable, or both — precedence is
**flag > env / `.env` > built-in default**.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--source` | `SOURCE` | `sim` | `sim` \| `finnhub` (`finnhub` is unimplemented — fails fast at startup) |
| `--env` | `ENV` | `dev` | `dev` \| `prod` — informational only, shapes log formatting |
| `--addr` | `ADDR` | `:8080` | HTTP listen address |
| `--agg-window` | `AGG_WINDOW` | `1m` | Finest candle interval (`10s` locally, `1m` in production); `5m/15m/1h/1d` are always computed alongside it |
| `--log-level` | `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error` |
| `--db-dsn` | `TIMESCALE_DSN` | — (required) | `postgres://user:pass@host:5432/dbname?sslmode=disable` |
| `--redis-addr` | `REDIS_ADDR` | — (required) | `host:6379` |
| `--healthchecks-url` | `HEALTHCHECKS_URL` | — (optional) | A [healthchecks.io](https://healthchecks.io) URL for the ingestion loop's dead-man switch; empty disables it |

## HTTP API

Internal-only — reached exclusively by the `portfolio` container over the
Docker network (no auth, no CORS; that's fine because nothing else can reach
it). See [`internal/api/router.go`](internal/api/router.go).

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthy` | Liveness — is the process up |
| GET | `/ready` | Readiness — pings Timescale and Redis |
| GET | `/metrics` | Prometheus exposition |
| GET | `/symbols` | List tracked symbols |
| GET | `/symbols/:symbol` | Latest snapshot (price, change, day range) |
| GET | `/symbols/:symbol/candles` | OHLCV history — `?interval=1m\|5m\|15m\|1h\|1d&from=&to=` |
| GET | `/symbols/:symbol/indicators` | EMA/RSI — `?set=ema,rsi&interval=1h&series=true` |
| GET | `/market/movers` | Top-5 gainers and losers |
| GET | `/col` | Simulated cost-of-living family (5 cities) |
| GET | `/col/:city` | One city's detail, with a basket breakdown |
| GET | `/stream` | SSE — `?symbols=SIM:NOVA,SIM:HELIX`, emits `quote` / `candle` / `heartbeat` |

## Project layout

```text
cmd/                   entry point — serve | migrate | version
server/                wires source → ingest → aggregate → API/SSE; owns startup + shutdown
flags/                 typed Config, loaded from CLI flags / env / .env
internal/source/       MarketSource interface, sim feed, cost-of-living data
internal/ingest/       normalize, leader election, producer, dead-man switch
internal/aggregate/    Redis Stream consumer → OHLCV candles → Timescale
internal/indicators/   EMA, Wilder RSI(14), Redis-backed cache
internal/store/        Timescale/Meta/Redis repositories (thin, mockable)
internal/api/          Gin handlers + router
internal/sse/          SSE hub (Redis pub/sub → event stream)
migrations/            embedded SQL, run via `ticker migrate` (golang-migrate)
```

## Build & deploy

The `Dockerfile` is one file with four targets: `dev` (backs the devcontainer),
`deps` → `build` → `runtime`. `runtime` is a fully static, `CGO_ENABLED=0`
binary on `gcr.io/distroless/static-debian12:nonroot` — no shell, so
migrations run as their own `ticker migrate` one-shot rather than a script,
and there's no `HEALTHCHECK`; `GET /ready` is polled externally instead.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) lints, vets, and
tests on every push and PR; on `main` it builds a `linux/arm64` image, pushes
it to ECR, and fires a `repository_dispatch` that triggers
[`infra`](https://github.com/AkashJam/infra)'s deploy workflow. Runtime
placement (one EC2 box, alongside `portfolio`/Redis/Timescale/Caddy) is owned
entirely by `infra` — this repo only ever builds and pushes an image.

## Tests

```bash
make test
```

~27 unit tests across `internal/aggregate`, `internal/indicators`, and
`internal/ingest`, all against fakes — no real Redis or Postgres required to
run them.
