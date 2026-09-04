.PHONY: run dev run-finnhub migrate test build lint services services-down

# Production-shape run: 1m finest interval (portfolio.md §15).
run:
	go run ./cmd serve --source sim --agg-window 1m

# Local dev-shape run: 10s finest interval — faster feedback while testing.
dev:
	go run ./cmd serve --source sim --agg-window 10s

# Throwaway backing services for local dev — not part of any compose file,
# just enough to satisfy `make migrate`/`make dev`'s REDIS_ADDR/TIMESCALE_DSN.
# Ports match .env.example. See README's devcontainer note if these don't
# resolve at `localhost` from where you're running `make dev`.
services:
	docker run -d --name ticker-redis -p 6379:6379 redis:7-alpine
	docker run -d --name ticker-timescale -p 5432:5432 \
		-e POSTGRES_USER=ticker -e POSTGRES_PASSWORD=ticker -e POSTGRES_DB=ticker \
		timescale/timescaledb:2.20.3-pg16

services-down:
	docker rm -f ticker-redis ticker-timescale

# Fails today — Finnhub is unimplemented in v1 (§16.1). Kept so the target
# exists for when that adapter lands.
run-finnhub:
	go run ./cmd serve --source finnhub

migrate:
	go run ./cmd migrate

test:
	go test ./... -v

build:
	go build -o bin/ticker ./cmd

lint:
	golangci-lint run
