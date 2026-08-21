.PHONY: run dev run-finnhub migrate test build lint

# Production-shape run: 1m finest interval (portfolio.md §15).
run:
	go run ./cmd serve --source sim --agg-window 1m

# Local dev-shape run: 10s finest interval — faster feedback while testing.
dev:
	go run ./cmd serve --source sim --agg-window 10s

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
