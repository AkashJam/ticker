# syntax=docker/dockerfile:1
#
# ticker (Go) image — portfolio.md §5: "one Dockerfile, two jobs": `dev`
# backs .devcontainer/devcontainer.json; `deps -> build -> runtime` (below)
# is what ticker/.github/workflows/ci.yml builds and pushes to ECR.
#
# Base: official Go 1.26 (current stable) on Debian trixie for `dev`/`deps`/
# `build` (non-Alpine for cgo/native compat, even though this binary itself
# is CGO_ENABLED=0 — matches `portfolio`'s Dockerfile's own base choice);
# distroless static for `runtime` — no shell, no package manager, smallest
# possible attack surface for a binary with zero C dependencies (pgx, gin,
# go-redis are all pure Go).

FROM golang:1.26-trixie AS dev

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      git \
      make \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Go dev tooling: language server + debugger (used by the VS Code Go extension).
RUN go install golang.org/x/tools/gopls@latest \
 && go install github.com/go-delve/delve/cmd/dlv@latest

WORKDIR /workspace

# Ticker REST + SSE API
EXPOSE 8080

# The devcontainer keeps the container alive and drives it interactively;
# `make run` / `go run ./cmd serve` are run from inside once the service exists.
CMD ["sleep", "infinity"]

FROM golang:1.26-trixie AS deps
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download

FROM deps AS build
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/ticker ./cmd

# distroless/static has no shell — migrations run via the `ticker migrate`
# subcommand as its own one-shot Compose service (infra/docker-compose.yml),
# not a shell script, and there's no Docker HEALTHCHECK here for the same
# reason (no shell to run a check command in); GET /ready is checked
# externally by the deploy pipeline instead.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=build /out/ticker /usr/local/bin/ticker
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ticker"]
CMD ["serve"]
