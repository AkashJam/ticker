# syntax=docker/dockerfile:1
#
# ticker (Go) image — portfolio.md §5: "one Dockerfile, two jobs".
#
# For now only the `dev` target exists — it backs .devcontainer/devcontainer.json
# ("build.dockerfile: ../Dockerfile", "build.target: dev"). The production
# stages (deps -> build -> runtime) get added in the Days 5-9 backend phase, once
# go.mod and the service source exist. Sketch of what lands here later:
#
#   FROM golang:1.26-trixie AS deps      # go mod download
#   FROM golang:1.26-trixie AS build     # CGO_ENABLED=0 go build ./cmd
#   FROM gcr.io/distroless/static-debian12 AS runtime  # static binary, PORT 8080
#
# Base: official Go 1.26 (current stable) on Debian trixie (Debian 13, current
# stable) — matches the future build stage; non-Alpine for cgo/native compat.

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
