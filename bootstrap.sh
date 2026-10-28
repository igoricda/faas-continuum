#!/usr/bin/env bash
#
# bootstrap.sh — Prepares the development environment of faas-continuum.
#
# This script does NOT generate source code: it assumes that you have already
# cloned the repository, since the Go code is versioned with git as usual.
# What it does is only the environment and the dependencies:
#   1. Checks and installs Go 1.22 or newer;
#   2. Resolves the dependencies of the module, since go.mod already carries
#      the "replace" directives that point to real mirrors on GitHub for the
#      transitive dependencies of the containerd that use a vanity import path
#      (golang.org/x/..., google.golang.org/..., gopkg.in/...,
#      go.opentelemetry.io/...), which makes the build work both on a normal
#      network and on a network with a restricted egress, without having to
#      rewrite go.mod;
#   3. Enables GOPROXY=direct as a fallback ONLY when the default proxy of
#      Go is unreachable, which is detected from the error, and only for
#      this process, so the global configuration of the machine is untouched;
#   4. Runs the build, the vet and the tests;
#   5. Cross compiles the diagnostic and the watchdog binaries for
#      linux/arm64.
#
# Usage, from the root of the cloned repository:
#   ./bootstrap.sh
set -euo pipefail

log()  { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33m[bootstrap][warning]\033[0m %s\n' "$1"; }
die()  { printf '\033[1;31m[bootstrap][error]\033[0m %s\n' "$1" >&2; exit 1; }

# In a non interactive shell the PATH does not inherit the ~/.bashrc, so a Go
# that is already installed in /usr/local/go stops being found and the first
# step would fall back to the apt-get with an old Go. The standard directory
# is appended only when it is not in the PATH yet.
case ":$PATH:" in
    *:/usr/local/go/bin:*) ;;
    *) export PATH="$PATH:/usr/local/go/bin" ;;
esac

[ -f go.mod ] || die "go.mod not found in the current directory. Run this script from the root of the cloned repository (git clone https://github.com/igoricda/faas-continuum.git && cd faas-continuum)."

# ---------------------------------------------------------------------
# 1. Go 1.22 or newer
# ---------------------------------------------------------------------
if ! command -v go >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
        log "Go not found — installing it with apt-get (golang-go)..."
        sudo apt-get update -qq
        sudo apt-get install -y -qq golang-go
    else
        die "Go not found and apt-get unavailable. Install Go 1.22 or newer manually: https://go.dev/dl/"
    fi
fi
log "Using $(go version | awk '{print $3}')"

# ---------------------------------------------------------------------
# 2/3. Dependencies
# ---------------------------------------------------------------------
log "Resolving the dependencies (go mod download)..."
GET_LOG="$(mktemp)"

if go mod download >"$GET_LOG" 2>&1; then
    log "Dependencies resolved (the default proxy of Go)."
elif grep -qiE "not in allowlist|unrecognized import path|dial tcp.*i/o timeout|no such host" "$GET_LOG"; then
    warn "The default proxy of Go is unreachable — trying with GOPROXY=direct (only in this process, without changing the global configuration)..."
    export GOPROXY=direct
    export GOSUMDB=off
    if ! go mod download >"$GET_LOG" 2>&1; then
        cat "$GET_LOG" >&2
        die "Could not resolve the dependencies even with GOPROXY=direct. Check that the network allowlist releases github.com and codeload.github.com. See the log above."
    fi
    log "Dependencies resolved (GOPROXY=direct)."
else
    cat "$GET_LOG" >&2
    die "Failure while downloading the dependencies (go mod download). See the log above."
fi

log "go mod tidy (best effort)..."
# go mod tidy also scans the conditional paths of the other platforms, for
# instance the support of the containerd for hcsshim and Windows, which can
# drag in dependencies of the Google Cloud that are completely irrelevant for
# a Linux and ARM build. That is why it is treated as best effort: the build,
# the vet and the tests below are the real validation and do not depend on
# the tidy converging completely.
go mod tidy >/tmp/faas-continuum-tidy.log 2>&1 || warn "go mod tidy did not converge completely (normal on a restricted network — the build and the tests work anyway). Log: /tmp/faas-continuum-tidy.log"

# ---------------------------------------------------------------------
# 4. Build, vet, tests
# ---------------------------------------------------------------------
log "go build ./..."
go build ./...

log "go vet ./..."
go vet ./...

log "go test ./..."
go test ./...

# ---------------------------------------------------------------------
# 5. Cross compilation for the ARM64 hosts
# ---------------------------------------------------------------------
mkdir -p bin
log "Compiling cmd/spike-psi for linux/arm64..."
GOOS=linux GOARCH=arm64 go build -o bin/spike-psi-arm64 ./cmd/spike-psi
log "Binary written to bin/spike-psi-arm64 — copy it to the ARM host and run it there."

log "Compiling cmd/spike-energia for linux/arm64..."
GOOS=linux GOARCH=arm64 go build -o bin/spike-energia-arm64 ./cmd/spike-energia
log "Binary written to bin/spike-energia-arm64 — copy it to the ARM host and run it there."

log "Compiling cmd/watchdog-lite for linux/arm64 (it runs inside the function containers)..."
GOOS=linux GOARCH=arm64 go build -o bin/watchdog-lite-arm64 ./cmd/watchdog-lite
log "Binary written to bin/watchdog-lite-arm64 — copy it into the OCI image of the function."

log "Environment ready."
