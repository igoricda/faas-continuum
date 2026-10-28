.PHONY: build test vet tidy spike-psi-arm64 spike-energia-arm64 build-arm64 build-arm64-raw build-agente-arm64 build-watchdog-lite-arm64 build-arbitro-arm64 build-agente-arm64-raw build-watchdog-lite-arm64-raw build-arbitro-arm64-raw

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

# Cross builds of the diagnostics, for the ARM64 hosts. Adjust GOARCH when
# the target device uses armv7 instead of arm64.
spike-psi-arm64:
	GOOS=linux GOARCH=arm64 go build -o bin/spike-psi-arm64 ./cmd/spike-psi

spike-energia-arm64:
	GOOS=linux GOARCH=arm64 go build -o bin/spike-energia-arm64 ./cmd/spike-energia

# Stripped arm64 release artifacts: the node agent, the watchdog and the
# arbiter.
build-arm64: build-agente-arm64 build-watchdog-lite-arm64 build-arbitro-arm64

build-agente-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o bin/agente-arm64 ./cmd/agente

build-watchdog-lite-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o bin/watchdog-lite-arm64 ./cmd/watchdog-lite

build-arbitro-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o bin/arbitro-arm64 ./cmd/arbitro

# Raw, unstripped cross builds, for the comparison of sizes only.
build-arm64-raw: build-agente-arm64-raw build-watchdog-lite-arm64-raw build-arbitro-arm64-raw

build-agente-arm64-raw:
	GOOS=linux GOARCH=arm64 go build -o bin/agente-arm64-raw ./cmd/agente

build-watchdog-lite-arm64-raw:
	GOOS=linux GOARCH=arm64 go build -o bin/watchdog-lite-arm64-raw ./cmd/watchdog-lite

build-arbitro-arm64-raw:
	GOOS=linux GOARCH=arm64 go build -o bin/arbitro-arm64-raw ./cmd/arbitro
