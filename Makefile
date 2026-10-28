.PHONY: build test vet tidy spike-psi-arm64 build-arm64 build-arm64-raw build-agente-arm64 build-watchdog-lite-arm64 build-arbitro-arm64 build-agente-arm64-raw build-watchdog-lite-arm64-raw build-arbitro-arm64-raw

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

# Compila o binário de diagnóstico do spike de PSI para as TVBoxes
# (ajustar GOARCH se o dispositivo real usar armv7 em vez de arm64).
spike-psi-arm64:
	GOOS=linux GOARCH=arm64 go build -o bin/spike-psi-arm64 ./cmd/spike-psi

# Stripped arm64 release artifacts (no spikes — release is agente + watchdog-lite + arbitro only).
# Spikes (cmd/spike-*) remain in go build ./... / go vet ./... / go test ./... by design;
# they are excluded from release artifacts simply by not being listed here.
# Lab debug build (go build -tags debug) is NOT stripped — keep pprof_debug.go intact.
build-arm64: build-agente-arm64 build-watchdog-lite-arm64 build-arbitro-arm64

build-agente-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o bin/agente-arm64 ./cmd/agente

build-watchdog-lite-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o bin/watchdog-lite-arm64 ./cmd/watchdog-lite

build-arbitro-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o bin/arbitro-arm64 ./cmd/arbitro

# Raw (unstripped) cross builds for size-comparison only (bin-sizes.txt raw column).
# Raw recipe: CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/<name>-arm64-raw ./cmd/<name>
build-arm64-raw: build-agente-arm64-raw build-watchdog-lite-arm64-raw build-arbitro-arm64-raw

build-agente-arm64-raw:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/agente-arm64-raw ./cmd/agente

build-watchdog-lite-arm64-raw:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/watchdog-lite-arm64-raw ./cmd/watchdog-lite

build-arbitro-arm64-raw:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/arbitro-arm64-raw ./cmd/arbitro
