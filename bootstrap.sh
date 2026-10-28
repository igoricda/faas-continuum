#!/usr/bin/env bash
#
# bootstrap.sh — Prepara o ambiente de desenvolvimento do faas-continuum.
#
# Este script NÃO gera código-fonte: ele assume que você já clonou o
# repositório (o código Go é versionado normalmente no git). O que ele
# faz é só a parte de ambiente/dependências:
#   1. Confere/instala Go 1.22+;
#   2. Resolve as dependências do módulo (go.mod já vem com "replace"
#      apontando para mirrors reais no GitHub das dependências
#      transitivas do containerd que usam import path "vanity"
#      (golang.org/x/..., google.golang.org/..., gopkg.in/...,
#      go.opentelemetry.io/...) — isso já deixa o build funcionando tanto
#      em redes normais quanto em redes com egress restrito, sem
#      precisar reescrever go.mod;
#   3. Ativa GOPROXY=direct como fallback SOMENTE se o proxy padrão do
#      Go estiver inacessível (detectado pelo erro), e só para este
#      processo — não mexe na configuração global da sua máquina;
#   4. Roda build, vet e testes;
#   5. Compila cmd/spike-psi e cmd/watchdog-lite para linux/arm64 (TVBoxes TG3/XP4).
#
# Uso (a partir da raiz do repositório já clonado):
#   ./bootstrap.sh
set -euo pipefail

log()  { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33m[bootstrap][aviso]\033[0m %s\n' "$1"; }
die()  { printf '\033[1;31m[bootstrap][erro]\033[0m %s\n' "$1" >&2; exit 1; }

# Em SSH não-interativo o PATH não herda o ~/.bashrc, então um Go já
# instalado em /usr/local/go (tvbox, devcontainer) deixa de ser encontrado
# e o passo 1 cairia no apt-get (Go antigo). Anexa o diretório padrão
# apenas se ainda não estiver no PATH.
case ":$PATH:" in
    *:/usr/local/go/bin:*) ;;
    *) export PATH="$PATH:/usr/local/go/bin" ;;
esac

[ -f go.mod ] || die "go.mod não encontrado no diretório atual. Rode este script a partir da raiz do repositório clonado (git clone https://github.com/igoricda/faas-continuum && cd faas-continuum)."

# ---------------------------------------------------------------------
# 1. Go 1.22+
# ---------------------------------------------------------------------
if ! command -v go >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
        log "Go não encontrado — instalando via apt-get (golang-go)..."
        sudo apt-get update -qq
        sudo apt-get install -y -qq golang-go
    else
        die "Go não encontrado e apt-get indisponível. Instale Go 1.22+ manualmente: https://go.dev/dl/"
    fi
fi
log "Usando $(go version | awk '{print $3}')"

# ---------------------------------------------------------------------
# 2/3. Dependências
# ---------------------------------------------------------------------
log "Resolvendo dependências (go mod download)..."
GET_LOG="$(mktemp)"

if go mod download >"$GET_LOG" 2>&1; then
    log "Dependências resolvidas (proxy padrão do Go)."
elif grep -qiE "not in allowlist|unrecognized import path|dial tcp.*i/o timeout|no such host" "$GET_LOG"; then
    warn "Proxy padrão do Go inacessível — tentando com GOPROXY=direct (só neste processo, sem alterar sua configuração global)..."
    export GOPROXY=direct
    export GOSUMDB=off
    if ! go mod download >"$GET_LOG" 2>&1; then
        cat "$GET_LOG" >&2
        die "Não foi possível resolver as dependências mesmo com GOPROXY=direct. Verifique se seu allowlist de rede libera: github.com, codeload.github.com. Veja o log acima."
    fi
    log "Dependências resolvidas (GOPROXY=direct)."
else
    cat "$GET_LOG" >&2
    die "Falha ao baixar dependências (go mod download). Veja o log acima."
fi

log "go mod tidy (best-effort)..."
# go mod tidy varre também caminhos condicionais de outras plataformas
# (ex.: o suporte a hcsshim/Windows do containerd), o que pode arrastar
# dependências do Google Cloud completamente irrelevantes para um build
# Linux/ARM. Por isso tratamos como best-effort: build/vet/test abaixo
# são a validação real e não dependem do tidy convergir 100%.
go mod tidy >/tmp/faas-continuum-tidy.log 2>&1 || warn "go mod tidy não convergiu totalmente (normal em redes restritas — build/test funcionam mesmo assim). Log: /tmp/faas-continuum-tidy.log"

# ---------------------------------------------------------------------
# 4. Build, vet, testes
# ---------------------------------------------------------------------
log "go build ./..."
go build ./...

log "go vet ./..."
go vet ./...

log "go test ./..."
go test ./...

# ---------------------------------------------------------------------
# 5. Compilação cruzada para as TVBoxes (ARM64) — ferramenta de spike
# ---------------------------------------------------------------------
mkdir -p bin
log "Compilando cmd/spike-psi para linux/arm64 (TVBoxes TG3/XP4)..."
GOOS=linux GOARCH=arm64 go build -o bin/spike-psi-arm64 ./cmd/spike-psi
log "Binário gerado em bin/spike-psi-arm64 — copie para a TVBox e rode conforme docs/spikes/psi-cgroups-tvbox.md"

log "Compilando cmd/watchdog-lite para linux/arm64 (roda dentro dos contêineres de função nas TVBoxes)..."
GOOS=linux GOARCH=arm64 go build -o bin/watchdog-lite-arm64 ./cmd/watchdog-lite
log "Binário gerado em bin/watchdog-lite-arm64 — copie para dentro da imagem OCI da função, conforme docs/ADR-0008-contrato-execucao-funcao.md"

log "Ambiente pronto."
