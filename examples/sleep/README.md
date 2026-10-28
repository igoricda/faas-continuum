# Exemplo: função Sleep

Função cujo workload é o próprio tempo de espera: recebe um número de
segundos, dorme esse tempo e responde `slept for X seconds`. Serve para
exercitar timeouts, SLO (`max_latency_ms`) e escalonamento sob cargas de
duração controlada, sem depender de modelo ou I/O (contrato da ADR-0008,
`docs/ADR-0008-contrato-execucao-funcao.md`).

## Contrato de Prontidão (ADR-0008, Emenda 2026-09-14, §3)

O `index.py` implementa **bind-first + lazy-load** (mesmo padrão do hello-world/alpr):

- `HTTPServer` faz `bind` em `127.0.0.1:$PORT` **antes** de qualquer trabalho pesado.
- Thread daemon carrega `handle()` e sinaliza `threading.Event _ready`.
- **GET `/_/ready`**: `200 {"ready": true}` se pronto, `503 {"ready": false}` caso contrário.
- **POST `/_/ready`**: **sempre `404`**.
- **POST `/`**: `503` enquanto carrega, `200` + JSON válido após `_ready`.
- **GET desconhecido**: `404`.

Variáveis de ambiente do **watchdog-lite** (injetadas via Dockerfile ENV ou defaults de código):

- `READY_PATH` (default `/_/ready`) — path sondado.
- `READY_TIMEOUT` (default `90s`) — teto do poll; **sintaxe Go duration obrigatória** (`90s`, `500ms`, `15m`; nunca valor nu `60`).
- `READY_INTERVAL` (default `250ms`) — intervalo entre sondagens.

O `watchdog-lite` **não abre o socket Unix** até receber `200` em `GET /_/ready` (socket gating, Emenda §1). Dial recusado ou status ≠ 200 mantêm o poll; apenas 200 prossegue.

## Contrato de entrada/saída

Segue o padrão `status`/`message` do [`alpr`](../alpr); a saída
é JSON válido por obrigação de contrato (o agente responde **502
`INVALID_FUNCTION_OUTPUT`** a qualquer saída que não seja JSON):

```
entrada:  número cru no corpo ("3") ou JSON {"seconds": 3}
saída:    {"status": "success", "message": "slept for 3 seconds"}
       ou {"status": "error", "message": "entrada inválida: ..."}
```

## Estrutura

```
handler.py -> function/handler.py   # handle(req): dorme N segundos, retorna {"status": "success", "message": "slept for X seconds"} (JSON)
index.py                            # servidor HTTP mínimo esperado pelo watchdog-lite
function/requirements.txt           # vazio — a linha de install fica no Dockerfile para receber deps
Dockerfile                          # imagem final, com watchdog-lite como processo principal
```

## Preparar

```bash
# a partir da raiz do repositório
GOOS=linux GOARCH=amd64 go build -o examples/sleep/watchdog-lite ./cmd/watchdog-lite
# nas TVBoxes (ARM), usar GOARCH=arm64 no lugar
```

## Build e teste com Docker

```bash
cd examples/sleep
docker build -t igoricda/sleep:latest .

# O bind mount sobrescreve o chown do Dockerfile: o diretório do host
# precisa pertencer ao uid do usuário app do contêiner (999:1000).
mkdir -p /tmp/sleep-sockets && sudo chown 999:1000 /tmp/sleep-sockets

docker run --rm -d --name sleep-test \
  -v /tmp/sleep-sockets:/run/faas-continuum-socket \
  igoricda/sleep:latest

# O watchdog-lite cria handle.sock com modo 0755 (dono app): um cliente
# não-root no host não conecta — usar sudo para o teste local.
sudo curl -s --unix-socket /tmp/sleep-sockets/handle.sock http://localhost/ -d '2'
# -> {"status": "success", "message": "slept for 2 seconds"}  (levou ~2s reais)

sudo curl -s --unix-socket /tmp/sleep-sockets/handle.sock http://localhost/ -d '{"seconds": 1}'
# -> {"status": "success", "message": "slept for 1 seconds"}

sudo curl -s --unix-socket /tmp/sleep-sockets/handle.sock http://localhost/ -d 'abc'
# -> {"status": "error", "message": "entrada inválida: ..."}

sudo curl -s --unix-socket /tmp/sleep-sockets/handle.sock http://localhost/ -d '-1'
# -> {"status": "error", "message": "entrada inválida: segundos não pode ser negativo"}

docker rm -f sleep-test
```

No agente real essa permissão não é problema: quem provisiona o socket é
o próprio agente (mesma origem do bind mount e do processo).

## Publicar

```bash
docker push igoricda/sleep:latest
```
