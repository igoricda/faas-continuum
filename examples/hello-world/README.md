# Exemplo: função Hello World

Versão mínima do [`alpr`](../alpr) contra o contrato da ADR-0008
(`docs/ADR-0008-contrato-execucao-funcao.md`): sem dependências de
sistema, sem pesos de modelo, sem pacotes Python — serve para exercitar o
encadeamento inteiro (watchdog-lite + index.py + handle(), socket Unix,
build OCI) com build e cold start rápidos. A entrada é uma string crua,
então invocar é um `curl -d` de uma palavra.

## Contrato de Prontidão (ADR-0008, Emenda 2026-09-14, §3)

O `index.py` implementa **bind-first + lazy-load**:

- `HTTPServer` faz `bind` em `127.0.0.1:$PORT` **antes** de importar `function.handler`.
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

## Estrutura

```
handler.py -> function/handler.py   # handle(req): sem estado, retorna {"message": "Hello, <req>!"} (JSON)
index.py                            # servidor HTTP mínimo esperado pelo watchdog-lite
function/requirements.txt           # vazio — a linha de install fica no Dockerfile para receber deps
Dockerfile                          # imagem final, com watchdog-lite como processo principal
```

## Preparar

```bash
# a partir da raiz do repositório
GOOS=linux GOARCH=amd64 go build -o examples/hello-world/watchdog-lite ./cmd/watchdog-lite
# nas TVBoxes (ARM), usar GOARCH=arm64 no lugar
```

## Build e teste com Docker

```bash
cd examples/hello-world
docker build -t faas-continuum/hello-world:latest .
docker run --rm -d --name hello-world-test \
  -v /tmp/hello-world-sockets:/run/faas-continuum-socket \
  faas-continuum/hello-world:latest

curl -s --unix-socket /tmp/hello-world-sockets/handle.sock http://localhost/ -d 'igor'
# -> {"message": "Hello, igor!"}

curl -s --unix-socket /tmp/hello-world-sockets/handle.sock http://localhost/ -d ''
# -> {"message": "Hello, world!"}  (corpo vazio vira "world")
```

A saída é JSON válido por obrigação de contrato: o agente valida a
resposta da função contra a ADR-0008 (retorno de `handle()` = campo
`result`) e responde **502 `INVALID_FUNCTION_OUTPUT`** a qualquer
invocação cuja saída não seja JSON — uma versão anterior desta função
devolvia texto cru (`Hello, igor!`) e produzia exatamente esse erro.

## O que já foi validado (sem Docker, mesmo caminho de código)

O caminho de código de dentro do contêiner (`watchdog-lite` +
`index.py` + `handler.py`, exatamente como o Dockerfile os empacota) foi
validado rodando como processos locais reais:

- `curl -d 'igor'` via socket Unix: **200 OK**, `{"message": "Hello,
  igor!"}`, na casa dos milissegundos;
- Corpo vazio (`curl -d ''`): **200 OK**, `{"message": "Hello,
  world!"}` — não derruba a conexão;
- Log do `watchdog-lite` confirma o contrato da ADR-0008: fprocess sobe
  **uma única vez** e fica residente entre invocações.

Falta validar com `docker build && docker run` de verdade (permissões do
`USER app` etc.) — ver a lição de metodologia registrada no README do
alpr: teste rápido sem Docker itera na lógica; o build completo é
obrigatório antes de considerar a função pronta.
