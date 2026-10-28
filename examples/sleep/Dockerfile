# Dockerfile da função Sleep — segue o contrato de execução de função
# definido na ADR-0008: o processo principal do contêiner é o
# watchdog-lite (não um shell nem o interpretador Python direto), que sobe
# FPROCESS uma única vez e faz proxy das invocações recebidas em um socket
# Unix (sem rede/CNI — ver docs/ADR-0008-contrato-execucao-funcao.md).
#
# Como o hello-world: sem dependências de sistema, sem pesos de modelo,
# sem pacotes Python (requirements.txt vazio) — build e cold start
# rápidos. O workload é o próprio tempo de sleep declarado na entrada.
#
# Build (a partir da raiz do repositório, para ter acesso a cmd/watchdog-lite):
#   GOOS=linux GOARCH=amd64 go build -o examples/sleep/watchdog-lite ./cmd/watchdog-lite
#   docker build -t igoricda/sleep:latest -f examples/sleep/Dockerfile examples/sleep

FROM --platform=${TARGETPLATFORM:-linux/amd64} python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN groupadd app && useradd -r -g app app

WORKDIR /home/app/

# O binário do watchdog-lite é compilado FORA do Docker (cross-compile
# Go trivial, ver ADR-0001) e só copiado aqui — não precisa de uma stage
# de build Go dentro da imagem da função.
COPY watchdog-lite /usr/bin/watchdog-lite
RUN chmod +x /usr/bin/watchdog-lite

COPY index.py .
COPY function/requirements.txt function/requirements.txt
# requirements.txt está vazio hoje; a linha fica para o exemplo já sair
# pronto para receber dependências reais (como no alpr).
RUN uv pip install --no-cache --system --target=/home/app/python -r function/requirements.txt

COPY function/ function/

# Mesma lição dos outros exemplos: todo diretório em que o processo da
# função precisa escrever depois de USER app precisa estar nesta lista.
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PATH=$PATH:/home/app/python/bin/
ENV PYTHONPATH=/home/app/python:/home/app

# Contrato da ADR-0008: FPROCESS é o comando que o watchdog-lite sobe
# uma única vez; UPSTREAM_PORT é o loopback interno entre watchdog-lite
# e este processo; SOCKET_PATH é onde o agente monta o socket Unix de
# invocação via bind mount (o agente decide o valor real na hora de
# provisionar — este é só o padrão para execução standalone/manual).
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

CMD ["watchdog-lite"]
