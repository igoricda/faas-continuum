# Dockerfile da função de detecção YOLO26n COCO (servidor amd64) — segue
# o contrato de execução de função definido na ADR-0008: o processo
# principal do contêiner é o watchdog-lite (não um shell nem o
# interpretador Python direto), que sobe FPROCESS uma única vez e faz
# proxy das invocações recebidas em um socket Unix (sem rede/CNI — ver
# docs/ADR-0008-contrato-execucao-funcao.md).
#
# Imagem amd64 com o modelo **default do ultralytics, pré-treinado no
# COCO** (yolo26n.pt, 5,3 MB, 80 classes) via ultralytics 8.4.153 + torch
# (CUDA via pip default) + numpy. É o par servidor do
# examples/yolo26n-coco-ncnn-slim (borda ARM): mesma entrada, mesma saída
# JSON (imagem anotada em base64 + detecções), só muda o backend.
#
# Build multi-stage (mesmo padrão do examples/alpr): o stage builder
# instala tudo em /home/app/python e remove matplotlib/polars (+ deps
# exclusivas deles), caches e pycache; o stage final copia SÓ os
# pacotes. matplotlib/polars são Requires-Dist do ultralytics, mas todos
# os imports deles no ultralytics são lazy (plotting, benchmarks,
# callbacks, trainer) — o caminho quente (import + inferência .pt +
# anotação com cv2) não os usa. O torch CUDA + nvidia-* ficam (são o
# runtime da GPU).
#
# Build (a partir da raiz do repositório, para ter acesso a cmd/watchdog-lite):
#   GOOS=linux GOARCH=amd64 go build -o examples/yolo26n-coco/watchdog-lite ./cmd/watchdog-lite
#   docker build --platform linux/amd64 -t faas-continuum/yolo26n-coco:latest \
#     -f examples/yolo26n-coco/Dockerfile examples/yolo26n-coco
#
# Variante sem GPU (torch +cpu, imagem bem menor): Dockerfile.cpu.

FROM --platform=linux/amd64 python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /home/app/

COPY function/requirements.txt function/requirements.txt
# ultralytics puxa o torch com CUDA (wheels do PyPI default) — em host
# sem GPU o mesmo torch roda em CPU; com GPU, subir com --gpus all.
RUN uv pip install --no-cache --system --target=/home/app/python -r function/requirements.txt \
  && rm -rf /home/app/python/matplotlib /home/app/python/mpl_toolkits /home/app/python/pylab.py \
      /home/app/python/polars /home/app/python/_polars_runtime_32 \
      /home/app/python/contourpy* /home/app/python/cycler* /home/app/python/fontTools \
      /home/app/python/kiwisolver* \
      /home/app/python/matplotlib-*.dist-info /home/app/python/polars-*.dist-info \
      /home/app/python/polars_runtime_32-*.dist-info /home/app/python/contourpy-*.dist-info \
      /home/app/python/cycler-*.dist-info /home/app/python/fonttools-*.dist-info \
      /home/app/python/kiwisolver-*.dist-info \
  && find /home/app/python -name "__pycache__" -type d -prune -exec rm -rf {} + \
  && find /home/app/python -name "*.pyc" -delete \
  && rm -rf /tmp/* /root/.cache

FROM --platform=linux/amd64 python:3.11-slim

# Pacotes de sistema exigidos pelo OpenCV (dependência do ultralytics,
# usada também pelo handler para decodificar/anotar) — mesmo conjunto do
# template OpenFaaS original.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd app && useradd -r -g app app

WORKDIR /home/app/

# O binário do watchdog-lite é compilado FORA do Docker (cross-compile
# Go trivial, ver ADR-0001) e só copiado aqui — não precisa de uma stage
# de build Go dentro da imagem da função.
COPY watchdog-lite /usr/bin/watchdog-lite
RUN chmod +x /usr/bin/watchdog-lite

# Só os pacotes já instalados e limpos no builder — sem uv, sem caches,
# sem matplotlib/polars.
COPY --from=builder /home/app/python /home/app/python

COPY index.py .
COPY function/ function/

# Pesos default do YOLO26n no COCO (ver "Preparar" no README).
COPY yolo26n.pt .

# IMPORTANTE: todo diretório em que o processo da função precisa
# escrever depois de USER app precisa estar nesta lista — inclui
# /tmp/Ultralytics (config do ultralytics, YOLO_CONFIG_DIR abaixo), não
# só /home/app e o socket. Faltar um item aqui é silencioso até rodar
# como usuário não-root (lição registrada no README do alpr).
RUN mkdir -p /run/faas-continuum-socket /tmp/Ultralytics && \
    chown -R app:app /home/app /run/faas-continuum-socket /tmp/Ultralytics

USER app

ENV PATH=$PATH:/home/app/python/bin/
ENV PYTHONPATH=/home/app/python:/home/app
ENV YOLO_CONFIG_DIR=/tmp/Ultralytics
ENV YOLO_VERBOSE=False
ENV MODEL_PATH=/home/app/yolo26n.pt
# Defaults do ultralytics (DEFAULT_CFG) — todos sobrescritíveis por env.
ENV CONFIDENCE_THRESHOLD=0.25
ENV IOU_THRESHOLD=0.7
ENV MAX_DET=300
ENV IMAGE_SIZE=640
# ANNOTATE=0 devolve só as detecções; ANNOTATE_MAX_SIDE>0 reduz a imagem
# anotada (e as coordenadas) para caber no teto de saída do nó (8 MiB).
ENV ANNOTATE=1
ENV JPEG_QUALITY=85
ENV ANNOTATE_MAX_SIDE=0

# Contrato da ADR-0008: FPROCESS é o comando que o watchdog-lite sobe
# uma única vez; UPSTREAM_PORT é o loopback interno entre watchdog-lite
# e este processo; SOCKET_PATH é onde o agente monta o socket Unix de
# invocação via bind mount (o agente decide o valor real na hora de
# provisionar — este é só o padrão para execução standalone/manual).
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# torch/ultralytics são conhecidos por levar 30-60s no primeiro import a
# frio (carregamento de bibliotecas nativas, sem relação com o tamanho
# do modelo em si). O padrão do watchdog-lite (30s) fica exatamente no
# limite desse pior caso — 90s dá margem confortável sem mascarar uma
# falha real de subida.
ENV READY_TIMEOUT=90s

CMD ["watchdog-lite"]
