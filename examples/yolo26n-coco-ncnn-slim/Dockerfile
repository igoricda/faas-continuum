# Dockerfile da função YOLO26n COCO na borda slim (backend ncnn direto) —
# segue o contrato de execução de função definido na ADR-0008: o processo
# principal do contêiner é o watchdog-lite, que sobe FPROCESS uma única
# vez e faz proxy das invocações recebidas em um socket Unix (sem
# rede/CNI — ver docs/ADR-0008-contrato-execucao-funcao.md).
#
# Par slim de examples/yolo26n-coco: MESMOS pesos (yolo26n.pt exportado
# para ncnn em imgsz=640), mesma entrada e mesma saída JSON (imagem
# anotada em base64 + detecções), mas SEM torch/ultralytics — só ncnn +
# numpy + opencv-headless. O export ncnn já traz o decode no grafo
# (anchors/strides/sigmoid na cauda do model.ncnn.param), então o
# handler replica letterbox + NMS em numpy com paridade medida contra o
# handler ultralytics (ver README). Sem o elefante do torch, a imagem
# fica na casa das centenas de MB e o cold não paga o import do
# torch/ultralytics.
#
# Build (a partir da raiz do repositório, para ter acesso a cmd/watchdog-lite):
#   GOOS=linux GOARCH=arm64 go build -o examples/yolo26n-coco-ncnn-slim/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/arm64 \
#     -t faas-continuum/yolo26n-coco-ncnn-slim:latest \
#     -f examples/yolo26n-coco-ncnn-slim/Dockerfile examples/yolo26n-coco-ncnn-slim
#
# Build NATIVO na TVBox de preferência (cross via qemu é lento).

FROM --platform=linux/arm64 python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /home/app/

COPY function/requirements.txt function/requirements.txt
# Só 3 pacotes: ncnn + numpy + opencv-headless. Sem torch, sem
# ultralytics, sem tensorflow — o caminho quente é ncnn puro.
RUN uv pip install --no-cache --system --target=/home/app/python \
      -r function/requirements.txt \
  && find /home/app/python -name "__pycache__" -type d -prune -exec rm -rf {} + \
  && find /home/app/python -name "*.pyc" -delete \
  && rm -rf /tmp/* /root/.cache

FROM --platform=linux/arm64 python:3.11-slim

# Pacotes de sistema exigidos pelo OpenCV headless — mesmo conjunto do
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

# Só os pacotes já instalados e limpos no builder — sem uv, sem caches.
COPY --from=builder /home/app/python /home/app/python

COPY index.py .
COPY function/ function/

# Detector ncnn exportado em imgsz=640 (model.ncnn.bin + model.ncnn.param
# + metadata.yaml) — o metadata é a procedência do export (task, imgsz,
# nomes das 80 classes); o handler em si lê os nomes de coco80.names,
# porque sem pyyaml não dá para parsear o metadata.yaml.
COPY yolo26n_ncnn_model/ yolo26n_ncnn_model/
COPY coco80.names .

# IMPORTANTE: todo diretório em que o processo da função precisa
# escrever depois de USER app precisa estar nesta lista. Sem ultralytics
# aqui, não existe /tmp/Ultralytics (o YOLO_CONFIG_DIR é do par .pt).
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PATH=$PATH:/home/app/python/bin/
ENV PYTHONPATH=/home/app/python:/home/app
ENV MODEL_PATH=/home/app/yolo26n_ncnn_model
ENV NAMES_PATH=/home/app/coco80.names
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
# N_THREADS=0 = default do ncnn (que já abre mais de uma thread na
# TVBox); >0 fixa. MODELS.md §2 validou threads=4 no ARM.
ENV N_THREADS=0

# Contrato da ADR-0008: FPROCESS é o comando que o watchdog-lite sobe
# uma única vez; UPSTREAM_PORT é o loopback interno entre watchdog-lite
# e este processo; SOCKET_PATH é onde o agente monta o socket Unix de
# invocação via bind mount (o agente decide o valor real na hora de
# provisionar — este é só o padrão para execução standalone/manual).
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# Sem torch/ultralytics o cold import é ordens de grandeza menor que os
# 30-60s do par .pt — 60s dá margem folgada (recalibrar na medição no
# box; o index.py ainda roda um warmup antes de _ready).
ENV READY_TIMEOUT=60s

CMD ["watchdog-lite"]
