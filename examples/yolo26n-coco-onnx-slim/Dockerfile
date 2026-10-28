# Dockerfile da função YOLO26n COCO na névoa com backend ONNX Runtime —
# terceiro membro da família YOLO26n: examples/yolo26n-coco (torch CPU,
# servidor), examples/yolo26n-coco-ncnn-slim (ncnn, borda ARM) e este
# (onnxruntime, névoa x86).
#
# Composição: MESMO handler/pós-processamento de azure_functions/yolo26n-coco
# (port 1:1 do par ncnn-slim, paridade validada contra torch/ncnn — ver o
# README de lá) + o contrato de execução ADR-0008 dos exemplos
# (watchdog-lite como PID 1, FPROCESS/UPSTREAM_PORT/SOCKET_PATH).
# Sem torch/ultralytics no cold: só onnxruntime + numpy + opencv-headless.
#
# Build (a partir da raiz do repositório):
#   GOOS=linux GOARCH=amd64 go build -o examples/yolo26n-coco-onnx-slim/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/amd64 \
#     -t igoricda/yolo26n-coco-onnx-slim:latest \
#     -f examples/yolo26n-coco-onnx-slim/Dockerfile examples/yolo26n-coco-onnx-slim
#
# O binário do watchdog-lite é compilado FORA do Docker (ADR-0001) para a
# arquitetura do nó: amd64 para a névoa (.94), arm64 para a borda (TVBox).

FROM --platform=linux/amd64 python:3.12-slim AS builder

COPY function/requirements.txt /requirements.txt
# Mesmos pins do requirements do host Functions em
# azure_functions/yolo26n-coco/requirements-slim.txt (onnxruntime 1.30.0,
# OpenCV headless 4.10.0.84, numpy 1.26.4) — o OpenCV 4.10 é o que garante
# o JPEG anotado byte-idêntico ao validado.
RUN pip install --no-cache-dir --target=/opt/site-packages -r /requirements.txt \
  && find /opt/site-packages -name "__pycache__" -type d -prune -exec rm -rf {} + \
  && find /opt/site-packages -name "*.pyc" -delete \
  && rm -rf /tmp/* /root/.cache

FROM --platform=linux/amd64 python:3.12-slim

# Pacotes de sistema exigidos pelo OpenCV headless (glib + libGL; headless
# não pede X11) — mesmo conjunto da imagem slim do ACA.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd app && useradd -r -g app app

WORKDIR /home/app/

COPY watchdog-lite /usr/bin/watchdog-lite
RUN chmod +x /usr/bin/watchdog-lite

# Só os pacotes já instalados e limpos no builder — sem caches.
COPY --from=builder /opt/site-packages /opt/site-packages

COPY index.py .
COPY function/ function/
COPY yolo26n.onnx .
COPY coco80.names .

# IMPORTANTE: todo diretório em que o processo da função precisa escrever
# depois de USER app precisa estar nesta lista (lição do alpr: falhar
# aqui é silencioso até rodar como não-root).
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/site-packages:/home/app
ENV MODEL_PATH=/home/app/yolo26n.onnx
ENV NAMES_PATH=/home/app/coco80.names
# Mesmos knobs dos outros dois exemplos e do handler do ACA.
ENV CONFIDENCE_THRESHOLD=0.25
ENV IOU_THRESHOLD=0.7
ENV MAX_DET=300
ENV IMAGE_SIZE=640
# ANNOTATE=0 devolve só as detecções; ANNOTATE_MAX_SIDE>0 reduz a imagem
# anotada (e as coordenadas) para caber no teto de saída do nó (8 MiB).
ENV ANNOTATE=1
ENV JPEG_QUALITY=85
ENV ANNOTATE_MAX_SIDE=0
# ORT_THREADS=0 = default do onnxruntime (>1 thread); >0 fixa. Numa névoa
# com várias instâncias, limitar evita oversubscription dos 16 vCPU do .94.
ENV ORT_THREADS=0

# Contrato da ADR-0008: FPROCESS é o comando que o watchdog-lite sobe uma
# única vez; UPSTREAM_PORT é o loopback interno entre watchdog-lite e este
# processo; SOCKET_PATH é onde o agente monta o socket Unix de invocação
# via bind mount (o agente decide o valor real no provisionamento — este
# é só o padrão standalone/manual).
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# Cold = import do cv2/numpy + criação da sessão ORT + warmup; sem torch,
# o import é ordens de grandeza menor (medir e recalibrar no nó-alvo;
# 60s dá margem folgada sem mascarar falha real de subida).
ENV READY_TIMEOUT=60s

CMD ["watchdog-lite"]
