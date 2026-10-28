# Dockerfile da função ALPR (detecção de placa, backend ONNX Runtime) —
# par ONNX/faas-continuum do examples/alpr-ncnn-slim (borda ARM) e da
# imagem slim do ACA em azure_functions/alpr: MESMO handler e
# pós-processamento (paridade já validada contra o handler torch: mesmo
# N/classe/rank, conf ±0.002, OBB ±1px em 5 imagens) e MESMO export
# `best.onnx` (imgsz=1280) — sem torch/ultralytics no cold.
#
# Alvo: nós x86 (névoa .94), onde o ORT ganha do ncnn (no ARM a ordem
# inverte — ver tabela em azure_functions/README.md). Contrato de execução
# ADR-0008 igual aos demais exemplos: watchdog-lite é o PID 1, sobe o
# FPROCESS e faz proxy reverso do socket Unix montado pelo agente.
#
# Build (a partir da raiz do repositório):
#   GOOS=linux GOARCH=amd64 go build -o examples/alpr-onnx-slim/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/amd64 \
#     -t igoricda/alpr-onnx-slim:latest \
#     -f examples/alpr-onnx-slim/Dockerfile examples/alpr-onnx-slim
#
# O binário do watchdog-lite é compilado FORA do Docker (ADR-0001) para a
# arquitetura do nó: amd64 para a névoa (.94), arm64 para a borda (TVBox).

FROM --platform=linux/amd64 python:3.12-slim AS builder

COPY function/requirements.txt /requirements.txt
# Mesmos pins do requirements-slim do ACA (onnxruntime 1.30.0, OpenCV
# headless 4.10.0.84, numpy 1.26.4): é o trio byte-idêntico ao host
# Functions, validado na paridade do handler.
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
COPY best.onnx .

# IMPORTANTE: todo diretório em que o processo da função precisa escrever
# depois de USER app precisa estar nesta lista (lição do alpr: falhar
# aqui é silencioso até rodar como não-root).
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/site-packages:/home/app
ENV MODEL_PATH=/home/app/best.onnx
# Mesmos knobs do handler do ACA (e do par ncnn): imgsz=1280 é o shape
# estático congelado no grafo exportado.
ENV CONFIDENCE_THRESHOLD=0.25
ENV IOU_THRESHOLD=0.7
ENV MAX_DET=300
ENV IMAGE_SIZE=1280
# ORT_THREADS=0 = default do onnxruntime (>1 thread); >0 fixa. Numa névoa
# com várias instâncias, limitar evita oversubscription dos vCPU do nó.
ENV ORT_THREADS=0

# Contrato da ADR-0008 (iguais aos outros exemplos).
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# Cold = import do cv2/numpy + sessão ORT (best.onnx 10 MB) + warmup;
# sem torch, o import é ordens de grandeza menor (medir e recalibrar no
# nó-alvo; 60s dá margem folgada sem mascarar falha real de subida).
ENV READY_TIMEOUT=60s

CMD ["watchdog-lite"]
