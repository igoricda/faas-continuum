# Dockerfile da função ALPR borda (detecção via ncnn) — segue o contrato
# de execução de função definido na ADR-0008: o processo principal do
# contêiner é o watchdog-lite (não um shell nem o interpretador Python
# direto), que sobe FPROCESS uma única vez e faz proxy das invocações
# recebidas em um socket Unix (sem rede/CNI — ver
# docs/ADR-0008-contrato-execucao-funcao.md).
#
# Imagem SOMENTE arm64 para as TVBoxes com o detector ncnn (MODELS.md
# §2 e §5, "Detector + OCR, edge (ARM, unified)"): ultralytics 8.4.153 +
# ncnn 1.0.20260526 + numpy<2, com torch +cpu (o torch do PyPI para
# aarch64 arrasta GBs de nvidia-* — ver MODELS.md §6). O export ncnn é
# em imgsz=640 (mAP50 0.995 / mAP50-95 0.984, infer ~222 ms em 4×
# Cortex-A55 — ~4× mais rápido que o export em 1280). É o par de borda
# do alpr (servidor): mesma entrada e mesma saída JSON, só muda o backend.
#
# Build multi-stage (mesmo padrão de azure_functions/): o stage builder
# instala tudo em /home/app/python e remove matplotlib/polars (+ deps
# exclusivas deles), caches e pycache; o stage final copia SÓ os
# pacotes. matplotlib/polars são Requires-Dist do ultralytics, mas todos
# os imports deles no ultralytics são lazy (plotting, benchmarks,
# callbacks, trainer) — o caminho quente (import + inferência ncnn) não
# os usa.
#
# Build (a partir da raiz do repositório, para ter acesso a cmd/watchdog-lite):
#   GOOS=linux GOARCH=arm64 go build -o examples/alpr-ncnn/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/arm64 \
#     -t faas-continuum/alpr-ncnn:latest -f examples/alpr-ncnn/Dockerfile examples/alpr-ncnn

FROM --platform=linux/arm64 python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /home/app/

COPY function/requirements.txt function/requirements.txt
# Ordem e comando único importam: torch +cpu PRIMEIRO, a partir do
# índice CPU do PyTorch — o torch do PyPI para aarch64 puxa GBs de
# pacotes nvidia-* (CUDA) inúteis na TVBox. Instalação em UM único
# comando: com --target o resolvedor não enxerga o diretório alvo como
# "já satisfeito", então um segundo install reinstalaria torch CUDA por
# cima do +cpu.
RUN uv pip install --no-cache --system --target=/home/app/python \
      --extra-index-url https://download.pytorch.org/whl/cpu \
      torch==2.14.0+cpu torchvision==0.29.0+cpu \
      -r function/requirements.txt \
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

FROM --platform=linux/arm64 python:3.11-slim

# Pacotes de sistema exigidos pelo OpenCV (usado internamente pelo
# ultralytics) — mesmo conjunto do template OpenFaaS original.
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

# Detector ncnn exportado em imgsz=640 (model.ncnn.bin + model.ncnn.param
# + metadata.yaml — sem o metadata o ultralytics não sabe que a task é
# obb nem o nome da classe e carrega 999 classes genéricas, zerando as
# detecções) — ver "Preparar" no README.
COPY best_ncnn_model/ best_ncnn_model/

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
ENV MODEL_PATH=/home/app/best_ncnn_model
ENV CONFIDENCE_THRESHOLD=0.25
ENV IMAGE_SIZE=640

# Contrato da ADR-0008: FPROCESS é o comando que o watchdog-lite sobe
# uma única vez; UPSTREAM_PORT é o loopback interno entre watchdog-lite
# e este processo; SOCKET_PATH é onde o agente monta o socket Unix de
# invocação via bind mount (o agente decide o valor real na hora de
# provisionar — este é só o padrão para execução standalone/manual).
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# O cold import do ultralytics/ncnn no ARM lento estoura fácil o padrão
# de 30s do watchdog-lite — 90s dá margem sem mascarar falha real.
# Na TVBox, a inferência ncnn em 640 leva ~270 ms/imagem (MODELS.md §2),
# então o timeout de registro no agente precisa cobrir isso com folga
# (ver README).
ENV READY_TIMEOUT=90s

CMD ["watchdog-lite"]
