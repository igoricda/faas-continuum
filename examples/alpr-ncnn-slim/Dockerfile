# Dockerfile da função ALPR borda slim (detecção via ncnn direto) — segue
# o contrato de execução de função definido na ADR-0008: o processo
# principal do contêiner é o watchdog-lite, que sobe FPROCESS uma única
# vez e faz proxy das invocações recebidas em um socket Unix (sem
# rede/CNI — ver docs/ADR-0008-contrato-execucao-funcao.md).
#
# Par slim de examples/alpr-ncnn: mesmo modelo (best_ncnn_model,
# imgsz=640), mesma saída JSON, mas SEM torch/ultralytics — só ncnn +
# numpy + opencv-headless. O export ncnn já traz o decode embutido no
# grafo, então o handler replica letterbox + NMS-probiou em numpy com
# paridade bit-exata (ver README). Sem o elefante do torch, o cold no
# ARM cai da casa dos 60 s para a casa dos ~10 s (projetado — a
# confirmar na primeira medição no box).
#
# Build (a partir da raiz do repositório, para ter acesso a cmd/watchdog-lite):
#   GOOS=linux GOARCH=arm64 go build -o examples/alpr-ncnn-slim/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/arm64 \
#     -t faas-continuum/alpr-ncnn-slim:latest -f examples/alpr-ncnn-slim/Dockerfile examples/alpr-ncnn-slim

FROM --platform=linux/arm64 python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /home/app/

COPY function/requirements.txt function/requirements.txt
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
# + metadata.yaml) — mesmo trio do alpr-ncnn.
COPY best_ncnn_model/ best_ncnn_model/

# IMPORTANTE: todo diretório em que o processo da função precisa
# escrever depois de USER app precisa estar nesta lista.
RUN mkdir -p /run/faas-continuum-socket /tmp/Ultralytics && \
    chown -R app:app /home/app /run/faas-continuum-socket /tmp/Ultralytics

USER app

ENV PATH=$PATH:/home/app/python/bin/
ENV PYTHONPATH=/home/app/python:/home/app
ENV MODEL_PATH=/home/app/best_ncnn_model
ENV CONFIDENCE_THRESHOLD=0.25
ENV IMAGE_SIZE=640
ENV IOU_THRESHOLD=0.7
ENV MAX_DET=300

# Contrato da ADR-0008: FPROCESS é o comando que o watchdog-lite sobe
# uma única vez; UPSTREAM_PORT é o loopback interno entre watchdog-lite
# e este processo; SOCKET_PATH é onde o agente monta o socket Unix de
# invocação via bind mount (o agente decide o valor real na hora de
# provisionar — este é só o padrão para execução standalone/manual).
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# Sem torch/ultralytics o cold import é ordens de grandeza menor que os
# 90s do alpr-ncnn — 60s dá margem folgada (recalibrar na medição).
ENV READY_TIMEOUT=60s

CMD ["watchdog-lite"]
