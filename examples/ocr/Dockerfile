# Dockerfile da função OCR (leitura da placa) — segue o contrato de
# execução de função definido na ADR-0008: o processo principal do
# contêiner é o watchdog-lite (não um shell nem o interpretador Python
# direto), que sobe FPROCESS uma única vez e faz proxy das invocações
# recebidas em um socket Unix (sem rede/CNI — ver
# docs/ADR-0008-contrato-execucao-funcao.md).
#
# Imagem multi-arquitetura (amd64 + arm64) com o MESMO modelo xs
# (MODELS.md §1–§2: accuracy igual à do s com runtime muito menor).
# Stack mínima da MODELS.md §5 ("OCR, server sidecar"): onnxruntime,
# numpy, opencv-headless — mais fast-plate-ocr (inferência + decode) e
# pillow. Sem torch, sem tensorflow, sem ultralytics.
#
# Build multi-stage (mesmo padrão de azure_functions/): o stage builder
# instala tudo em /home/app/python e limpa caches/pycache; o stage final
# copia SÓ os pacotes. fast-plate-ocr (deps base: numpy,
# opencv-headless, pyyaml, rich, tqdm; sem matplotlib/polars) não precisa
# de remoção de pacotes.
#
# Build (a partir da raiz do repositório, para ter acesso a cmd/watchdog-lite):
#   GOOS=linux GOARCH=amd64 go build -o examples/ocr/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     -t faas-continuum/ocr:latest -f examples/ocr/Dockerfile examples/ocr
#
# Variante GPU (servidor com CUDA): trocar o pacote do ORT no build:
#   docker buildx build --build-arg ONNXRUNTIME_PKG=onnxruntime-gpu ...

FROM --platform=${TARGETPLATFORM:-linux/amd64} python:3.11-slim AS builder

ARG TARGETARCH
ARG ONNXRUNTIME_PKG="onnxruntime==1.30.0"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /home/app/

COPY function/requirements.txt function/requirements.txt
# requirements.txt fixa o ORT CPU; o ARG ONNXRUNTIME_PKG permite trocar
# por onnxruntime-gpu na variante CUDA — por isso o ORT sai da lista
# aqui e entra pelo ARG (default = o mesmo pin do requirements.txt).
RUN grep -v '^onnxruntime' function/requirements.txt > /tmp/ocr-req.txt \
    && uv pip install --no-cache --system --target=/home/app/python -r /tmp/ocr-req.txt \
    && uv pip install --no-cache --system --target=/home/app/python "$ONNXRUNTIME_PKG" \
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

FROM --platform=${TARGETPLATFORM:-linux/amd64} python:3.11-slim

ARG TARGETARCH

# libgl1/libglib2.0-0: exigidos pelo OpenCV headless em algumas bases slim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd app && useradd -r -g app app

WORKDIR /home/app/

# O binário do watchdog-lite é compilado FORA do Docker (cross-compile
# Go trivial, ver ADR-0001) e só copiado aqui — não precisa de uma stage
# de build Go dentro da imagem da função. Como esta imagem é
# multi-arquitetura, há um binário por arch (TARGETARCH é automático do
# buildx: amd64/arm64) — ver "Preparar" no README.
COPY watchdog-lite-$TARGETARCH /usr/bin/watchdog-lite
RUN chmod +x /usr/bin/watchdog-lite

# Só os pacotes já instalados e limpos no builder — sem uv, sem caches.
COPY --from=builder /home/app/python /home/app/python

COPY index.py .
COPY function/ function/

# Modelo xs finetuned (3.2 MB) + config da placa — ver "Preparar" no README.
COPY best.onnx .
COPY cct_xs_v2_global_plate_config.yaml .

# IMPORTANTE: todo diretório em que o processo da função precisa
# escrever depois de USER app precisa estar nesta lista (lição do
# alpr: falhar aqui é silencioso até rodar como não-root).
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PATH=$PATH:/home/app/python/bin/
ENV PYTHONPATH=/home/app/python:/home/app
ENV MODEL_PATH=/home/app/best.onnx
ENV CONFIG_PATH=/home/app/cct_xs_v2_global_plate_config.yaml
# Núcleos do ONNX Runtime (0 = padrão do ORT; 4 é o valor validado no ARM).
ENV ORT_THREADS=0

# Contrato da ADR-0008: FPROCESS é o comando que o watchdog-lite sobe
# uma única vez; UPSTREAM_PORT é o loopback interno entre watchdog-lite
# e este processo; SOCKET_PATH é onde o agente monta o socket Unix de
# invocação via bind mount (o agente decide o valor real na hora de
# provisionar — este é só o padrão para execução standalone/manual).
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# O cold import aqui (cv2/onnxruntime/fast-plate-ocr) é de segundos, não
# dezenas de segundos como torch/ultralytics — 90s é margem confortável
# inclusive em disco lento de borda.
ENV READY_TIMEOUT=90s

CMD ["watchdog-lite"]
