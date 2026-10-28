# Dockerfile of the license plate detection function, with the ONNX Runtime
# backend. It is the ONNX counterpart of the slim edge example, with the
# same handler and the same post-processing, whose parity against the
# handler built on torch was already validated, with the same number of
# detections, the same classes and the same ranking, a confidence within
# two thousandths and an oriented box within one pixel over five images,
# and the same export, the model with an image size of 1280, without torch
# and without the ultralytics runtime in the cold path.
#
# The target is an x86 host, where the ONNX runtime wins over ncnn; on ARM
# the order is the other way around. The execution contract is the same as
# the one of the other examples: the watchdog is the process with identifier
# one, it starts the function process and it proxies in reverse the
# invocations that arrive through the Unix socket mounted by the agent.
#
# Build, from the root of the repository:
#   GOOS=linux GOARCH=amd64 go build -o examples/alpr-onnx-slim/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/amd64 \
#     -t igoricda/alpr-onnx-slim:latest \
#     -f examples/alpr-onnx-slim/Dockerfile examples/alpr-onnx-slim
#
# The watchdog binary is built outside Docker, for the architecture of the
# target host: amd64 for an x86 host and arm64 for an ARM edge host.

FROM --platform=linux/amd64 python:3.12-slim AS builder

COPY function/requirements.txt /requirements.txt
# The pins of the requirements, the ONNX runtime 1.30.0, the headless build
# of the image library 4.10.0.84 and numpy 1.26.4, are the trio that was
# validated in the parity of the handler.
RUN pip install --no-cache-dir --target=/opt/site-packages -r /requirements.txt \
  && find /opt/site-packages -name "__pycache__" -type d -prune -exec rm -rf {} + \
  && find /opt/site-packages -name "*.pyc" -delete \
  && rm -rf /tmp/* /root/.cache

FROM --platform=linux/amd64 python:3.12-slim

# The system packages required by the headless build of the image library,
# which are the toolkit and the OpenGL library; the headless build does not
# ask for X11.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd app && useradd -r -g app app

WORKDIR /home/app/

COPY watchdog-lite /usr/bin/watchdog-lite
RUN chmod +x /usr/bin/watchdog-lite

# Only the packages already installed and cleaned in the builder, without
# the caches.
COPY --from=builder /opt/site-packages /opt/site-packages

COPY index.py .
COPY function/ function/
COPY best.onnx .

# IMPORTANT: every directory that the process of the function has to write
# to after the unprivileged user is in effect has to be in this list. A
# missing entry is silent until the container runs as an unprivileged user.
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/site-packages:/home/app
ENV MODEL_PATH=/home/app/best.onnx
# The same knobs as the other examples of the family: the image size of
# 1280 is the static shape frozen in the exported graph.
ENV CONFIDENCE_THRESHOLD=0.25
ENV IOU_THRESHOLD=0.7
ENV MAX_DET=300
ENV IMAGE_SIZE=1280
# Zero leaves the default of the ONNX runtime, which uses more than one
# thread; a positive value pins the count. On a host with several
# instances, limiting it avoids the oversubscription of the processors of
# the node.
ENV ORT_THREADS=0

# The execution contract, the same as the one of the other examples.
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# The cold start is the import of the image library and of numpy, the
# session of the runtime with the model and the warmup. Without torch the
# import is orders of magnitude smaller, so sixty seconds gives a generous
# margin without masking a real failure of the startup. It has to be
# recalibrated on the target host.
ENV READY_TIMEOUT=60s

CMD ["watchdog-lite"]
