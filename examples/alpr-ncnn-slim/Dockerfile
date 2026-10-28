# Dockerfile of the slim edge license plate detection function, with the
# direct ncnn backend. The main process of the container is the watchdog,
# which starts the function process exactly once and proxies the
# invocations it receives over a Unix socket, with no network
# configuration needed.
#
# Slim counterpart of the edge example: the same model, the directory
# `best_ncnn_model` exported with an image size of 640, and the same JSON
# output, but **without torch and without ultralytics**, with only ncnn,
# numpy and the headless build of opencv. The ncnn export already carries
# the decoding inside the graph, so the handler replicates the letterbox
# and the suppression with the probabilistic intersection over union in
# numpy, with a bit exact parity against the handler built on the
# ultralytics runtime. Without the weight of torch, the cold start on ARM
# falls from about 60 seconds to about 10 seconds, which is a projection
# and has to be confirmed by the first measurement on the host.
#
# Build, from the root of the repository:
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

# The system packages required by the headless build of the image library.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd app && useradd -r -g app app

WORKDIR /home/app/

# The watchdog binary is built outside Docker, since cross compiling Go
# is trivial, and it is only copied here: the image of the function does
# not need a build stage for Go inside it.
COPY watchdog-lite /usr/bin/watchdog-lite
RUN chmod +x /usr/bin/watchdog-lite

# Only the packages already installed and cleaned in the builder, without
# the installer and the caches.
COPY --from=builder /home/app/python /home/app/python

COPY index.py .
COPY function/ function/

# The ncnn detector exported with an image size of 640, with the weights,
# the parameters and the metadata, the same trio as the edge example.
COPY best_ncnn_model/ best_ncnn_model/

# IMPORTANT: every directory that the process of the function has to write
# to after the unprivileged user is in effect has to be in this list.
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

# The execution contract: the function process is the command that the
# watchdog starts exactly once, the upstream port is the loopback between
# the watchdog and this process, and the socket path is where the node
# agent mounts the Unix socket of the invocations with a bind mount. The
# agent decides the real value during the provisioning, so this one is
# only the default for a standalone run.
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# Without torch and without the ultralytics runtime the cold import is
# orders of magnitude smaller than the 90 seconds of the edge example, so
# 60 seconds gives plenty of margin. It has to be recalibrated after the
# measurement.
ENV READY_TIMEOUT=60s

CMD ["watchdog-lite"]
