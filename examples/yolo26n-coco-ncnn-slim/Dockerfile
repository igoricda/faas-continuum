# Dockerfile of the YOLO26n COCO detection function at the edge, with the
# slim direct ncnn backend. The main process of the container is the
# watchdog, which starts the function process exactly once and proxies the
# invocations it receives over a Unix socket, with no network configuration
# needed.
#
# Slim counterpart of the amd64 example: the **same weights**, the
# checkpoint exported to ncnn with an image size of 640, the same input and
# the same JSON output, with the annotated image in base64 and the
# detections, but **without torch and without ultralytics**, with only
# ncnn, numpy and the headless build of the image library. The ncnn export
# already carries the decoding inside the graph, with the anchors, the
# strides and the sigmoid at the end of the parameter file, so the handler
# replicates the letterbox and the suppression in numpy, with the parity
# measured against the handler of the ultralytics runtime. See the README.
# Without the weight of torch the image stays in the hundreds of
# megabytes and the cold start does not pay the import of torch and of the
# ultralytics runtime.
#
# Build, from the root of the repository:
#   GOOS=linux GOARCH=arm64 go build -o examples/yolo26n-coco-ncnn-slim/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/arm64 \
#     -t faas-continuum/yolo26n-coco-ncnn-slim:latest \
#     -f examples/yolo26n-coco-ncnn-slim/Dockerfile examples/yolo26n-coco-ncnn-slim
#
# A **native** build on the ARM host is preferable, since a cross build
# through the emulation is slow.

FROM --platform=linux/arm64 python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /home/app/

COPY function/requirements.txt function/requirements.txt
# Only three packages: ncnn, numpy and the headless build of the image
# library. There is no torch, no ultralytics and no tensorflow, since the
# hot path is pure ncnn.
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

# The watchdog binary is built outside Docker, since cross compiling Go is
# trivial, and it is only copied here: the image of the function does not
# need a build stage for Go inside it.
COPY watchdog-lite /usr/bin/watchdog-lite
RUN chmod +x /usr/bin/watchdog-lite

# Only the packages already installed and cleaned in the builder, without
# the installer and the caches.
COPY --from=builder /home/app/python /home/app/python

COPY index.py .
COPY function/ function/

# The ncnn detector exported with an image size of 640, with the weights,
# the parameters and the metadata file. The metadata is the provenance of
# the export, with the task, the image size and the names of the eighty
# classes; the handler itself reads the names from the plain text file,
# since without a YAML parser the metadata file cannot be parsed.
COPY yolo26n_ncnn_model/ yolo26n_ncnn_model/
COPY coco80.names .

# IMPORTANT: every directory that the process of the function has to write
# to after the unprivileged user is in effect has to be in this list.
# Without the ultralytics runtime there is no configuration directory of
# it, since that belongs to the counterpart built on the checkpoint.
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PATH=$PATH:/home/app/python/bin/
ENV PYTHONPATH=/home/app/python:/home/app
ENV MODEL_PATH=/home/app/yolo26n_ncnn_model
ENV NAMES_PATH=/home/app/coco80.names
# The defaults of the ultralytics runtime, all of them overridable through
# the environment.
ENV CONFIDENCE_THRESHOLD=0.25
ENV IOU_THRESHOLD=0.7
ENV MAX_DET=300
ENV IMAGE_SIZE=640
# With the annotation disabled the answer carries only the detections; with
# a positive maximum side the annotated image, and the coordinates with
# it, is reduced to fit the output ceiling of the node, of eight mebibytes.
ENV ANNOTATE=1
ENV JPEG_QUALITY=85
ENV ANNOTATE_MAX_SIDE=0
# Zero leaves the default of the ncnn runtime, which already opens more
# than one thread; a positive value pins the count, and four threads were
# validated on ARM.
ENV N_THREADS=0

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
# orders of magnitude smaller than the thirty to sixty seconds of the
# counterpart built on the checkpoint, so sixty seconds gives a generous
# margin. It has to be recalibrated after the measurement on the host; the
# HTTP process also runs a warmup before the ready event.
ENV READY_TIMEOUT=60s

CMD ["watchdog-lite"]
