# Dockerfile of the YOLO26n COCO detection function on an x86 host, with
# the ONNX Runtime backend. It is the third member of the family of the
# YOLO26n examples: the amd64 one built on torch with the CPU build of the
# runtime, the slim edge one built on ncnn, and this one built on the ONNX
# runtime.
#
# Composition: the **same handler and post-processing** as the slim ncnn
# example, a one to one port whose parity against torch and ncnn was
# validated there, plus the execution contract of the examples, with the
# watchdog as the process with identifier one and the function process,
# the upstream port and the socket path. There is no torch and no
# ultralytics in the cold path: only the ONNX runtime, numpy and the
# headless build of the image library.
#
# Build, from the root of the repository:
#   GOOS=linux GOARCH=amd64 go build -o examples/yolo26n-coco-onnx-slim/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/amd64 \
#     -t igoricda/yolo26n-coco-onnx-slim:latest \
#     -f examples/yolo26n-coco-onnx-slim/Dockerfile examples/yolo26n-coco-onnx-slim
#
# The watchdog binary is built outside Docker, for the architecture of the
# target host: amd64 for an x86 host and arm64 for an ARM edge host.

FROM --platform=linux/amd64 python:3.12-slim AS builder

COPY function/requirements.txt /requirements.txt
# The pins of the requirements, the ONNX runtime 1.30.0, the headless build
# of the image library 4.10.0.84 and numpy 1.26.4, are the ones that
# guarantee an annotated JPEG byte identical to the validated one.
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
COPY yolo26n.onnx .
COPY coco80.names .

# IMPORTANT: every directory that the process of the function has to write
# to after the unprivileged user is in effect has to be in this list. A
# missing entry is silent until the container runs as an unprivileged user.
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/site-packages:/home/app
ENV MODEL_PATH=/home/app/yolo26n.onnx
ENV NAMES_PATH=/home/app/coco80.names
# The same knobs as the other two examples of the family.
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
# Zero leaves the default of the ONNX runtime, which uses more than one
# thread; a positive value pins the count. On a host with several
# instances, limiting it avoids the oversubscription of the processors of
# the node.
ENV ORT_THREADS=0

# The execution contract: the function process is the command that the
# watchdog starts exactly once, the upstream port is the loopback between
# the watchdog and this process, and the socket path is where the node
# agent mounts the Unix socket of the invocations with a bind mount. The
# agent decides the real value during the provisioning, so this one is
# only the default for a standalone run.
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# The cold start is the import of the image library and of numpy, the
# creation of the session of the runtime and the warmup. Without torch the
# import is orders of magnitude smaller, so sixty seconds gives a generous
# margin without masking a real failure of the startup. It has to be
# recalibrated on the target host.
ENV READY_TIMEOUT=60s

CMD ["watchdog-lite"]
