# Dockerfile of the edge license plate detection function, with the ncnn
# backend. The main process of the container is the watchdog, not a shell
# and not the Python interpreter directly: it starts the function process
# exactly once and proxies the invocations it receives over a Unix socket,
# with no network configuration needed.
#
# An arm64 only image for the ARM hosts with the ncnn detector: the
# ultralytics runtime with ncnn and numpy below the version two, together
# with the CPU build of torch, since the default build of torch for
# aarch64 drags gigabytes of driver packages that are useless here. The
# ncnn export uses an image size of 640, with a mean average precision at
# the threshold of 0.5 of 0.995 and at the threshold of 0.95 of 0.984 and
# an inference of about 222 ms on four Cortex-A55 cores, which is about
# four times faster than the export with an image size of 1280. It is the
# edge counterpart of the server example, with the same input and the same
# JSON output and only a different backend.
#
# Multi stage build: the builder stage installs everything under
# /home/app/python and removes the plotting and the dataframe packages,
# with their exclusive dependencies, the caches and the compiled files;
# the final stage copies **only** the packages. The plotting and the
# dataframe packages are declared dependencies of the ultralytics runtime,
# but every import of them inside it is lazy, in the plotting, in the
# benchmarks, in the callbacks and in the trainer, so the hot path, which
# is the import and the inference, does not use them.
#
# Build, from the root of the repository:
#   GOOS=linux GOARCH=arm64 go build -o examples/alpr-ncnn/watchdog-lite ./cmd/watchdog-lite
#   docker buildx build --platform linux/arm64 \
#     -t faas-continuum/alpr-ncnn:latest -f examples/alpr-ncnn/Dockerfile examples/alpr-ncnn

FROM --platform=linux/arm64 python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /home/app/

COPY function/requirements.txt function/requirements.txt
# The order and the single command matter: the CPU build of torch comes
# FIRST, from the CPU index of PyTorch, since the default build of torch
# for aarch64 pulls gigabytes of driver packages that are useless on an
# ARM host. The installation happens in a SINGLE command: with a target
# directory the resolver does not see it as already satisfied, so a second
# install would put the CUDA build of torch back on top of the CPU one.
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

# The system packages required by the image library, which the ultralytics
# runtime uses internally.
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
# the installer, the caches and the plotting packages.
COPY --from=builder /home/app/python /home/app/python

COPY index.py .
COPY function/ function/

# The ncnn detector exported with an image size of 640, with the weights,
# the parameters and the metadata. Without the metadata the ultralytics
# runtime does not know that the task is the oriented one and does not
# know the name of the class, so it loads a thousand generic classes and
# the detections reach zero.
COPY best_ncnn_model/ best_ncnn_model/

# IMPORTANT: every directory that the process of the function has to write
# to after the unprivileged user is in effect has to be in this list. It
# includes the configuration directory of the ultralytics runtime, and not
# only the home directory and the socket. A missing entry is silent until
# the container runs as an unprivileged user.
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

# The execution contract: the function process is the command that the
# watchdog starts exactly once, the upstream port is the loopback between
# the watchdog and this process, and the socket path is where the node
# agent mounts the Unix socket of the invocations with a bind mount. The
# agent decides the real value during the provisioning, so this one is
# only the default for a standalone run.
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# The cold import of the ultralytics runtime and of ncnn on a slow ARM
# host easily exceeds the default of 30 seconds of the watchdog, so 90
# seconds gives margin without masking a real failure. On the ARM host the
# inference through ncnn with an image size of 640 takes about 270 ms per
# image, so the timeout of the registration on the agent has to cover it
# with room to spare.
ENV READY_TIMEOUT=90s

CMD ["watchdog-lite"]
