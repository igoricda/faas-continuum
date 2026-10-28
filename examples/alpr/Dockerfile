# Dockerfile of the server license plate detection function, with the full
# model. The main process of the container is the watchdog, not a shell
# and not the Python interpreter directly: it starts the function process
# exactly once and proxies the invocations it receives over a Unix socket,
# with no network configuration needed.
#
# An amd64 image for a cloud host with a GPU, with the full PyTorch model:
# the ultralytics runtime with torch, and CUDA through the default of
# PyPI, plus numpy. It is the server counterpart of the edge example, with
# the same input and the same JSON output and only a different backend.
#
# Multi stage build: the builder stage installs everything under
# /home/app/python and removes the plotting and the dataframe packages,
# with their exclusive dependencies, the caches and the compiled files;
# the final stage copies **only** the packages. The plotting and the
# dataframe packages are declared dependencies of the ultralytics runtime,
# but every import of them inside it is lazy, in the plotting, in the
# benchmarks, in the callbacks and in the trainer, so the hot path, which
# is the import and the inference, does not use them. The CUDA build of
# torch and the driver packages stay, since they are the runtime of the
# GPU.
#
# Build, from the root of the repository:
#   GOOS=linux GOARCH=amd64 go build -o examples/alpr/watchdog-lite ./cmd/watchdog-lite
#   docker build --platform linux/amd64 -t faas-continuum/alpr:latest \
#     -f examples/alpr/Dockerfile examples/alpr

FROM --platform=linux/amd64 python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /home/app/

COPY function/requirements.txt function/requirements.txt
# The ultralytics runtime pulls the CUDA build of torch, from the default
# wheels of PyPI. On a host without a GPU the same torch runs on the CPU;
# with a GPU, start the container with the flag that exposes it.
RUN uv pip install --no-cache --system --target=/home/app/python -r function/requirements.txt \
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

FROM --platform=linux/amd64 python:3.11-slim

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
COPY best.pt .

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
ENV MODEL_PATH=/home/app/best.pt
ENV CONFIDENCE_THRESHOLD=0.25
ENV IMAGE_SIZE=1280

# The execution contract: the function process is the command that the
# watchdog starts exactly once, the upstream port is the loopback between
# the watchdog and this process, and the socket path is where the node
# agent mounts the Unix socket of the invocations with a bind mount. The
# agent decides the real value during the provisioning, so this one is
# only the default for a standalone run.
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

# The torch and the ultralytics runtime are known to take 30 to 60
# seconds on the first cold import, since the native libraries have to be
# loaded, and that has nothing to do with the size of the model itself.
# The default of the watchdog of 30 seconds sits exactly at the limit of
# that worst case, so 90 seconds gives a comfortable margin without
# masking a real failure of the startup.
ENV READY_TIMEOUT=90s

CMD ["watchdog-lite"]
