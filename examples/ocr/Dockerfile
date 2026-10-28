# Dockerfile of the text recognition function, which reads the license
# plate. The main process of the container is the watchdog, not a shell
# and not the Python interpreter directly: it starts the function process
# exactly once and proxies the invocations it receives over a Unix socket,
# with no network configuration needed.
#
# A multi-architecture image, for amd64 and arm64, with the **same** small
# model, which reaches the accuracy of the larger variant with a much
# smaller runtime. The minimal stack is the ONNX runtime, numpy and the
# headless build of the image library, plus the recognition package, which
# does the inference and the decoding, and pillow. There is no torch, no
# tensorflow and no ultralytics.
#
# Multi stage build: the builder stage installs everything under
# /home/app/python and cleans the caches and the compiled files; the final
# stage copies **only** the packages. The recognition package depends on
# numpy, on the headless build of the image library, on the YAML parser,
# on the progress bars and on the progress bars themselves, without the
# plotting and the dataframe packages, so no package has to be removed.
#
# Build, from the root of the repository:
#   GOOS=linux GOARCH=amd64 go build -o examples/ocr/watchdog-lite-amd64 ./cmd/watchdog-lite
#   GOOS=linux GOARCH=arm64 go build -o examples/ocr/watchdog-lite-arm64 ./cmd/watchdog-lite
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     -t faas-continuum/ocr:latest -f examples/ocr/Dockerfile examples/ocr
#
# The GPU variant, for a server with CUDA, swaps the package of the runtime
# at build time:
#   docker buildx build --build-arg ONNXRUNTIME_PKG=onnxruntime-gpu ...

FROM --platform=${TARGETPLATFORM:-linux/amd64} python:3.11-slim AS builder

ARG TARGETARCH
ARG ONNXRUNTIME_PKG="onnxruntime==1.30.0"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /home/app/

COPY function/requirements.txt function/requirements.txt
# The requirements file pins the CPU build of the runtime, and the build
# argument allows swapping it for the GPU build in the CUDA variant, which
# is why the runtime is removed from the list here and installed through
# the argument, whose default is the same pin of the requirements file.
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

# The toolkit and the OpenGL library are required by the headless build of
# the image library in some of the slim base images.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libglib2.0-0 libgl1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd app && useradd -r -g app app

WORKDIR /home/app/

# The watchdog binary is built outside Docker, since cross compiling Go is
# trivial, and it is only copied here: the image of the function does not
# need a build stage for Go inside it. Since this image is multi
# architecture, there is one binary per architecture, and the build
# argument of the builder is set automatically by the builder, with amd64
# and arm64. See "Prepare" in the README.
COPY watchdog-lite-$TARGETARCH /usr/bin/watchdog-lite
RUN chmod +x /usr/bin/watchdog-lite

# Only the packages already installed and cleaned in the builder, without
# the installer and the caches.
COPY --from=builder /home/app/python /home/app/python

COPY index.py .
COPY function/ function/

# The fine tuned small model, 3.2 MB, and the configuration of the plate.
COPY best.onnx .
COPY cct_xs_v2_global_plate_config.yaml .

# IMPORTANT: every directory that the process of the function has to write
# to after the unprivileged user is in effect has to be in this list. A
# missing entry is silent until the container runs as an unprivileged user.
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PATH=$PATH:/home/app/python/bin/
ENV PYTHONPATH=/home/app/python:/home/app
ENV MODEL_PATH=/home/app/best.onnx
ENV CONFIG_PATH=/home/app/cct_xs_v2_global_plate_config.yaml
# The cores used by the ONNX runtime, where zero means the default of the
# runtime and four is the value validated on ARM.
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

# The cold import here, of the image library, of the runtime and of the
# recognition package, takes seconds and not tens of seconds, so ninety
# seconds is a comfortable margin even on a slow edge storage.
ENV READY_TIMEOUT=90s

CMD ["watchdog-lite"]
