# Dockerfile of the sleep function. The main process of the container is
# the watchdog, not a shell and not the Python interpreter directly: it
# starts the function process exactly once and proxies the invocations it
# receives over a Unix socket, with no network configuration needed.
#
# Like the hello world example: no system dependencies, no model weights
# and no Python packages, so the build and the cold start are fast. The
# workload is the wait time declared in the input.
#
# Build, from the root of the repository:
#   GOOS=linux GOARCH=amd64 go build -o examples/sleep/watchdog-lite ./cmd/watchdog-lite
#   docker build -t igoricda/sleep:latest -f examples/sleep/Dockerfile examples/sleep

FROM --platform=${TARGETPLATFORM:-linux/amd64} python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN groupadd app && useradd -r -g app app

WORKDIR /home/app/

# The watchdog binary is built outside Docker, since cross compiling Go
# is trivial, and it is only copied here: the image of the function does
# not need a build stage for Go inside it.
COPY watchdog-lite /usr/bin/watchdog-lite
RUN chmod +x /usr/bin/watchdog-lite

COPY index.py .
COPY function/requirements.txt function/requirements.txt
# The requirements file is empty today; the line stays so that the example
# is already ready to receive real dependencies.
RUN uv pip install --no-cache --system --target=/home/app/python -r function/requirements.txt

COPY function/ function/

# Every directory that the process of the function has to write to after
# the unprivileged user is in effect has to be in this list.
RUN mkdir -p /run/faas-continuum-socket && \
    chown -R app:app /home/app /run/faas-continuum-socket

USER app

ENV PATH=$PATH:/home/app/python/bin/
ENV PYTHONPATH=/home/app/python:/home/app

# The execution contract: the function process is the command that the
# watchdog starts exactly once, the upstream port is the loopback between
# the watchdog and this process, and the socket path is where the node
# agent mounts the Unix socket of the invocations with a bind mount. The
# agent decides the real value during the provisioning, so this one is
# only the default for a standalone run.
ENV FPROCESS="python3 index.py"
ENV UPSTREAM_PORT=8081
ENV SOCKET_PATH=/run/faas-continuum-socket/handle.sock

CMD ["watchdog-lite"]
