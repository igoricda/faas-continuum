# Example: hello world function

The minimal version of the license plate detection example: no system
dependencies, no model weights and no Python packages. It exercises the
whole chain, which is the watchdog, the HTTP process, the handler, the Unix
socket and the OCI build, with a fast build and a fast cold start. The
input is a raw string, so an invocation is a `curl` of a single word.

## Readiness contract

The `index.py` implements **bind first and lazy load**:

- The HTTP server binds `127.0.0.1:$PORT` **before** importing
  `function.handler`.
- A daemon thread loads `handle()` and signals the `threading.Event` named
  `_ready`.
- **GET `/_/ready`**: `200 {"ready": true}` when ready and
  `503 {"ready": false}` otherwise.
- **POST `/_/ready`**: **always `404`**.
- **POST `/`**: `503` while loading and `200` with a valid JSON body after
  the ready event.
- **Unknown GET**: `404`.

Environment variables of the **watchdog**, injected through the `ENV`
directives of the Dockerfile or the defaults of the code:

- `READY_PATH` (default `/_/ready`) — the route that is polled.
- `READY_TIMEOUT` (default `90s`) — the ceiling of the poll. The **Go
  duration syntax is mandatory** (`90s`, `500ms`, `15m`; never a bare `60`).
- `READY_INTERVAL` (default `250ms`) — the interval between the polls.

The watchdog **does not open the Unix socket** until it receives `200` on
`GET /_/ready`. A refused dial or a status other than `200` keeps the poll
running; only `200` lets it proceed.

## Structure

```
function/handler.py   # handle(req): stateless, returns {"message": "Hello, <req>!"} as JSON
index.py              # the minimal HTTP server expected by the watchdog
function/requirements.txt  # empty, the install line lives in the Dockerfile to receive the dependencies
Dockerfile            # the final image, with the watchdog as the main process
```

## Prepare

```bash
# from the root of the repository
GOOS=linux GOARCH=amd64 go build -o examples/hello-world/watchdog-lite ./cmd/watchdog-lite
# on an ARM host, use GOARCH=arm64 instead
```

## Build and test with Docker

```bash
cd examples/hello-world
docker build -t faas-continuum/hello-world:latest .
docker run --rm -d --name hello-world-test \
  -v /tmp/hello-world-sockets:/run/faas-continuum-socket \
  faas-continuum/hello-world:latest

curl -s --unix-socket /tmp/hello-world-sockets/handle.sock http://localhost/ -d 'world'
# -> {"message": "Hello, world!"}

curl -s --unix-socket /tmp/hello-world-sockets/handle.sock http://localhost/ -d ''
# -> {"message": "Hello, world!"}  (an empty body becomes "world")
```

The output is valid JSON by contract: the node validates the answer of the
function and responds with **502 `INVALID_FUNCTION_OUTPUT`** to any
invocation whose output is not JSON.

## What was validated without Docker, on the same code path

The code path inside the container, which is the watchdog, the HTTP process
and the handler, exactly as the Dockerfile packages them, was validated by
running them as real local processes:

- a request over the Unix socket: **200 OK** with
  `{"message": "Hello, world!"}`, in the order of milliseconds;
- an empty body: **200 OK** with `{"message": "Hello, world!"}`, and the
  connection is not dropped;
- the log of the watchdog confirms the contract: the function process starts
  **exactly once** and stays resident between invocations.

The full build with `docker build` and `docker run` still has to be run
before the function is considered ready, since the permissions of the
unprivileged user of the image are only exercised there.
