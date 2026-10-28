# Example: sleep function

A function whose workload is the wait itself: it receives a number of
seconds, sleeps for that time and answers `slept for X seconds`. It is
useful to exercise timeouts, objectives (`max_latency_ms`) and scaling under
a controlled load, without depending on a model or on any input and output.

## Readiness contract

The `index.py` implements **bind first and lazy load**, the same pattern as
the hello world and the detection examples:

- The HTTP server binds `127.0.0.1:$PORT` **before** any heavy work.
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

## Input and output contract

The pattern is the `status` and `message` pair of the other examples. The
output is valid JSON by contract, since the node responds with **502
`INVALID_FUNCTION_OUTPUT`** to any output that is not JSON:

```
input:   a bare number in the body ("3") or the JSON {"seconds": 3}
output:  {"status": "success", "message": "slept for 3 seconds"}
         or {"status": "error", "message": "invalid input: ..."}
```

## Structure

```
function/handler.py   # handle(req): sleeps for N seconds, returns {"status": "success", "message": "slept for X seconds"} as JSON
index.py              # the minimal HTTP server expected by the watchdog
function/requirements.txt  # empty, the install line lives in the Dockerfile to receive the dependencies
Dockerfile            # the final image, with the watchdog as the main process
```

## Prepare

```bash
# from the root of the repository
GOOS=linux GOARCH=amd64 go build -o examples/sleep/watchdog-lite ./cmd/watchdog-lite
# on an ARM host, use GOARCH=arm64 instead
```

## Build and test with Docker

```bash
cd examples/sleep
docker build -t igoricda/sleep:latest .

# The bind mount overrides the ownership set by the Dockerfile: the host
# directory has to belong to the user of the container, which is 999:1000.
mkdir -p /tmp/sleep-sockets && sudo chown 999:1000 /tmp/sleep-sockets

docker run --rm -d --name sleep-test \
  -v /tmp/sleep-sockets:/run/faas-continuum-socket \
  igoricda/sleep:latest

# The watchdog creates the socket with the mode 0755 and the owner set to
# the unprivileged user of the image, so an unprivileged client on the host
# cannot connect; use sudo for the local test.
sudo curl -s --unix-socket /tmp/sleep-sockets/handle.sock http://localhost/ -d '2'
# -> {"status": "success", "message": "slept for 2 seconds"}  (it really took about 2s)

sudo curl -s --unix-socket /tmp/sleep-sockets/handle.sock http://localhost/ -d '{"seconds": 1}'
# -> {"status": "success", "message": "slept for 1 seconds"}

sudo curl -s --unix-socket /tmp/sleep-sockets/handle.sock http://localhost/ -d 'abc'
# -> {"status": "error", "message": "invalid input: ..."}

sudo curl -s --unix-socket /tmp/sleep-sockets/handle.sock http://localhost/ -d '-1'
# -> {"status": "error", "message": "invalid input: the number of seconds cannot be negative"}

docker rm -f sleep-test
```

On a real node the permission is not a problem, since the socket is
provisioned by the node agent itself, which is the same origin as the bind
mount and as the process.

## Publish

```bash
docker push igoricda/sleep:latest
```
