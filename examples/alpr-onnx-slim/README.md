# License plate detection in the fog, with the ONNX Runtime backend

The license plate detector, with the single class `License-plate` and an
oriented box, for an **x86** host, following the same execution contract as
the other examples: the watchdog is the process with identifier one, it
starts the function process and it proxies the invocations in reverse over
the Unix socket.

The family of the license plate examples:

| Example | Backend | Target host |
|---|---|---|
| `examples/alpr` | torch and the ultralytics runtime | a server with a GPU |
| `examples/alpr-ncnn-slim` | ncnn | an ARM edge host |
| `examples/alpr-onnx-slim` | the ONNX runtime | an x86 host |

The handler is a port of the handler of the ncnn slim example, whose parity
against the handler built on torch was already validated, with the same
number of detections, the same classes and the same ranking, a confidence
within two thousandths and an oriented box within one pixel over five
images. What changes here is only the envelope of the local execution: the
HTTP process with the loopback of the watchdog, the readiness route and the
warmup, and the image with the watchdog built in.

## Input and output

```jsonc
// The input is the raw one of the node; the envelope {"input": ...} is
// also accepted.
{"image_base64": "<jpg or png in base64>"}

// The output
{"status": "success", "detections": [{"class": "License-plate",
  "confidence": 0.9, "obb": {"x": 0, "y": 0, "w": 0, "h": 0, "rotation_rad": 0}}]}
{"status": "error", "message": "..."}
```

## Build

From the root of the repository:

```sh
GOOS=linux GOARCH=amd64 go build -o examples/alpr-onnx-slim/watchdog-lite ./cmd/watchdog-lite
docker buildx build --platform linux/amd64 \
  -t igoricda/alpr-onnx-slim:latest \
  -f examples/alpr-onnx-slim/Dockerfile examples/alpr-onnx-slim
```

The watchdog binary is built outside the image, for the architecture of the
target host, and it is ignored by Git with the pattern
`/examples/*/watchdog-lite`, so every checkout has to build its own.

## Local smoke test, without the node agent

```sh
mkdir -p /tmp/sock && chmod 777 /tmp/sock
docker run -d --name alpr-smoke -v /tmp/sock:/run/faas-continuum-socket \
  igoricda/alpr-onnx-slim:latest
# POST the payload over the socket:
#   /run/faas-continuum-socket/handle.sock
```

## Deployment on the node

1. Register the function on the node agent, with the suggested name `alpr`
   and the category `lpr`, and with the timeout and the idle timeout of the
   workload.
2. Include `"alpr":"lpr"` in the `-function-categories` of the agent, so
   that the node announces the category to the arbiter.
3. Optionally pull the image ahead of time with
   `ctr -n faas-continuum images pull docker.io/igoricda/alpr-onnx-slim:latest`
   and restart the agent so that it reads the registry again.
