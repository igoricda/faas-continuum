# YOLO26n COCO in the fog, with the ONNX Runtime backend

The generic detection function of the YOLO26n model, with the eighty
classes of COCO, for an **x86** host, following the same execution contract
as the other examples: the watchdog is the process with identifier one, it
starts the function process and it proxies in reverse the invocations that
arrive through the Unix socket mounted by the node agent.

It is the third member of the family of the YOLO26n examples:

| Example | Backend | Target host |
|---|---|---|
| `examples/yolo26n-coco` | torch with the CPU build and the ultralytics runtime | an x86 server |
| `examples/yolo26n-coco-ncnn-slim` | ncnn | an ARM edge host |
| `examples/yolo26n-coco-onnx-slim` | the ONNX runtime | an x86 host |

The handler and its post-processing are a one to one port of the slim ncnn
example, whose parity with the other backends was already validated there,
with the same number of detections, the same classes and the same ranking,
a confidence within two thousandths and an oriented box within one pixel.
What changes here is only the envelope of the local execution: the HTTP
process with the loopback of the watchdog, with the readiness route and the
unwrapping of the envelope, instead of the shim of the serverless host, and
the image with the watchdog built in.

## Build

From the root of the repository:

```sh
GOOS=linux GOARCH=amd64 go build -o examples/yolo26n-coco-onnx-slim/watchdog-lite ./cmd/watchdog-lite
docker buildx build --platform linux/amd64 \
  -t igoricda/yolo26n-coco-onnx-slim:latest \
  -f examples/yolo26n-coco-onnx-slim/Dockerfile examples/yolo26n-coco-onnx-slim
```

The watchdog binary is built outside the image, for the architecture of the
target host, and it is ignored by Git with the pattern
`/examples/*/watchdog-lite`, so every checkout has to build its own.

## Contract

- The function process is started with `python3 index.py`, the upstream
  port is 8081, the socket is `/run/faas-continuum-socket/handle.sock` and
  the readiness timeout is 60 seconds.
- `GET /_/ready` answers with 503 until the handler, which is the session of
  the runtime, is loaded and the dummy warmup has run, and with 200
  afterwards. The Unix socket only opens after the 200.
- The node posts the **raw** input on `POST /`, and the envelope
  `{"input": ...}` is also accepted and unwrapped by the HTTP process.
- The output is the JSON produced by the handler, either
  `{"status":"success", ...}` or `{"status":"error", ...}`, with the ceiling
  of eight mebibytes of the node.

## Local smoke test, without the node agent

```sh
mkdir -p /tmp/sock && chmod 777 /tmp/sock
docker run -d --name onnx-smoke -v /tmp/sock:/run/faas-continuum-socket \
  igoricda/yolo26n-coco-onnx-slim:latest
docker exec onnx-smoke python3 -c '
import http.client, socket, json
class U(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/run/faas-continuum-socket/handle.sock")
c = U("x"); c.request("GET", "/_/ready"); print(c.getresponse().status)'
```

The expected answer during the cold start, in the log of the watchdog, is
that the function process is ready on the loopback interface, followed by
the confirmation of the warmup.

## Deployment on the node

The node resolves the image by its OCI reference during the provisioning.
To change the image of the function without registering it again:

1. Edit the image reference in the local registry, under
   `/var/lib/faas-continuum/functions.json`, and keep the timeout, the idle
   timeout and the minimum and maximum number of instances, or use the
   `PUT /function/yolo26n-coco` route of the management API of the agent.
2. Optionally pull the image ahead of time with
   `ctr -n faas-continuum images pull docker.io/igoricda/yolo26n-coco-onnx-slim:latest`,
   which avoids paying the pull on the first provisioning.
3. Restart the agent so that it reads the registry again.
