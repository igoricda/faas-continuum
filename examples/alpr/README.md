# Example: server license plate detection function (YOLO26-OBB, full model)

The first stage of the license plate pipeline in the cloud. It receives the
image of the scene and returns the coordinates of the plates as JSON. It is
the server counterpart of the [edge example](../alpr-ncnn): the input and the
output are the same and only the backend of the detector changes.

## Tags

| Tag | Torch | Size | When to use |
|---|---|---|---|
| `igoricda/alpr:latest` (= `:gpu`) | CUDA (the default of PyPI, about 6 GB of `nvidia-*` packages) | about 12 GB | a server **with** a GPU |
| `igoricda/alpr:cpu` (`Dockerfile.cpu`) | the CPU build of PyTorch | about 2.5 GB | a server **without** a GPU (a much smaller pull, with the same output) |

The `:latest` tag is the GPU variant: with a visible GPU the Torch runtime
uses CUDA and without one it falls back to the CPU automatically. On a host
with the NVIDIA Container Toolkit, register the function with `"gpu": true`
(see below) to inject the driver into the container; without it the runtime
only sees the CPU even when the host has a GPU.

## Model

`best.pt` — YOLO26n-OBB, 5.9 MB, with the single class `License-plate`,
through the ultralytics runtime and the CUDA backend of torch. The
detection runs with an image size of 1280, which was validated with a
precision of 1.0, a recall of 1.0, a mean average precision at the
threshold of 0.5 of 0.995 and of 0.989 at the threshold of 0.95.

## Readiness contract

The `index.py` implements **bind first and lazy load**, the same pattern as
the other examples:

- The HTTP server binds `127.0.0.1:$PORT` **before** importing
  `function.handler`, where the model is loaded once.
- A daemon thread loads `handle()` and signals the `threading.Event` named
  `_ready`.
- **GET `/_/ready`**: `200 {"ready": true}` when ready and `503` otherwise.
- **POST `/_/ready`**: **always `404`**.
- **POST `/`**: `503` while loading and `200` with a valid JSON body after
  the ready event.
- **Unknown GET**: `404`.

## Structure

```
function/handler.py   # handle(req): the model is loaded once and called on every invocation
index.py              # the minimal HTTP server expected by the watchdog
function/requirements.txt  # ultralytics==8.4.153 + numpy (torch and CUDA come with the dependency)
best.pt               # the weights of the model (see "Prepare" below)
Dockerfile            # the final amd64 image, with the watchdog as the main process
```

## Prepare

```bash
# from the root of the repository
curl -L -o examples/alpr/best.pt \
  https://github.com/igoricda/alpr-yolo-models/raw/refs/heads/main/musgd_def/weights/best.pt

# build the watchdog for the server (amd64)
GOOS=linux GOARCH=amd64 go build -o examples/alpr/watchdog-lite ./cmd/watchdog-lite
```

## Build and test with Docker

```bash
cd examples/alpr
docker build --platform linux/amd64 -t igoricda/alpr:gpu .

# with a GPU (the runtime of the image already comes with CUDA; without the
# flag it falls back to the CPU):
docker run --rm -d --name alpr-test --gpus all \
  -v /tmp/alpr-sockets:/run/faas-continuum-socket \
  faas-continuum/alpr:latest

# build the payload (the image in base64) and invoke it over the mounted
# Unix socket
python3 -c "
import base64, json
b64 = base64.b64encode(open('/path/to/a/photo/of/a/plate.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json

curl -s --unix-socket /tmp/alpr-sockets/handle.sock http://localhost/ -d @/tmp/payload.json
# -> {"status": "success", "detections": [{"class": "License-plate",
#      "confidence": 0.9, "obb": {"x": .., "y": .., "w": .., "h": ..,
#      "rotation_rad": ..}}]}
```

Invocation through the node agent: the node expects the envelope
`{"input": ...}` and forwards only the content of `input` to the function.
Without the envelope, `handle()` receives an empty body and answers with
`{"status": "error", ...}`.

```bash
python3 -c "
import base64, json
b64 = base64.b64encode(open('/path/to/a/photo/of/a/plate.jpg','rb').read()).decode()
print(json.dumps({'input': {'image_base64': b64}}))
" > /tmp/invoke.json

curl -s -X POST "$BASE_AGENTE/function/alpr/invoke" \
  -H "Authorization: $INVOKE_TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/invoke.json
```

The confidence threshold and the inference size come from environment
variables (`CONFIDENCE_THRESHOLD=0.25` and `IMAGE_SIZE=1280`, which is the
validated one).

## Registration on the node agent (timeout per function)

The cold import of torch and of the ultralytics runtime takes 30 to 60
seconds, which fits in the default ceiling of 90 seconds of the node, but
that default leaves no margin against the `READY_TIMEOUT=90s` of this
Dockerfile. Register the function with a ceiling of its own of 120 seconds,
which leaves a margin of 30 seconds, the larger of ten seconds and twenty
percent of the ceiling:

```bash
curl -i -X POST "$BASE_AGENTE/function/alpr" \
  -H "Authorization: $ADMIN_TOKEN" -H "Content-Type: application/json" -d '{
    "image_ref": "faas-continuum/alpr:latest",
    "min_instances": 1,
    "timeout_ms": 120000
  }'
# 201 -> {"function":{...},"invoke_token":"..."}  KEEP THE INVOKE TOKEN!
```

The GPU variant, on a host with the NVIDIA driver and the Container
Toolkit installed, points to the `:gpu` or `:latest` tag and passes
`"gpu": true`. The agent injects the prestart hook of NVIDIA and the
variable that exposes every device to the container; the default is CPU
only:

```bash
curl -i -X POST "$BASE_AGENTE/function/alpr-gpu" \
  -H "Authorization: $ADMIN_TOKEN" -H "Content-Type: application/json" -d '{
    "image_ref": "igoricda/alpr:gpu",
    "min_instances": 1,
    "timeout_ms": 120000,
    "gpu": true
  }'
```

The variant without a GPU, with a pull of about one gigabyte instead of
about six, is `igoricda/alpr:cpu` without the `"gpu"` field.
