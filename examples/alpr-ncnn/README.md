# Example: edge license plate detection function (YOLO26-OBB through ncnn)

The first stage of the license plate pipeline at the edge. It receives the
image of the scene and returns the coordinates of the plates as JSON. It is
the edge counterpart of the [server example](../alpr), with the same input
and the same output and only a different backend for the detector, ncnn
instead of torch with CUDA.

## Model

`best_ncnn_model/`, with the weights and the parameters of the network, 4.8
MB and 28 KB. They are the same weights of the checkpoint of the server
example, exported to ncnn with an image size of 640. On ARM, with four
Cortex-A55 cores and four threads, the mean average precision at the
threshold of 0.5 is 0.995 and at the threshold of 0.95 it is 0.984, against
0.989 for the export with an image size of 1280. The inference takes about
222 ms per image in the validation and about 270 ms per invocation, which is
about four times faster than the export with an image size of 1280 and about
eleven times faster than the CPU backend of torch on the same ARM host.

Out of scope on purpose: the TFLite backend, which reaches a mean average
precision of zero, and the text recognition through ncnn, which collapses
the axis of the tokens. The text recognition at the edge uses the ONNX
runtime, in the [`ocr`](../ocr) example.

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
function/requirements.txt  # ultralytics==8.4.153 + ncnn + numpy<2 (torch with the CPU build comes from the Dockerfile)
best_ncnn_model/      # the weights and the parameters of the network (see "Prepare" below)
Dockerfile            # the final arm64 only image, with the watchdog as the main process
```

## Prepare

```bash
# from the root of the repository — the export freezes the input shape at
# 640, so the image size of the handler and of the Dockerfile has to match
# the image size of the export:
yolo export task=obb model=runs/obb/musgd_def/weights/best.pt \
  format=ncnn imgsz=640 quantize=16
mkdir -p examples/alpr-ncnn/best_ncnn_model
cp runs/obb/musgd_def_640/weights/best_ncnn_model/model.ncnn.bin \
   runs/obb/musgd_def_640/weights/best_ncnn_model/model.ncnn.param \
   runs/obb/musgd_def_640/weights/best_ncnn_model/metadata.yaml \
   examples/alpr-ncnn/best_ncnn_model/
# The metadata file, with the task, the class and the image size, is
# MANDATORY next to the weights and the parameters: without it the
# ultralytics runtime loads a thousand generic classes and the detection
# reaches zero.

# build the watchdog for the ARM host (arm64)
GOOS=linux GOARCH=arm64 go build -o examples/alpr-ncnn/watchdog-lite ./cmd/watchdog-lite
```

## Build and test with Docker

```bash
cd examples/alpr-ncnn
docker buildx build --platform linux/arm64 -t faas-continuum/alpr-ncnn:latest .
docker run --rm -d --name alpr-ncnn-test \
  -v /tmp/alpr-ncnn-sockets:/run/faas-continuum-socket \
  faas-continuum/alpr-ncnn:latest
```

## Publish

The published tags are `docker.io/igoricda/alpr-ncnn:640` and `:latest`,
which point to the same digest. The build has to run **natively on the ARM
host**, since a cross build through the emulation of another architecture
is slow, and the context has to contain the arm64 watchdog, built with
`GOOS=linux GOARCH=arm64 go build -o examples/alpr-ncnn/watchdog-lite
./cmd/watchdog-lite`.

```bash
# build the payload (the image in base64) and invoke it over the mounted
# Unix socket
python3 -c "
import base64, json
b64 = base64.b64encode(open('/path/to/a/photo/of/a/plate.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json

curl -s --unix-socket /tmp/alpr-ncnn-sockets/handle.sock http://localhost/ -d @/tmp/payload.json
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

curl -s -X POST "$BASE_AGENTE/function/alpr-ncnn/invoke" \
  -H "Authorization: $INVOKE_TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/invoke.json
```

The confidence threshold and the inference size come from environment
variables (`CONFIDENCE_THRESHOLD=0.25` and `IMAGE_SIZE=640`, which is the
validated one and matches the image size of the export).

## Registration on the node agent (timeout per function)

The cold import on a slow ARM host, together with the `READY_TIMEOUT=90s`
of the image, calls for a ceiling of its own of 120 seconds, with the same
margin of thirty seconds of the server example. The inference through ncnn
with an image size of 640 takes about 270 ms per image on the ARM host, so
the ceiling below covers it with room to spare for isolated invocations:

```bash
curl -i -X POST "$BASE_AGENTE/function/alpr-ncnn" \
  -H "Authorization: $ADMIN_TOKEN" -H "Content-Type: application/json" -d '{
    "image_ref": "faas-continuum/alpr-ncnn:latest",
    "min_instances": 1,
    "timeout_ms": 120000
  }'
# 201 -> {"function":{...},"invoke_token":"..."}  KEEP THE INVOKE TOKEN!
```
