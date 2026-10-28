# Example: text recognition function (reads the license plate)

The second stage of the license plate pipeline. It receives the **crop of
the plate**, since the perspective warp of the oriented box happens in the
middle of the pipeline and not here, and it returns the text of the plate
as JSON.

## Model

A **multi-architecture** image, for amd64 and arm64, with the **same**
model: the small variant of the recognition model, fine tuned and exported
to ONNX, 3.2 MB, run through the ONNX runtime. It reaches the accuracy of
the larger variant, with a plate accuracy of 0.9864 against 0.9868 and a
character accuracy of 0.9985, with a much smaller runtime, about four times
smaller on x86 and about eleven times smaller on ARM.

The input of the model is **RGB and not BGR**, with the channels last and
the values as bytes, and the normalization by 255 is inside the graph. The
outputs are the plate, with ten slots, and the region, decoded with the
alphabet of the digits and the uppercase letters and the underscore, with
the underscore as the padding.

The minimal stack is the ONNX runtime, numpy and the headless build of
opencv, plus the `fast-plate-ocr` package, which pre-processes and
post-processes exactly as the training did, and pillow. There is no torch,
no tensorflow and no ultralytics.

## Readiness contract

The `index.py` implements **bind first and lazy load**, the same pattern as
the other examples:

- The HTTP server binds `127.0.0.1:$PORT` **before** importing
  `function.handler`, where the recognizer is loaded once.
- A daemon thread loads `handle()` and signals the `threading.Event` named
  `_ready`.
- **GET `/_/ready`**: `200 {"ready": true}` when ready and `503` otherwise.
- **POST `/_/ready`**: **always `404`**.
- **POST `/`**: `503` while loading and `200` with a valid JSON body after
  the ready event.
- **Unknown GET**: `404`.

## Structure

```
function/handler.py   # handle(req): the recognizer is loaded once and called on every invocation
index.py              # the minimal HTTP server expected by the watchdog
function/requirements.txt  # fast-plate-ocr + onnxruntime + opencv-headless + numpy + pillow
best.onnx             # the weights of the fine tuned small model (see "Prepare" below)
cct_xs_v2_global_plate_config.yaml  # the configuration of the plate, with the alphabet, the slots and the regions
Dockerfile            # the final multi-architecture image, with the watchdog as the main process
```

## Prepare

```bash
# from the root of the repository
cp <path>/exported_xs/best.onnx examples/ocr/best.onnx
cp <path>/config/cct_xs_v2_global_plate_config.yaml \
  examples/ocr/cct_xs_v2_global_plate_config.yaml

# build the watchdog for both architectures (the multi-architecture
# Dockerfile copies the watchdog of the target architecture)
GOOS=linux GOARCH=amd64 go build -o examples/ocr/watchdog-lite-amd64 ./cmd/watchdog-lite
GOOS=linux GOARCH=arm64 go build -o examples/ocr/watchdog-lite-arm64 ./cmd/watchdog-lite
```

## Build and test with Docker

```bash
cd examples/ocr
docker buildx build --platform linux/amd64,linux/arm64 \
  -t faas-continuum/ocr:latest .
docker run --rm -d --name ocr-test \
  -v /tmp/ocr-sockets:/run/faas-continuum-socket \
  faas-continuum/ocr:latest
```

## Publish

The published tag is `docker.io/igoricda/ocr:latest`, with a manifest for
amd64 and arm64. A build through the emulation of another architecture is
slow, so the flow is a native build per architecture followed by the
manifest:

```bash
# amd64, on the server: build the architecture and push it
docker build --platform linux/amd64 -t igoricda/ocr:amd64 -f examples/ocr/Dockerfile examples/ocr
docker push igoricda/ocr:amd64
# arm64, on the ARM host, with a context that carries the arm64 watchdog:
# the same, with the tag :arm64
# a single manifest that combines both architectures:
docker buildx imagetools create -t igoricda/ocr:latest \
  igoricda/ocr:amd64 docker.io/igoricda/ocr:arm64
```

```bash
# build the payload (the crop of the plate in base64) and invoke it over
# the mounted Unix socket
python3 -c "
import base64, json
b64 = base64.b64encode(open('/path/to/the/crop/of/the/plate.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json

curl -s --unix-socket /tmp/ocr-sockets/handle.sock http://localhost/ -d @/tmp/payload.json
# -> {"status": "success", "plate": "ABC1D23", "region": "Brazil"}
```

Invocation through the node agent: the node expects the envelope
`{"input": ...}` and forwards only the content of `input` to the function.
Without the envelope, `handle()` receives an empty body and answers with
`{"status": "error", ...}`.

```bash
python3 -c "
import base64, json
b64 = base64.b64encode(open('/path/to/the/crop/of/the/plate.jpg','rb').read()).decode()
print(json.dumps({'input': {'image_base64': b64}}))
" > /tmp/invoke.json

curl -s -X POST "$BASE_AGENTE/function/ocr/invoke" \
  -H "Authorization: $INVOKE_TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/invoke.json
```

The GPU variant, on a server with CUDA, does not require any change in the
requirements:

```bash
docker buildx build --build-arg ONNXRUNTIME_PKG=onnxruntime-gpu \
  -t faas-continuum/ocr:gpu -f examples/ocr/Dockerfile examples/ocr
```

On ARM, the number of cores of the runtime comes from `ORT_THREADS`, whose
default of zero means the default of the runtime; four is the validated
value:

```bash
docker run --rm -d --name ocr-test -e ORT_THREADS=4 \
  -v /tmp/ocr-sockets:/run/faas-continuum-socket \
  faas-continuum/ocr:latest
```

## Registration on the node agent (timeout per function)

The cold import here, of the image library, of the runtime and of the
recognition package, takes a few seconds, and the default ceiling of 90
seconds of the node clears the `READY_TIMEOUT=90s` of the image with
margin, so the default is enough:

```bash
curl -i -X POST "$BASE_AGENTE/function/ocr" \
  -H "Authorization: $ADMIN_TOKEN" -H "Content-Type: application/json" -d '{
    "image_ref": "faas-continuum/ocr:latest",
    "min_instances": 1
  }'
# 201 -> {"function":{...},"invoke_token":"..."}  KEEP THE INVOKE TOKEN!
```
