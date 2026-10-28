# Example: YOLO26n COCO detection function (default model, amd64 server)

Generic object detection. It **receives the image and returns the annotated
image**, in base64, together with the detections as JSON. It is the amd64
counterpart of the [slim edge example](../yolo26n-coco-ncnn-slim): the same
input, the same output and the same weights, with only a different backend
for the detector.

Unlike the license plate examples, there is **no** fine tuning and **no**
private dataset here: the model is the nano variant of YOLO26 with the
default weights of the ultralytics runtime, pre-trained on COCO, with
eighty classes and the detection task. It serves as a canary function to
measure the framework with an inference that everyone recognizes.

## Model

`yolo26n.pt` — YOLO26 **nano**, pre-trained on **COCO**, the default of the
ultralytics runtime, which the runtime itself downloads on the first load.

| Property | Value |
|---|---|
| Format | `.pt`, with the PyTorch format, 5,544,453 bytes |
| Task and head | `detect` and `Detect`, with the output `[cx, cy, w, h, 80 scores]` |
| Classes | 80, from COCO, exposed by `model.names` |
| Parameters | 2.57 M, in 454 modules |
| `stride` | `[8, 16, 32]`, which gives 8400 anchors at 640 by 640 |
| Default `imgsz` | 640 |
| Runtime | the ultralytics runtime with torch, and CUDA through the default of PyPI |

**No validation of the accuracy was run for this example**: what it
validates is the *parity between the backends*, as in the table below and
not the mean average precision. For the accuracy of the model, consult the
sheet of the ultralytics runtime or run a validation with the dataset of
COCO in the evaluation environment.

## Input and output contract

```
input:   {"image_base64": "<jpg or png in base64>"}
output:  {"status": "success",
          "image_base64": "<annotated jpg in base64>",
          "image_format": "jpeg",
          "image_size": {"width": 810, "height": 1080},
          "detections": [{"class": "bus", "confidence": 0.9031,
                          "box": {"x1":.., "y1":.., "x2":.., "y2":..}}, ...],
          "inference_ms": 16.0}
         or {"status": "error", "message": "..."}
```

- The coordinates are in pixels of the **input image**, the same coordinate
  system as the returned annotated image.
- The encoded image disappears when the annotation is disabled, which turns
  the answer into about 590 bytes instead of about 313 kilobytes.
- `inference_ms` covers the inference only, which is the pre-processing,
  the forward pass and the suppression. The encoding of the image, the
  encoding in base64 and the JSON are outside of it.

A real answer, with the encoded image abbreviated:

```json
{"status": "success",
 "image_base64": "/9j/4AAQSkZJRgABAQAAAQABAAD...",
 "image_format": "jpeg",
 "image_size": {"width": 810, "height": 1080},
 "detections": [
   {"class": "bus", "confidence": 0.903, "box": {"x1": 9.72, "y1": 230.64, "x2": 801.06, "y2": 748.01}},
   {"class": "person", "confidence": 0.8548, "box": {"x1": 47.04, "y1": 397.75, "x2": 237.68, "y2": 901.59}},
   {"class": "person", "confidence": 0.845, "box": {"x1": 223.57, "y1": 407.0, "x2": 345.86, "y2": 861.3}},
   {"class": "person", "confidence": 0.8373, "box": {"x1": 669.09, "y1": 387.31, "x2": 810.0, "y2": 880.1}},
   {"class": "person", "confidence": 0.5567, "box": {"x1": 0.0, "y1": 552.99, "x2": 59.66, "y2": 872.6}}],
 "inference_ms": 46.4}
```

### Attention to the ceiling of eight mebibytes of the node

The ceiling of the request body and of the answer of the function is eight
mebibytes. An annotated photograph in 4K overflows it once it is encoded in
base64. Use `ANNOTATE_MAX_SIDE=1280` for large inputs: with an image of
3840 by 2160 the annotated JPEG falls from 596,141 bytes to 102,058 bytes,
and the coordinates follow the new scale. With the annotation disabled the
problem disappears.

## The `rect=False` option, the only deviation from the default

The prediction command of the runtime uses the smallest rectangle, which is
faster. The ncnn export of the slim counterpart is **static at 640 by 640**,
so that the two examples infer at the same shape the handler disables the
rectangular inference and forces the square with a padding of 114.

Measured over two images, with the rectangular inference against the square
one on the same checkpoint:

| image | class | confidence with the rectangle | confidence with the square | difference |
|---|---|---|---|---|
| a bus photo | bus | 0.8806 | 0.9031 | +0.023 |
| a bus photo | person, the second one | 0.8608 | 0.8450 | −0.016 |
| a portrait photo | person, the second one | 0.8533 | 0.7927 | −0.061 |

The same class, the same count and a **confidence that differs by about one
hundredth**, which is why both handlers of the pair run with the square
letterbox and why the README of the slim counterpart measures the parity
with the same pre-processing.

## Parity with the slim ncnn counterpart

Same host, same inputs, with a confidence of 0.25, an image size of 640 and
the square letterbox:

| Set | Images | Detections | Same count | Same classes | Maximum difference of confidence | Maximum difference of box |
|---|---|---|---|---|---|---|
| Real photographs, at 1280 by 720 | 20 | 40 | 20/20 | 20/20 | 0.0016 | 0.12 px |
| Synthetic regression, with a black image, noise, a single pixel, 4K, a portrait, a panorama, a collage, an image with an alpha channel, a grayscale image and a JPEG with the lowest quality | 15 | 26 | 15/15 | 15/15 | 0.0036 | 1.94 px, on the 4K image |

The annotated image comes out **byte identical** in 15 of the 20 real
photographs and in 6 of the 15 synthetic ones. In the remaining cases the
difference is the border of a box displaced by one pixel by the rounding,
with a delta of at most 200 bytes in the JPEG. The same set of invalid
inputs, with a broken JSON, a missing key, an invalid base64, bytes that
are not an image and an empty payload, produces the same error answer in
both.

The difference of latency on this x86 host, with the median of five warm
runs over the bus photograph, is **20 to 24 ms** of wall time against **50
to 60 ms** for the slim ncnn variant. On a desktop the torch runtime wins,
since it uses the vectorized instructions of the processor; the advantage
of the slim variant is the **cold start and the size of the image**, not
the throughput.

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
yolo26n.pt            # the default weights of COCO (see "Prepare" below)
Dockerfile            # the final amd64 image, with the watchdog as the main process
Dockerfile.cpu        # the variant with the CPU build of torch, without a GPU
```

## Prepare

```bash
# from the root of the repository
curl -L -o examples/yolo26n-coco/yolo26n.pt \
  https://github.com/ultralytics/assets/releases/download/v8.4.0/yolo26n.pt

# build the watchdog for the server (amd64)
GOOS=linux GOARCH=amd64 go build -o examples/yolo26n-coco/watchdog-lite ./cmd/watchdog-lite
```

## Build and test with Docker

```bash
cd examples/yolo26n-coco
docker build --platform linux/amd64 -t igoricda/yolo26n-coco:gpu .   # with CUDA
docker build --platform linux/amd64 -t igoricda/yolo26n-coco:cpu \
  -f Dockerfile.cpu .                                                # with the CPU build
```

The bulk of the image is the torch runtime, and the layer that changes the
ownership of the home directory duplicates it and is worth about one
gigabyte of the image:

| Tag | Torch | Home directory of the application | Image |
|---|---|---|---|
| `yolo26n-coco:cpu`, with `Dockerfile.cpu` | the CPU build | 1.1 GB | **2.46 GB** |
| `yolo26n-coco:gpu`, with the `Dockerfile` | CUDA, the default of PyPI | — | about 6.5 GB |

The `:latest` and `:gpu` tags are the variant with CUDA: with a visible GPU
the torch runtime uses CUDA and without one it falls back to the CPU
automatically. On a host with the NVIDIA Container Toolkit, register the
function with `"gpu": true`, as shown below, to inject the driver.

```bash
# the process of the function runs as the unprivileged user of the image,
# so the mounted directory of the socket has to allow writing, otherwise
# the watchdog dies with a permission denied when it binds the socket.
rm -rf /tmp/yolo-sockets && mkdir -p /tmp/yolo-sockets && chmod 777 /tmp/yolo-sockets
docker run --rm -d --name yolo-test \
  -v /tmp/yolo-sockets:/run/faas-continuum-socket \
  igoricda/yolo26n-coco:cpu

python3 -c "
import base64, json
b64 = base64.b64encode(open('/path/to/a/photo.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json

curl -s --unix-socket /tmp/yolo-sockets/handle.sock http://localhost/ -d @/tmp/payload.json \
  | python3 -m json.tool | head
# -> {"status": "success", "image_base64": "/9j/4AAQ...", "image_format": "jpeg",
#      "image_size": {...}, "detections": [{"class": "bus", "confidence": 0.903, ...}],
#      "inference_ms": 46.4}
```

The node returns the encoded image of the answer, so the image can be
recovered with:

```bash
curl -s --unix-socket /tmp/yolo-sockets/handle.sock http://localhost/ -d @/tmp/payload.json \
  | python3 -c "
import base64, json, sys
r = json.load(sys.stdin)
open('/tmp/annotated.jpg','wb').write(base64.b64decode(r['image_base64']))
print(r['detections'])
"
```

Invocation through the node agent: the node expects the envelope
`{"input": ...}` and forwards only the content of `input` to the function.
Without the envelope, `handle()` receives an empty body and answers with
`{"status": "error", ...}`.

```bash
python3 -c "
import base64, json
b64 = base64.b64encode(open('/path/to/a/photo.jpg','rb').read()).decode()
print(json.dumps({'input': {'image_base64': b64}}))
" > /tmp/invoke.json

curl -s -X POST "$BASE_AGENTE/function/yolo26n-coco/invoke" \
  -H "Authorization: $INVOKE_TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/invoke.json
```

## Knobs

| Variable | Default | Effect |
|---|---|---|
| `MODEL_PATH` | the weights inside the image | the weights, any checkpoint of the ultralytics runtime |
| `CONFIDENCE_THRESHOLD` | `0.25` | the confidence threshold |
| `IOU_THRESHOLD` | `0.7` | the intersection over union used by the suppression |
| `MAX_DET` | `300` | the maximum number of detections |
| `IMAGE_SIZE` | `640` | the image size of the inference; it has to match the ncnn export of the slim counterpart |
| `ANNOTATE` | `1` | with zero the answer carries only the detections, about 590 bytes |
| `JPEG_QUALITY` | `85` | the quality of the annotated image, which drives the size of the payload |
| `ANNOTATE_MAX_SIDE` | `0` | with a positive value the annotated image is reduced, and the coordinates follow it, to fit the ceiling of eight mebibytes |

## Registration on the node agent (timeout per function)

The cold import of torch and of the ultralytics runtime takes 30 to 60
seconds, which fits in the default ceiling of 90 seconds of the node, but
that default leaves no margin against the `READY_TIMEOUT=90s` of this
Dockerfile. Register the function with a ceiling of its own of 120 seconds,
which leaves a margin of 30 seconds, the larger of ten seconds and twenty
percent of the ceiling:

```bash
curl -i -X POST "$BASE_AGENTE/function/yolo26n-coco" \
  -H "Authorization: $ADMIN_TOKEN" -H "Content-Type: application/json" -d '{
    "image_ref": "docker.io/igoricda/yolo26n-coco:cpu",
    "min_instances": 1,
    "timeout_ms": 120000
  }'
# 201 -> {"function":{...},"invoke_token":"..."}  KEEP THE INVOKE TOKEN!
```

The GPU variant, on a host with the NVIDIA driver and the Container
Toolkit installed, points to the `:gpu` tag and passes `"gpu": true`. The
agent injects the prestart hook of NVIDIA and the variable that exposes
every device to the container; the default is CPU only:

```bash
curl -i -X POST "$BASE_AGENTE/function/yolo26n-coco-gpu" \
  -H "Authorization: $ADMIN_TOKEN" -H "Content-Type: application/json" -d '{
    "image_ref": "docker.io/igoricda/yolo26n-coco:gpu",
    "min_instances": 1,
    "timeout_ms": 120000,
    "gpu": true
  }'
```

## Measured latency

With an 810 by 1080 photograph, five detections and the annotation enabled:

| Path | Number of runs | Time |
|---|---|---|
| The local handler, warm | median of five | **20 to 24 ms**, with an inference of 12 to 16 ms |
| Over the Unix socket inside the container | three invocations | 59 to 91 ms of wall time |
| The first invocation, warming up the vectorized instructions | one | 3.8 s |

The gap between the first two rows is the cost of the edge: the base64 of a
313 KB answer, the JSON, the socket and the proxy, plus the load of the
host. To compare the pure inference latency, always look at
`inference_ms`.

## What was verified

| Check | Result |
|---|---|
| The compilation of both handlers and of the HTTP process | OK |
| The parity between the checkpoint and the slim ncnn variant over 35 images | the same count and the same classes in 35 of 35 |
| Invalid inputs, five cases | the same error answer in both |
| The annotation switch, the maximum side, the maximum number of detections and the confidence threshold | OK, the coordinates follow the scale |
| The HTTP process: the bind before the load, the transition from 503 to 200, the 404 on the readiness route and the 404 on an unknown route | OK |
| The container end to end, with the watchdog and the Unix socket | `GET /_/ready` with 200 and `POST /` with 200, with five detections and an encoded image of 312,192 bytes |
| The validation of the accuracy | **not run**, it is out of the scope of this example |
