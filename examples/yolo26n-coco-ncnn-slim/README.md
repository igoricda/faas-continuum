# Example: YOLO26n COCO detection function at the edge (slim ncnn, without torch)

The **arm64** counterpart of the [amd64 example](../yolo26n-coco): the same
input, the same JSON output and the same weights, the default checkpoint of
COCO exported to ncnn, but **without torch and without ultralytics**, with
only ncnn, numpy and the headless build of opencv, as declared in
`function/requirements.txt`, on the slim base image of Python 3.11.

## Model

`yolo26n_ncnn_model/` — the export of the default checkpoint of COCO to
ncnn with an image size of 640, with the optional quantization to sixteen
bits described in "Prepare":

| File | Size | Role |
|---|---|---|
| `model.ncnn.bin` | 9,736,936 bytes | the weights |
| `model.ncnn.param` | 26,404 bytes | the graph |
| `metadata.yaml` | 1,509 bytes | the provenance of the export, with the task, the image size and the eighty names; it is **not read at runtime** |
| `coco80.names`, at the root of the example | 621 bytes | the eighty names, as plain text |

The `metadata.yaml` stays in the image as a record of the export, for the
same reason as in the edge license plate example, but the handler **does
not** parse it: without a YAML parser in the runtime the eighty class names
come from `coco80.names`, a plain text file with eighty lines generated
from the checkpoint. The handler aborts with a clear message when the count
does not match the one of the model.

The output of the exported graph comes out as **eighty-four rows by eight
thousand four hundred columns**, with the center, the size and the scores
of the eighty classes, in pixels of the resized image: the decoding, with
the anchors, the strides and the sigmoid, is already at the end of the
parameter file. The handler only replicates what comes **after** the forward
pass.

## Why it exists

Torch is dead weight in the ncnn path, since the ultralytics runtime
imports it unconditionally even when the inference runs through ncnn.
Without the two of them the edge image falls to **811 MB** instead of
2.46 GB, the size of the amd64 counterpart with the CPU build of torch,
and the import of the handler falls from 1.2 s to 0.13 s on this host. On
ARM with an eMMC storage the difference is orders of magnitude greater.

The gain here is **not** the throughput: on this x86 host ncnn is about
2.4 times slower than the torch runtime with its vectorized instructions,
with 50 to 60 ms against 20 to 24 ms of wall time when warm. It is the size
of the image, the cold start and the memory. For the throughput on ARM, the
edge license plate example measured ncnn as 2.55 times faster than the CPU
backend of torch on the same host; **for this model nothing has been
measured on ARM yet**.

## Parity with the counterpart

Same host, same inputs, with a confidence of 0.25, an image size of 640
and the square letterbox, which is what the amd64 counterpart uses in
order to match the static shape of this export:

| Set | Images | Detections | Same count | Same classes | Maximum difference of confidence | Maximum difference of box |
|---|---|---|---|---|---|---|
| Real photographs, at 1280 by 720 | 20 | 40 | 20/20 | 20/20 | 0.0016 | 0.12 px |
| Synthetic regression, with a black image, noise, a single pixel, 4K, a portrait, a panorama, a collage, an image with an alpha channel, a grayscale image and a JPEG with the lowest quality | 15 | 26 | 15/15 | 15/15 | 0.0036 | 1.94 px, on the 4K image |
| The arm64 container against the amd64 container, over the same photograph | 1 | 5 | 1/1 | 1/1 | 0.0048 | 2.09 px |

The last row compares the **real images**, the two published ones: the same
five detections, the same classes and the same order, with the bus at 0.9030
on amd64 against 0.9038 on arm64. The difference is larger than the one on
x86 because of the aarch64 kernels of ncnn, which use the vector
instructions of the processor and a different order of accumulation. The
floating point behavior of the emulation is exact, so the number is the
real aarch64 one, even though the **time** measured through it does not
mean anything, as described in the section of the measured latency.

The **annotated image comes out byte identical** in 15 of the 20 real
photographs, with the same palette, the same thickness and the same JPEG,
since the annotation helper is a literal copy between the two handlers. In
the remaining five the difference is the border of a box displaced by one
pixel by the rounding, with a delta of at most 200 bytes in the JPEG.

The same set of invalid inputs, with a broken JSON, a missing key, an
invalid base64, bytes that are not an image and an empty payload, produces
the same error answer in both handlers.

## Input and output contract

Identical to the amd64 counterpart, whose complete contract is described
there. In summary:

```
input:   {"image_base64": "<jpg or png in base64>"}
output:  {"status": "success", "image_base64": "<annotated jpg>",
          "image_format": "jpeg", "image_size": {...},
          "detections": [{"class": ..., "confidence": ..., "box": {...}}, ...],
          "inference_ms": 42.0}
         or {"status": "error", "message": "..."}
```

## Readiness contract

The `index.py` implements **bind first and lazy load**, the same pattern as
the other examples, and it also warms up at boot: a dummy inference runs
before the ready event is signaled, it fails open and the variable
`SKIP_WARMUP=1` disables it. The status codes are the same: `GET /_/ready`
answers with 200 or 503, `POST /_/ready` always answers with 404 and
`POST /` answers with 503 until the instance is ready, while an unknown
route answers with 404.

During the readiness window the log of the container shows a broken pipe
error, traced by the server of the standard library, when the poll of the
watchdog receives the 503 and closes the connection. It is cosmetic and it
comes from the HTTP process that is shared with the other examples: the
watchdog keeps polling and the instance becomes ready.

## Structure

```
function/handler.py       # the pipeline with ncnn and numpy, without torch
function/requirements.txt # ncnn + numpy + opencv-headless
index.py                  # the same contract as the edge example, with the warmup
yolo26n_ncnn_model/       # the trio of the ncnn export, the weights, the parameters and the metadata
coco80.names              # the eighty class names, as plain text
Dockerfile                # arm64 only
```

## Prepare

```bash
# from the root of the repository. The export freezes the input shape at
# 640 by 640, so the image size of the handler and of the Dockerfile has to
# match the image size of the export.
yolo export model=yolo26n.pt format=ncnn imgsz=640
# or, for half the size of the weights with a small penalty in the
# accuracy, measured on x86 with the same classes and counts, a maximum
# difference of confidence of 0.0067, a maximum difference of box of 0.8 px
# and twelve of the twenty photographs byte identical:
#   yolo export model=yolo26n.pt format=ncnn imgsz=640 quantize=16

mkdir -p examples/yolo26n-coco-ncnn-slim/yolo26n_ncnn_model
cp yolo26n_ncnn_model/model.ncnn.bin \
   yolo26n_ncnn_model/model.ncnn.param \
   yolo26n_ncnn_model/metadata.yaml \
   examples/yolo26n-coco-ncnn-slim/yolo26n_ncnn_model/

# the class names come from the checkpoint, as plain text, since the
# handler has no YAML parser to read the metadata file:
python3 -c "
from ultralytics import YOLO
names = YOLO('yolo26n.pt').names
assert list(names) == list(range(80))
open('examples/yolo26n-coco-ncnn-slim/coco80.names','w').write(
    ''.join(names[i] + '\n' for i in range(80)))
"

# build the watchdog for the ARM host (arm64)
GOOS=linux GOARCH=arm64 go build -o examples/yolo26n-coco-ncnn-slim/watchdog-lite ./cmd/watchdog-lite
```

If the model is replaced, the file with the names has to be regenerated
together with it, since the handler aborts when the counts do not match.

The image size of 640 of the export is what binds the two examples
together: the parameter file does not record the input shape in a
declarative way, since the input of the graph has no shape, but the graph
is built for 8400 anchors and the metadata file records the image size.
Both handlers use an image size of 640 and the square letterbox because of
that. If the checkpoint used the smallest rectangle of the prediction
command and ncnn used the square one, the two would diverge in the
pre-processing, with a difference of confidence of up to 0.06, which was
measured and is described in the README of the amd64 counterpart.

## Build and test with Docker

```bash
cd examples/yolo26n-coco-ncnn-slim
docker buildx build --platform linux/arm64 -t faas-continuum/yolo26n-coco-ncnn-slim:latest .
```

The measured size of the image is 811 MB, and the home directory of the
application holds 207 MB with ncnn at 11 MB, numpy at 51 MB and the
headless build of opencv at 80 MB. The layer that changes the ownership of
the home directory duplicates about 200 MB.

```bash
# the process runs as the unprivileged user of the image, so the mounted
# directory of the socket has to allow writing, otherwise the watchdog
# dies with a permission denied when it binds the socket.
rm -rf /tmp/yolo-arm-sockets && mkdir -p /tmp/yolo-arm-sockets && chmod 777 /tmp/yolo-arm-sockets
docker run --rm -d --name yolo-arm \
  -v /tmp/yolo-arm-sockets:/run/faas-continuum-socket \
  faas-continuum/yolo26n-coco-ncnn-slim:latest

python3 -c "
import base64, json
b64 = base64.b64encode(open('/path/to/a/photo.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json
curl -s --unix-socket /tmp/yolo-arm-sockets/handle.sock http://localhost/ -d @/tmp/payload.json
```

## Publish

The published tag is
`docker.io/igoricda/yolo26n-coco-ncnn-slim:latest`. The build should run
**natively on the ARM host**, since a cross build through the emulation of
another architecture is slow, and the context has to contain the arm64
watchdog.

## Registration on the node agent (timeout per function)

The cold start of the slim variant takes a few seconds, since there is no
torch, so the `READY_TIMEOUT=60s` of the image leaves plenty of margin and
the timeout can be much tighter than in the amd64 counterpart, which still
pays 30 to 60 seconds of import. On an ARM host with Cortex-A55 cores,
start conservatively at 60 seconds and recalibrate after the first
measurement:

```bash
curl -i -X POST "$BASE_AGENTE/function/yolo26n-coco-ncnn-slim" \
  -H "Authorization: $ADMIN_TOKEN" -H "Content-Type: application/json" -d '{
    "image_ref": "docker.io/igoricda/yolo26n-coco-ncnn-slim:latest",
    "min_instances": 1,
    "timeout_ms": 60000
  }'
# 201 -> {"function":{...},"invoke_token":"..."}  KEEP THE INVOKE TOKEN!
```

The invocation uses the same envelope `{"input": {"image_base64": ...}}` as
the amd64 counterpart, since the node forwards only the input to the
handler.

## Knobs

The same ones as the amd64 counterpart, plus two on this side:

| Variable | Default | Effect |
|---|---|---|
| `MODEL_PATH` | the directory of the export inside the image | the ncnn export |
| `NAMES_PATH` | the file of the names inside the image | the class names, as plain text |
| `N_THREADS` | `0` | zero leaves the default of ncnn and a positive value pins the count, with four validated on ARM |
| `CONFIDENCE_THRESHOLD` | `0.25` | the confidence threshold |
| `IOU_THRESHOLD` | `0.7` | the intersection over union used by the suppression |
| `MAX_DET` | `300` | the maximum number of detections |
| `IMAGE_SIZE` | `640` | it has to match the image size of the export |
| `ANNOTATE` | `1` | with zero the answer carries only the detections |
| `JPEG_QUALITY` | `85` | the quality of the annotated image |
| `ANNOTATE_MAX_SIDE` | `0` | with a positive value the annotated image is reduced to fit the ceiling of eight mebibytes |

## Measured latency

- **x86, with the ncnn handler running locally:** 50 to 60 ms of wall time
  when warm, with an inference of 42 to 52 ms, the median of five runs over
  a photograph with five detections, and an import of the handler of 0.13 s.
- **The arm64 container under the emulation, which is functional but is
  not a number of ARM:** the invocation answers with a status of 200 and the
  five correct detections. The inference of 14.6 s is almost entirely the
  overhead of the emulation, since the aarch64 binary runs instruction by
  instruction. It serves to prove that the image starts and answers, not to
  dimension the ARM host.
- **ARM: nothing has been measured for this model yet.** The edge license
  plate example, with the same backend and an image size of 640, measured
  about 270 ms per warm invocation on an ARM host as an order of magnitude
  reference; this model has eighty classes instead of one, so the
  suppression costs a little more, with the same size of the network.

## What was verified

| Check | Result |
|---|---|
| The compilation of the handler and of the HTTP process | OK |
| The arm64 container end to end, under the emulation | `GET /_/ready` with 200, `POST /_/ready` with 404 and `POST /` with 200, with five detections and the same classes as the amd64 container |
| The parity against the counterpart, over 20 real photographs, 15 synthetic ones and the arm64 container | the same count and the same classes in 36 of 36 |
| The annotation switch | an answer of 587 bytes with the checkpoint and 584 bytes with the slim variant, without the encoded image |
| The build of the image | 811 MB, with 207 MB in the home directory of the application |

The `ncnn` package of the requirements pulls the full build of opencv, which
is not the headless one, together with a few other packages, as declared
dependencies. The handler only uses the network and the matrix types of the
compiled module, so the two extra modules are never imported. They stay in
the image for compatibility with the slim edge license plate example. A
possible optimization is to install ncnn without its dependencies, which
would cut about 64 MB of the libraries of the graphical stack and the
dependency on the OpenGL library; it was **not applied here** so that the
requirements do not diverge from the ones of the validated example.
