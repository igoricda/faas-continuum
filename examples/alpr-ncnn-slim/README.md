# Example: slim edge license plate detection function (direct ncnn, without torch)

The slim counterpart of the [edge example](../alpr-ncnn): the same input,
the same output and the same model, the directory `best_ncnn_model`
exported with an image size of 640, but **without torch and without
ultralytics**, with only ncnn, numpy and the headless build of opencv, as
declared in `function/requirements.txt`.

## Why it exists

On an ARM board with an eMMC storage the edge example pays about 63 seconds
of cold start: about 31 seconds to import the stack of torch and of the
ultralytics runtime and about 32 seconds of page-in on the first forward.
Torch is dead weight there, since the inference runs through ncnn while the
ultralytics runtime imports torch unconditionally. The ncnn export already
carries the decoding inside the graph, with the anchors, the strides, the
sigmoid and the cosine and sine in the tail of the parameter file, so the
handler replicates the path with numpy: the letterbox, with the rounding of
the borders by one tenth, the inference, the confidence filter, the fast
suppression with the probabilistic intersection over union and the rescaling
back to the original coordinates. The projected cold start falls to about
ten seconds.

## Parity

The slim handler and the handler built on the ultralytics runtime, with the
same weights and the same images, produce **no difference** in the class,
in the confidence and in the five fields of the oriented box, over three
real test images, five synthetic images that cover the wide, the tall and
the padded letterbox and one collage with four detections kept by the
suppression. The warm path is equivalent on x86, with 86 ms against 103 ms,
since the suppression in numpy is about twenty milliseconds slower, which is
irrelevant next to the about 300 ms of the warm path on ARM. The import is
5.8 times faster even with a warm page cache, and the environment is 348 MB
against 1.6 GB.

## Readiness contract

The `index.py` implements **bind first and lazy load**, the same pattern as
the other examples, and it also warms up at boot: a dummy inference runs
before the ready event is signaled, it fails open and the variable
`SKIP_WARMUP=1` disables it. The status codes are the same: `GET /_/ready`
answers with 200 or 503, `POST /_/ready` always answers with 404 and
`POST /` answers with 503 until the instance is ready.

## Structure

```
function/handler.py       # the pipeline with ncnn and numpy, without torch
function/requirements.txt # ncnn + numpy + opencv-headless
index.py                  # the same contract as the edge example, with the warmup
best_ncnn_model/          # the copy of the trio of the edge example, the weights, the parameters and the metadata
Dockerfile                # arm64 only
```

## Build and publish

```bash
# from the root of the repository:
GOOS=linux GOARCH=arm64 go build -o examples/alpr-ncnn-slim/watchdog-lite ./cmd/watchdog-lite
cp examples/alpr-ncnn/best_ncnn_model/* examples/alpr-ncnn-slim/best_ncnn_model/
docker buildx build --platform linux/arm64 \
  -t docker.io/igoricda/alpr-ncnn-slim:latest \
  -f examples/alpr-ncnn-slim/Dockerfile examples/alpr-ncnn-slim --push
```

The build should run **natively on the ARM host**, since a cross build
through the emulation of another architecture is slow.

## Registration on the node agent

With a projected cold start of about ten seconds, a ceiling of its own of 60
seconds leaves a margin of about six times, and the image carries a
`READY_TIMEOUT=60s`. It has to be recalibrated after the first measurement:

```bash
curl -i -X POST "$BASE_AGENTE/function/alpr-ncnn-slim" \
  -H "Authorization: $ADMIN_TOKEN" -H "Content-Type: application/json" -d '{
    "image_ref": "docker.io/igoricda/alpr-ncnn-slim:latest",
    "min_instances": 0,
    "timeout_ms": 60000
  }'
```

The invocation uses the same envelope `{"input": {"image_base64": ...}}` as
the edge example. The extra knobs are `IOU_THRESHOLD=0.7` and `MAX_DET=300`,
which are the defaults of the ultralytics runtime, while
`CONFIDENCE_THRESHOLD=0.25` and `IMAGE_SIZE=640` are the same as in the
original.
