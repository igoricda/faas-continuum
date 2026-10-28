"""ALPR function, which is license plate detection with the ncnn backend.

It is the first stage of the license plate pipeline at the edge, on an ARM
aarch64 processor: it receives the image of the scene and returns the
coordinates of the plates. It uses the same training as the server,
exported to ncnn with an image size of 640, which is the directory
`best_ncnn_model` with the weights and the parameters of the network, of
4.8 MB and 28 KB, through the ultralytics runtime and the ncnn backend. It
is the edge counterpart of the server example: the input and the JSON output
are the same, only the backend of the detector changes.

Measured on an ARM processor with the same weights, an input of 640 and four
threads: a mean average precision at the threshold of 0.5 of 0.995 and at
the threshold of 0.95 of 0.984, and an inference of about 222 ms on four
Cortex-A55 cores, which is about four times faster than the export with an
image size of 1280 and about eleven times faster than the CPU backend of
torch.

The model is loaded once, at the import of the module, which is what the
watchdog exploits by keeping the process alive between invocations. The
handler is called repeatedly by the HTTP process on every request received.

Input and output contract:

    input:   {"image_base64": "<jpg or png in base64>"}
    output:  {"status": "success", "detections": [...]}
             or {"status": "error", "message": "..."}
"""
import base64
import binascii
import json
import os

os.environ["YOLO_CONFIG_DIR"] = "/tmp/Ultralytics"
os.environ["YOLO_VERBOSE"] = "False"

from ultralytics import YOLO  # noqa: E402

MODEL_PATH = os.environ.get("MODEL_PATH", "./best_ncnn_model")
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
# The image size of 640 is the validated one on ARM, with four threads and
# an inference of about 222 ms, which is about four times faster than 1280
# with the same mean average precision. The shape is frozen in the ncnn
# export, so the value here only has to match the one of the export.
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "640"))

# Loaded only once, which is what the pool of minimal instances depends on:
# the cold start of loading the model cannot happen on every invocation. The
# ncnn directory is detected by the ncnn backend of the ultralytics runtime,
# which exposes the same API as the checkpoint.
model = YOLO(MODEL_PATH)


def handle(req: str) -> str:
    try:
        data = json.loads(req)
        image_bytes = base64.b64decode(data["image_base64"], validate=True)
    except (KeyError, ValueError, TypeError, binascii.Error) as exc:
        return json.dumps({"status": "error", "message": f"invalid input: {exc}"})

    tmp_path = "/tmp/invocation_input.jpg"
    with open(tmp_path, "wb") as f:
        f.write(image_bytes)

    try:
        results = model(tmp_path, conf=CONFIDENCE_THRESHOLD, imgsz=IMAGE_SIZE, verbose=False)
        if not results:
            return json.dumps({"status": "error", "message": "the model returned no result, the image is invalid or corrupted"})
        result = results[0]
    except Exception as exc:  # The inference can fail on a corrupted image.
        return json.dumps({"status": "error", "message": f"inference failed: {exc}"})

    detections = []
    if result.obb is not None:
        for box in result.obb:
            x, y, w, h, r = box.xywhr[0].tolist()
            detections.append({
                "class": model.names[int(box.cls)],
                "confidence": round(float(box.conf), 4),
                "obb": {"x": x, "y": y, "w": w, "h": h, "rotation_rad": r},
            })

    return json.dumps({"status": "success", "detections": detections})
