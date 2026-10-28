"""ALPR function, which is license plate detection with the full model.

It is the first stage of the license plate pipeline on the server or in the
cloud, on amd64 with a GPU: it receives the image of the scene and returns
the coordinates of the plates. It uses the YOLO26n-OBB model, which is
5.9 MB and has a single class, the license plate, through the ultralytics
runtime and the CUDA backend of torch.

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

MODEL_PATH = os.environ.get("MODEL_PATH", "./best.pt")
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
# The image size of 1280 is the validated one, with a precision of 1.0, a
# recall of 1.0 and a mean average precision at the threshold of 0.5 of
# 0.995 on the calibration set of 1280 by 720 images.
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "1280"))

# Loaded only once, which is what the pool of minimal instances depends on:
# the cold start of loading the model cannot happen on every invocation.
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
