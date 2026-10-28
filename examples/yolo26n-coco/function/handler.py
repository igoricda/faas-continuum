"""Object detection function, with the YOLO26n model trained on COCO and the
full backend.

It is the reference example and the amd64 counterpart of the slim ncnn
example, which runs at the edge on ARM: the input and the JSON output are
the same and only the backend of the detector changes, with torch and the
ultralytics runtime here and the direct ncnn runtime there.

The model is the nano variant of YOLO26 with the default weights of the
ultralytics runtime, pre-trained on COCO, with eighty classes, the
detection task and 5.3 MB, through the ultralytics runtime and torch. There
is no fine tuning and no private dataset, since it is the model that the
runtime downloads on the first load.

The rectangular inference is disabled on purpose and it is the only
difference in relation to the default of the prediction command: it forces
the square letterbox of 640 by 640 with a padding of 114, which is the
static shape frozen in the ncnn export of the slim counterpart. Without it
the runtime would use the smallest rectangle and the two examples would
diverge in the pre-processing, with the same class and the same count but a
confidence that differs by about one hundredth.

The model is loaded once, at the import of the module, which is what the
watchdog exploits by keeping the process alive between invocations. The
handler is called repeatedly by the HTTP process on every request received.

Input and output contract:

    input:   {"image_base64": "<jpg or png in base64>"}
    output:  {"status": "success", "image_base64": "<annotated jpg>",
              "image_format": "jpeg", "image_size": {...},
              "detections": [...], "inference_ms": 12.3}
             or {"status": "error", "message": "..."}

The boxes are drawn here with the image library and not with the plotting
helper of the runtime, so that the annotated image comes out byte identical
to the one of the slim ncnn counterpart: the plotting helper of the runtime
draws with its own font library, which the slim counterpart does not bring.
The annotation helper and the palette are identical copies in both
handlers.
"""
import base64
import binascii
import json
import os
import time

os.environ["YOLO_CONFIG_DIR"] = "/tmp/Ultralytics"
os.environ["YOLO_VERBOSE"] = "False"

import cv2  # noqa: E402
import numpy as np  # noqa: E402
from ultralytics import YOLO  # noqa: E402

MODEL_PATH = os.environ.get("MODEL_PATH", "./yolo26n.pt")
# The defaults of the ultralytics runtime: a confidence of 0.25, an
# intersection over union of 0.7 and at most three hundred detections.
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
IOU_THRESHOLD = float(os.environ.get("IOU_THRESHOLD", "0.7"))
MAX_DET = int(os.environ.get("MAX_DET", "300"))
# The image size of 640 is the default of the model and the shape of the ncnn
# export of the slim counterpart, so the two examples have to infer at the
# same size.
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "640"))
# With the annotation disabled the answer carries only the detections and no
# encoded image, which suits clients that need a light JSON or that draw the
# image themselves.
ANNOTATE = os.environ.get("ANNOTATE", "1").lower() not in ("0", "false", "no")
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "85"))
# With a maximum side of zero the resolution of the input is kept. With a
# positive value the annotated image is reduced, and the coordinates follow
# it, so that it fits the output ceiling of the node, which is eight
# mebibytes by default, which is useful for a 4K input that overflows the
# encoded image.
ANNOTATE_MAX_SIDE = int(os.environ.get("ANNOTATE_MAX_SIDE", "0"))

# The palette of the ultralytics runtime, the same one used by its
# annotator, so that the annotation looks like the output of the prediction
# command.
_PALETTE = (
    "042AFF", "0BDBEB", "F3F3F3", "00DFB7", "111F68",
    "FF6FDD", "FF444F", "CCED00", "00F344", "BD00FF",
    "00B4FF", "DD00BA", "00FFFF", "26C000", "01FFB3",
    "7D24FF", "7B0068", "FF1B6C", "FC6D2F", "A2FF0B",
)

# Loaded only once, which is what the pool of minimal instances depends on:
# the cold start of loading the model cannot happen on every invocation.
model = YOLO(MODEL_PATH)


def _bgr(cls_id):
    """The color of the class as a BGR tuple, since the image library draws
    in BGR."""
    h = _PALETTE[int(cls_id) % len(_PALETTE)]
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return b, g, r


def _annotate(bgr, dets):
    """Draws the boxes and the label, with the class and the confidence, on
    top of the image, which it modifies in place. It mirrors the annotator of
    the ultralytics runtime, with the same palette, the same line thickness
    and the same white text over the color of the class."""
    lw = max(round(sum(bgr.shape) / 2 * 0.003), 2)
    font = cv2.FONT_HERSHEY_SIMPLEX
    scale = max(lw / 3.0, 0.5)
    thick = max(lw - 1, 1)
    for cls_id, conf, (x1, y1, x2, y2) in dets:
        color = _bgr(cls_id)
        p1 = (int(round(x1)), int(round(y1)))
        p2 = (int(round(x2)), int(round(y2)))
        cv2.rectangle(bgr, p1, p2, color, lw)
        label = f"{model.names[cls_id]} {conf:.2f}"
        (tw, th), base = cv2.getTextSize(label, font, scale, thick)
        top = p1[1] - th - base if p1[1] - th - base >= 0 else p1[1]
        cv2.rectangle(bgr, (p1[0], top), (p1[0] + tw + 4, top + th + base), color, -1)
        cv2.putText(bgr, label, (p1[0] + 2, top + th), font, scale, (255, 255, 255), thick, cv2.LINE_AA)
    return bgr


def _resize_for_output(bgr, dets):
    """Applies the maximum side of the annotation, where zero keeps the
    original size. It returns the image and the detections with the
    coordinates in the coordinate system of the returned image."""
    if ANNOTATE_MAX_SIDE <= 0:
        return bgr, dets
    h, w = bgr.shape[:2]
    if max(h, w) <= ANNOTATE_MAX_SIDE:
        return bgr, dets
    factor = ANNOTATE_MAX_SIDE / max(h, w)
    nw, nh = max(1, round(w * factor)), max(1, round(h * factor))
    out = cv2.resize(bgr, (nw, nh), interpolation=cv2.INTER_AREA)
    scaled = [(c, s, tuple(v * factor for v in box)) for c, s, box in dets]
    return out, scaled


def _encode_jpeg(bgr):
    ok, buf = cv2.imencode(".jpg", bgr, [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY])
    if not ok:
        raise RuntimeError("the image could not be encoded")
    return base64.b64encode(buf.tobytes()).decode("ascii")


def _detections_json(dets):
    return [
        {
            "class": model.names[cls_id],
            "confidence": round(conf, 4),
            "box": {"x1": round(box[0], 2), "y1": round(box[1], 2),
                    "x2": round(box[2], 2), "y2": round(box[3], 2)},
        }
        for cls_id, conf, box in dets
    ]


def handle(req: str) -> str:
    try:
        data = json.loads(req)
        image_bytes = base64.b64decode(data["image_base64"], validate=True)
    except (KeyError, ValueError, TypeError, binascii.Error) as exc:
        return json.dumps({"status": "error", "message": f"invalid input: {exc}"})

    # The two examples decode the image the same way: the color mode of the
    # image library returns BGR already corrected by the orientation of the
    # EXIF header, and that is what the runtime consumes, since the predictor
    # swaps the channels and divides by 255.
    img = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        return json.dumps({"status": "error", "message": "invalid or corrupted image, it cannot be decoded"})

    t0 = time.perf_counter()
    try:
        results = model(
            img,
            conf=CONFIDENCE_THRESHOLD,
            iou=IOU_THRESHOLD,
            max_det=MAX_DET,
            imgsz=IMAGE_SIZE,
            rect=False,  # Square letterbox, the same as the ncnn export.
            verbose=False,
        )
        if not results:
            return json.dumps({"status": "error", "message": "the model returned no result, the image is invalid or corrupted"})
        boxes = results[0].boxes
        dets = [
            (int(b.cls), float(b.conf), tuple(float(v) for v in b.xyxy[0].tolist()))
            for b in boxes
        ]
    except Exception as exc:  # The inference can fail on a degenerate image.
        return json.dumps({"status": "error", "message": f"inference failed: {exc}"})
    inference_ms = (time.perf_counter() - t0) * 1000.0

    out = {"status": "success"}
    out_dets = dets
    if ANNOTATE:
        try:
            canvas, out_dets = _resize_for_output(img.copy(), dets)
            canvas = _annotate(canvas, out_dets)
            out["image_base64"] = _encode_jpeg(canvas)
            out["image_format"] = "jpeg"
            out["image_size"] = {"width": int(canvas.shape[1]), "height": int(canvas.shape[0])}
        except Exception as exc:
            return json.dumps({"status": "error", "message": f"the annotation failed: {exc}"})
    out["detections"] = _detections_json(out_dets)
    out["inference_ms"] = round(inference_ms, 2)
    return json.dumps(out)
