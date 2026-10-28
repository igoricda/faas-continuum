"""ALPR function, which is license plate detection with the direct ncnn
backend.

It is the slim counterpart of the ncnn example: the input and the JSON output
are the same and the model is the same, the directory `best_ncnn_model`
exported with an image size of 640, but without torch and without
ultralytics, with only ncnn, numpy and the headless build of opencv. The ncnn
export already carries the decoding inside the graph, with the anchors, the
strides, the sigmoid and the cosine and sine, so the runtime here is the
letterbox, the inference, the confidence filter, the fast suppression with
the probabilistic intersection over union and the rescaling back to the
original coordinates. The parity against the ultralytics handler was
validated on real images, with no difference in the class, in the confidence
and in the five fields of the oriented box, including with four detections
kept by the suppression.

The motivation is that torch is dead weight in the ncnn path, since the
ultralytics runtime imports it unconditionally, and it dominates the cold
start on ARM, with about thirty-one seconds of import and about thirty-two
seconds of page-in on the first forward on an embedded board with an eMMC
storage. Without it, the projected cold start falls to about ten seconds.

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

import cv2
import ncnn
import numpy as np

MODEL_PATH = os.environ.get("MODEL_PATH", "./best_ncnn_model")
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
# The image size of 640 is the validated one on ARM and the shape is frozen
# in the ncnn export, so it has to match the image size of the export.
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "640"))
# The suppression uses the same patterns as the ultralytics runtime, with an
# intersection over union of 0.7 and at most three hundred detections.
IOU_THRESHOLD = float(os.environ.get("IOU_THRESHOLD", "0.7"))
MAX_DET = int(os.environ.get("MAX_DET", "300"))
CLASS_NAME = "License-plate"


def _load_net(path):
    net = ncnn.Net()
    net.opt.use_vulkan_compute = False
    net.load_param(os.path.join(path, "model.ncnn.param"))
    net.load_model(os.path.join(path, "model.ncnn.bin"))
    return net


# Loaded only once, which is what the pool of minimal instances depends on:
# the cold start of loading the model cannot happen on every invocation.
net = _load_net(MODEL_PATH)


def letterbox(bgr, new_shape=640, color=(114, 114, 114)):
    """Resizes and pads the image like the letterbox of the ultralytics
    runtime, including the rounding of the borders by one tenth. It returns the
    image, the ratio and the padding on each axis."""
    h, w = bgr.shape[:2]
    r = min(new_shape / h, new_shape / w)
    nw, nh = round(w * r), round(h * r)
    resized = cv2.resize(bgr, (nw, nh), interpolation=cv2.INTER_LINEAR)
    dw, dh = new_shape - nw, new_shape - nh
    left, right = round(dw / 2 - 0.1), round(dw / 2 + 0.1)
    top, bottom = round(dh / 2 - 0.1), round(dh / 2 + 0.1)
    out = cv2.copyMakeBorder(resized, top, bottom, left, right,
                             cv2.BORDER_CONSTANT, value=color)
    return out, r, (left, top)


def _covariance(boxes):
    """The three coefficients of the gaussian of an oriented box given as
    center, size and angle, which mirrors the covariance matrix of the
    ultralytics runtime."""
    b = boxes.astype(np.float32)
    w2 = (b[:, 2] ** 2) / np.float32(12.0)
    h2 = (b[:, 3] ** 2) / np.float32(12.0)
    cos = np.cos(b[:, 4]).astype(np.float32)
    sin = np.sin(b[:, 4]).astype(np.float32)
    a = w2 * cos * cos + h2 * sin * sin
    bb = w2 * sin * sin + h2 * cos * cos
    c = (w2 - h2) * cos * sin
    return a, bb, c


def batch_probiou(boxes1, boxes2, eps=1e-7):
    """The pairwise probabilistic intersection over union of two sets of
    oriented boxes given as center, size and angle, which mirrors the batch
    function of the ultralytics runtime."""
    x1 = boxes1[:, 0:1].astype(np.float32)
    y1 = boxes1[:, 1:2].astype(np.float32)
    x2 = boxes2[:, 0:1].T.astype(np.float32)
    y2 = boxes2[:, 1:2].T.astype(np.float32)
    a1, b1, c1 = [v[:, None] for v in _covariance(boxes1)]
    a2, b2, c2 = [v[None, :] for v in _covariance(boxes2)]
    den = (a1 + a2) * (b1 + b2) - (c1 + c2) ** 2 + eps
    t1 = (((a1 + a2) * (y1 - y2) ** 2 + (b1 + b2) * (x1 - x2) ** 2) / den) * 0.25
    t2 = (((c1 + c2) * (x2 - x1) * (y1 - y2)) / den) * 0.5
    t3 = np.log(
        ((a1 + a2) * (b1 + b2) - (c1 + c2) ** 2)
        / (4 * np.sqrt(np.clip(a1 * b1 - c1 ** 2, 0, None)
                       * np.clip(a2 * b2 - c2 ** 2, 0, None)) + eps)
        + eps
    ) * 0.5
    bd = np.clip(t1 + t2 + t3, eps, 100.0)
    hd = np.sqrt(1.0 - np.exp(-bd) + eps)
    return 1 - hd


def fast_nms(boxes, scores, iou_thres, max_det=300):
    """The fast suppression through a triangular matrix, which mirrors the
    fast suppression of the ultralytics runtime."""
    if boxes.shape[0] == 0:
        return np.zeros((0,), dtype=np.int64)
    order = np.argsort(-scores, kind="stable")
    b = boxes[order]
    ious = batch_probiou(b, b)
    ious = np.triu(ious, k=1)
    keep = np.nonzero((ious >= iou_thres).sum(0) <= 0)[0]
    return order[keep][:max_det]


def predict_bgr(bgr):
    """The complete pipeline over a BGR image. It returns the oriented boxes
    as an array of seven columns, with the center, the size, the angle, the
    confidence and the class, in the original coordinates."""
    oh, ow = bgr.shape[:2]
    lb, _, (pad_x, pad_y) = letterbox(bgr, IMAGE_SIZE)
    rgb = lb[:, :, ::-1].astype(np.float32) / np.float32(255.0)
    chw = np.ascontiguousarray(rgb.transpose(2, 0, 1))

    ex = net.create_extractor()
    ex.input("in0", ncnn.Mat(chw))
    _, out = ex.extract("out0")
    # The output holds six rows by eight thousand four hundred columns, with
    # the center, the size, the confidence and the angle already in pixels of
    # the resized image, with the decoding built into the exported graph.
    pred = np.asarray(out).T

    filt = pred[:, 4] > np.float32(CONFIDENCE_THRESHOLD)
    p = pred[filt]
    if p.shape[0] == 0:
        return np.zeros((0, 7), dtype=np.float32)
    boxes = p[:, :4].astype(np.float32)
    scores = p[:, 4].astype(np.float32)
    angles = p[:, 5].astype(np.float32)
    keep = fast_nms(np.concatenate([boxes, angles[:, None]], axis=1),
                    scores, np.float32(IOU_THRESHOLD), MAX_DET)
    k = p[keep]
    # The rescaling of the boxes works on the center and the size: the
    # padding is removed from the center, every length is divided by the
    # ratio, and the angle is not rescaled.
    gain = min(IMAGE_SIZE / oh, IMAGE_SIZE / ow)
    cx = (k[:, 0] - pad_x) / gain
    cy = (k[:, 1] - pad_y) / gain
    w = k[:, 2] / gain
    h = k[:, 3] / gain
    out = np.stack([cx, cy, w, h, angles[keep],
                    scores[keep], np.zeros_like(scores[keep])], axis=1)
    return out.astype(np.float32)


def handle(req: str) -> str:
    try:
        data = json.loads(req)
        image_bytes = base64.b64decode(data["image_base64"], validate=True)
    except (KeyError, ValueError, TypeError, binascii.Error) as exc:
        return json.dumps({"status": "error", "message": f"invalid input: {exc}"})

    img = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        return json.dumps({"status": "error",
                           "message": "the model returned no result, the image is invalid or corrupted"})
    try:
        obbs = predict_bgr(img)
    except Exception as exc:  # The inference can fail on a corrupted image.
        return json.dumps({"status": "error", "message": f"inference failed: {exc}"})

    detections = []
    for o in obbs:
        x, y, w, h, r, conf, _ = (float(v) for v in o.tolist())
        detections.append({
            "class": CLASS_NAME,
            "confidence": round(conf, 4),
            "obb": {"x": x, "y": y, "w": w, "h": h, "rotation_rad": r},
        })

    return json.dumps({"status": "success", "detections": detections})
