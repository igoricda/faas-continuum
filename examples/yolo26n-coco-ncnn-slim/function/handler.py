"""Object detection function, with the YOLO26n model trained on COCO, the
direct ncnn backend and an ARM edge host.

It is the slim counterpart of the amd64 example: the input and the JSON
output are the same and the weights are the same, but without torch and
without ultralytics, with only ncnn, numpy and the headless build of
opencv.

The ncnn export already carries the decoding inside the graph, with the
anchors, the strides, the sigmoid and the scaling of the strides, so the
output comes out as eighty-four rows by eight thousand four hundred columns,
with the center, the size and the scores of the eighty classes, in pixels of
the resized image. The handler replicates the rest of the path of the
ultralytics runtime with numpy: the letterbox, with the rounding of the
borders by one tenth, the inference, the best class per box, the suppression
with the class offset and the rescaling back to the original coordinates
with a clip.

The motivation for the slim variant is that torch is dead weight in the
ncnn path, since the ultralytics runtime imports it unconditionally, and it
dominates the cold start on ARM. Without it the image of the container stays
in the hundreds of megabytes instead of gigabytes, with a cold start orders
of magnitude smaller than the one of the counterpart built on torch.

The model is loaded once, at the import of the module, which is what the
watchdog exploits by keeping the process alive between invocations. The
handler is called repeatedly by the HTTP process on every request received.

Input and output contract:

    input:   {"image_base64": "<jpg or png in base64>"}
    output:  {"status": "success", "image_base64": "<annotated jpg>",
              "image_format": "jpeg", "image_size": {...},
              "detections": [...], "inference_ms": 218.7}
             or {"status": "error", "message": "..."}
"""
import base64
import binascii
import json
import os
import time

import cv2
import ncnn
import numpy as np

MODEL_PATH = os.environ.get("MODEL_PATH", "./yolo26n_ncnn_model")
# Without the ultralytics runtime there is no metadata file to read the class
# names from, so they come from a plain text file generated from the
# checkpoint.
NAMES_PATH = os.environ.get("NAMES_PATH", "./coco80.names")
# The defaults of the ultralytics runtime: a confidence of 0.25, an
# intersection over union of 0.7 and at most three hundred detections.
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
IOU_THRESHOLD = float(os.environ.get("IOU_THRESHOLD", "0.7"))
MAX_DET = int(os.environ.get("MAX_DET", "300"))
# The image size of 640 is the default of the model and the static shape
# frozen in the ncnn export, so it has to match the one of the export.
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "640"))
# With the annotation disabled the answer carries only the detections and no
# encoded image, which suits clients that need a light JSON or that draw the
# image themselves.
ANNOTATE = os.environ.get("ANNOTATE", "1").lower() not in ("0", "false", "no")
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "85"))
# With a maximum side of zero the resolution of the input is kept. With a
# positive value the annotated image is reduced, and the coordinates follow
# it, so that it fits the output ceiling of the node, which is eight
# mebibytes by default.
ANNOTATE_MAX_SIDE = int(os.environ.get("ANNOTATE_MAX_SIDE", "0"))
# Zero leaves the default of the ncnn runtime, which already opens more than
# one thread; a positive value pins the count, and four threads were
# validated on ARM.
N_THREADS = int(os.environ.get("N_THREADS", "0"))
# The two constants of the suppression of the ultralytics runtime: the cut
# applied before the suppression and the offset that prevents the
# suppression between different classes.
MAX_NMS = int(os.environ.get("MAX_NMS", "30000"))
MAX_WH = np.float32(7680.0)

# The palette of the ultralytics runtime, the same one used by its
# annotator, so that the annotation looks like the output of the prediction
# command.
_PALETTE = (
    "042AFF", "0BDBEB", "F3F3F3", "00DFB7", "111F68",
    "FF6FDD", "FF444F", "CCED00", "00F344", "BD00FF",
    "00B4FF", "DD00BA", "00FFFF", "26C000", "01FFB3",
    "7D24FF", "7B0068", "FF1B6C", "FC6D2F", "A2FF0B",
)


def _load_net(path):
    net = ncnn.Net()
    net.opt.use_vulkan_compute = False  # No GPU on the target, and the driver costs a cold start.
    if N_THREADS > 0:
        net.opt.num_threads = N_THREADS
    net.load_param(os.path.join(path, "model.ncnn.param"))
    net.load_model(os.path.join(path, "model.ncnn.bin"))
    return net


def _load_names(path):
    with open(path, encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]
    if not names:
        raise ValueError(f"the list of classes in {path} is empty")
    return names


# Both are loaded only once, at the import, which is what the pool of
# minimal instances depends on: the cold start of loading the model cannot
# happen on every invocation.
net = _load_net(MODEL_PATH)
NAMES = _load_names(NAMES_PATH)


def _letterbox(bgr, new_shape=640, color=(114, 114, 114)):
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


def _nms(boxes, scores, iou_thres):
    """The greedy suppression, with the per-class suppression already done by
    the offset applied by the caller. It mirrors the operator of the
    torchvision library, which is what the ultralytics runtime uses when the
    torchvision library is available: it orders by confidence and suppresses
    every box whose intersection over union is above the threshold. The
    boxes already carry the class offset."""
    order = np.argsort(-scores, kind="stable")
    b = boxes[order]
    x1, y1, x2, y2 = b[:, 0].copy(), b[:, 1].copy(), b[:, 2].copy(), b[:, 3].copy()
    areas = (x2 - x1) * (y2 - y1)
    keep = []
    while x1.size > 0:
        keep.append(order[0])
        if x1.size == 1:
            break
        w = np.maximum(0.0, np.minimum(x2[0], x2[1:]) - np.maximum(x1[0], x1[1:]))
        h = np.maximum(0.0, np.minimum(y2[0], y2[1:]) - np.maximum(y1[0], y1[1:]))
        inter = w * h
        alive = inter / (areas[0] + areas[1:] - inter) <= iou_thres
        x1, y1, x2, y2 = x1[1:][alive], y1[1:][alive], x2[1:][alive], y2[1:][alive]
        areas, order = areas[1:][alive], order[1:][alive]
    return np.array(keep, dtype=np.int64)


def _predict(bgr):
    """The complete inference over a BGR image. It returns the list of
    detections, with the class, the confidence and the corners of the box, in
    the coordinates of the original image."""
    oh, ow = bgr.shape[:2]
    lb, _gain, (pad_x, pad_y) = _letterbox(bgr, IMAGE_SIZE)
    # The ncnn runtime expects RGB, since the letterbox returns BGR,
    # normalized in the range from zero to one.
    chw = np.ascontiguousarray(
        (lb[:, :, ::-1].astype(np.float32) / np.float32(255.0)).transpose(2, 0, 1)
    )

    ex = net.create_extractor()
    ex.input("in0", ncnn.Mat(chw))
    _, out = ex.extract("out0")
    # The output holds four rows plus the classes by eight thousand four
    # hundred columns, with the center, the size and the scores already in
    # pixels of the resized image, with the decoding built into the exported
    # graph. The array copies the data, since the buffer of the matrix
    # belongs to the extractor.
    pred = np.array(out, dtype=np.float32).T  # (8400, 4+nc)
    del ex

    cls = pred[:, 4:]
    if cls.shape[1] != len(NAMES):
        raise ValueError(
            f"the model has {cls.shape[1]} classes and {NAMES_PATH} has "
            f"{len(NAMES)}, regenerate the file from the checkpoint"
        )
    # The best class per box: the suppression of the runtime runs with a
    # single label on the prediction, so one box carries one class.
    cls_id = np.argmax(cls, axis=1)
    conf = cls[np.arange(cls.shape[0]), cls_id]
    keep = conf > np.float32(CONFIDENCE_THRESHOLD)
    if not keep.any():
        return []

    pred, cls_id, conf = pred[keep], cls_id[keep], conf[keep]
    # The conversion from the center and the size to the corners, which the
    # runtime does before the suppression.
    cx, cy, bw, bh = pred[:, 0], pred[:, 1], pred[:, 2], pred[:, 3]
    boxes = np.stack([cx - bw / 2, cy - bh / 2, cx + bw / 2, cy + bh / 2], axis=1)
    if boxes.shape[0] > MAX_NMS:
        top = np.argsort(-conf, kind="stable")[:MAX_NMS]
        boxes, cls_id, conf = boxes[top], cls_id[top], conf[top]
    # The class offset: boxes of different classes never suppress each other.
    idx = _nms(boxes + cls_id[:, None].astype(np.float32) * MAX_WH,
               conf, np.float32(IOU_THRESHOLD))[:MAX_DET]
    boxes, cls_id, conf = boxes[idx], cls_id[idx], conf[idx]

    # The rescaling and the clip of the boxes: the padding is removed from
    # every corner, every length is divided by the ratio, and the result is
    # clamped to the original image. The temporary array is a copy, since the
    # slice with an index list is a fancy index, so the clip writes into the
    # copy and the result is assigned back to the boxes; a clip applied
    # directly to the columns of the boxes would be discarded.
    gain = min(IMAGE_SIZE / oh, IMAGE_SIZE / ow)
    xy = (boxes[:, [0, 2]] - pad_x) / gain
    np.clip(xy, 0.0, float(ow), out=xy)
    boxes[:, [0, 2]] = xy
    xy = (boxes[:, [1, 3]] - pad_y) / gain
    np.clip(xy, 0.0, float(oh), out=xy)
    boxes[:, [1, 3]] = xy

    return [(int(c), float(s), tuple(float(v) for v in b)) for c, s, b in zip(cls_id, conf, boxes)]


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
        label = f"{NAMES[cls_id]} {conf:.2f}"
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
            "class": NAMES[cls_id],
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
    # EXIF header.
    img = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        return json.dumps({"status": "error", "message": "invalid or corrupted image, it cannot be decoded"})

    t0 = time.perf_counter()
    try:
        dets = _predict(img)
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
