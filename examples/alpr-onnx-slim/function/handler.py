"""Função ALPR (detecção de placa, backend ONNX Runtime direto) contra o contrato da ADR-0008.

Handler ONNX do detector (`best.onnx`, exportado em imgsz=1280, mAP
idêntico ao `.pt` validado na calib): mesma entrada e mesma saída JSON
do handler torch original, mas SEM torch/ultralytics — só
`onnxruntime` + `numpy` + `opencv-headless`. O export ONNX já traz o
decode embutido no grafo (grids/strides/sigmoid, como no export ncnn),
então o runtime aqui é: letterbox -> ORT `images`/`output0` -> filtro
de confiança -> fast-NMS com IoU probabilístico (probiou) ->
scale-back. O pós-processamento (letterbox/NMS/scale-back) é port
verbatim de `examples/alpr-ncnn-slim/function/handler.py`, cuja
paridade contra o ultralytics foi validada (aqui: mesmo N/classe/rank,
conf ±0.002, OBB ±1px em 5 imagens).

Motivação: no ACA (x86, Consumption) o cold de ~42 s é dominado pelo
pull da imagem de ~3 GB, cujo grosso é torch+CUDA+ultralytics (pull de
987 MB comprimidos medido em 15,9 s). Sem eles, a imagem cai para ~1 GB
e o import de ~3 s para ~1 s.
"""
import base64
import binascii
import json
import os

import cv2
import numpy as np
import onnxruntime as ort

MODEL_PATH = os.environ.get("MODEL_PATH", "./best.onnx")
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
# imgsz=1280 = tamanho do export best.onnx e do handler torch (MODELS.md
# §1: P 1.0 / R 1.0, mAP50 0.995 no data_calib, dataset 1280×720).
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "1280"))
# NMS: mesmos padrões do ultralytics (DEFAULT_CFG iou=0.7, max_det=300).
IOU_THRESHOLD = float(os.environ.get("IOU_THRESHOLD", "0.7"))
MAX_DET = int(os.environ.get("MAX_DET", "300"))
CLASS_NAME = "License-plate"

_ort_threads = int(os.environ.get("ORT_THREADS", "0"))
_sess_options = ort.SessionOptions()
if _ort_threads > 0:
    _sess_options.intra_op_num_threads = _ort_threads
    _sess_options.inter_op_num_threads = _ort_threads

# Carregado uma vez só, no import — o watchdog-lite/host reaproveita o
# processo entre invocações, então o cold de load não se repete por
# request (só por réplica fria).
_sess = ort.InferenceSession(
    MODEL_PATH, sess_options=_sess_options, providers=["CPUExecutionProvider"]
)
_IN_NAME = _sess.get_inputs()[0].name
_OUT_NAME = _sess.get_outputs()[0].name


def letterbox(bgr, new_shape=1280, color=(114, 114, 114)):
    """Redimensiona + padding como o LetterBox do ultralytics (inclui o
    arredondamento ±0.1 das bordas). Devolve (imagem, razão, (pad_x, pad_y))."""
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
    """(a, b, c) da gaussiana por OBB xywhr — espelha
    _get_covariance_matrix (ultralytics/utils/metrics.py)."""
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
    """IoU probabilístico par-a-par entre OBBs xywhr — espelha
    batch_probiou (ultralytics/utils/metrics.py)."""
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
    """Fast-NMS via matriz triangular — espelha TorchNMS.fast_nms
    (ultralytics/utils/nms.py)."""
    if boxes.shape[0] == 0:
        return np.zeros((0,), dtype=np.int64)
    order = np.argsort(-scores, kind="stable")
    b = boxes[order]
    ious = batch_probiou(b, b)
    ious = np.triu(ious, k=1)
    keep = np.nonzero((ious >= iou_thres).sum(0) <= 0)[0]
    return order[keep][:max_det]


def predict_bgr(bgr):
    """Pipeline completo sobre imagem BGR; devolve obbs (N,7) como
    [cx, cy, w, h, angle, conf, cls] em coordenadas originais."""
    oh, ow = bgr.shape[:2]
    lb, _, (pad_x, pad_y) = letterbox(bgr, IMAGE_SIZE)
    rgb = lb[:, :, ::-1].astype(np.float32) / np.float32(255.0)
    nchw = np.ascontiguousarray(rgb.transpose(2, 0, 1))[None]

    # output0 (1,6,33600) = [cx, cy, w, h, conf, angle] já em px-1280,
    # com o decode embutido no grafo exportado (mesmo layout do out0
    # ncnn em 640).
    pred = _sess.run([_OUT_NAME], {_IN_NAME: nchw})[0][0].T

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
    # scale_boxes com xywh=True: o pad sai só de cx,cy; tudo /gain; o
    # ângulo não é reescalado.
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
        return json.dumps({"status": "error", "message": f"entrada inválida: {exc}"})

    img = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        return json.dumps({"status": "error",
                           "message": "modelo não retornou resultado (imagem inválida ou corrompida?)"})
    try:
        obbs = predict_bgr(img)
    except Exception as exc:  # inferência pode falhar por imagem corrompida etc.
        return json.dumps({"status": "error", "message": f"falha na inferência: {exc}"})

    detections = []
    for o in obbs:
        x, y, w, h, r, conf, _ = (float(v) for v in o.tolist())
        detections.append({
            "class": CLASS_NAME,
            "confidence": round(conf, 4),
            "obb": {"x": x, "y": y, "w": w, "h": h, "rotation_rad": r},
        })

    return json.dumps({"status": "success", "detections": detections})
