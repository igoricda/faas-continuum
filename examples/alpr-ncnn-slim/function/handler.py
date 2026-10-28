"""Função ALPR (detecção de placa, backend ncnn direto) contra o contrato da ADR-0008.

Par slim de [`alpr-ncnn`](../alpr-ncnn): mesma entrada e mesma saída
JSON, mesmo modelo (`best_ncnn_model/`, exportado em imgsz=640), mas
SEM torch/ultralytics — só `ncnn` + `numpy` + `opencv-headless`. O
export ncnn já traz o decode embutido no grafo (anchors/strides/
sigmoid/cos-sin — verificado na cauda do `model.ncnn.param`), então o
runtime aqui é: letterbox -> ncnn in0/out0 -> filtro de confiança ->
fast-NMS com IoU probabilístico (probiou) -> scale-back. Paridade
bit-exata contra o handler ultralytics validada em imagens reais
(diff 0 em classe, confiança e nos 5 campos do OBB, inclusive com 4
detecções via NMS).

Motivação: o torch é peso morto no caminho ncnn (o ultralytics o
importa incondicionalmente) e domina o cold start no ARM — ~31 s de
import + ~32 s de page-in no primeiro forward em TVBox com eMMC. Sem
ele, o cold projetado cai para a casa de ~10 s (a confirmar no box).

O modelo é carregado uma única vez, no import do módulo — é isso que o
watchdog-lite aproveita ao manter o processo vivo entre invocações (ver
docs/ADR-0008-contrato-execucao-funcao.md, item 1). `handle(req)` é
chamada repetidamente por index.py a cada requisição recebida.

Contrato de entrada/saída (input/result de InvocationRequest/
ExecutionResponse, modelo-dados-schemas.md, Seções 5 e 8):

    entrada:  {"image_base64": "<jpg ou png em base64>"}
    saída:    {"status": "success", "detections": [...]}
           ou {"status": "error", "message": "..."}
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
# imgsz=640 é o tamanho validado no ARM (MODELS.md §2) e o shape
# congelado no export ncnn: precisa coincidir com o imgsz do export
# (metadata.yaml).
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "640"))
# NMS: mesmos padrões do ultralytics (DEFAULT_CFG iou=0.7, max_det=300).
IOU_THRESHOLD = float(os.environ.get("IOU_THRESHOLD", "0.7"))
MAX_DET = int(os.environ.get("MAX_DET", "300"))
CLASS_NAME = "License-plate"


def _load_net(path):
    net = ncnn.Net()
    net.opt.use_vulkan_compute = False
    net.load_param(os.path.join(path, "model.ncnn.param"))
    net.load_model(os.path.join(path, "model.ncnn.bin"))
    return net


# Carregado uma vez só — todo o ganho do pool de instâncias mínimas
# (RF-01, ADR-0005) depende disso: o cold start de carregar o modelo
# não pode acontecer a cada invocação.
net = _load_net(MODEL_PATH)


def letterbox(bgr, new_shape=640, color=(114, 114, 114)):
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
    chw = np.ascontiguousarray(rgb.transpose(2, 0, 1))

    ex = net.create_extractor()
    ex.input("in0", ncnn.Mat(chw))
    _, out = ex.extract("out0")
    # out0 (6,8400) = [cx, cy, w, h, conf, angle] já em px-640, com o
    # decode (anchors/strides/sigmoid) embutido no grafo exportado.
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
