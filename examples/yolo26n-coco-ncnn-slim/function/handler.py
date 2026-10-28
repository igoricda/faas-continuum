"""Função de detecção de objetos (YOLO26n COCO) na borda ARM, backend ncnn
direto, contra o contrato da ADR-0008.

Par slim de [`yolo26n-coco`](../yolo26n-coco) (amd64): **mesma entrada e
mesma saída JSON, mesmos pesos** — mas SEM torch/ultralytics. Só `ncnn` +
`numpy` + `opencv-headless` (`function/requirements.txt`).

O export ncnn (`yolo export format=ncnn imgsz=640`) já traz o decode no
grafo — anchors, strides, sigmoid e o scale dos strides estão na cauda do
`model.ncnn.param` (ver `Concat cat_22` + `Sigmoid sigmoid_0`), então o
`out0` sai como (84, 8400) = [cx, cy, w, h, 80 classes] em px-640. O
handler replica o resto do caminho do ultralytics em numpy: letterbox
(com o arredondamento ±0.1 das bordas) → ncnn `in0`/`out0` → melhor
classe por caixa → NMS com offset de classe → scale-back com clip.

Motivação do slim: o torch é peso morto no caminho ncnn (o ultralytics o
importa incondicionalmente) e domina o cold start no ARM. Sem ele, a
imagem da TVBox (Debian 12, python:3.11-slim) fica na casa das centenas de
MB em vez de GB, com um cold ordens de grandeza menor que o do par
torch/ultralytics (ver README para os números medidos).

O modelo é carregado uma única vez, no import do módulo — é isso que o
watchdog-lite aproveita ao manter o processo vivo entre invocações (ver
docs/ADR-0008-contrato-execucao-funcao.md, item 1). `handle(req)` é
chamada repetidamente por index.py a cada requisição recebida.

Contrato de entrada/saída (input/result de InvocationRequest/
ExecutionResponse, modelo-dados-schemas.md, Seções 5 e 8):

    entrada:  {"image_base64": "<jpg ou png em base64>"}
    saída:    {"status": "success", "image_base64": "<jpg anotado>",
               "image_format": "jpeg", "image_size": {...},
               "detections": [...], "inference_ms": 218.7}
           ou {"status": "error", "message": "..."}
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
# Sem ultralytics para ler os nomes de classe do metadata.yaml, eles vêm
# de um arquivo de texto puro gerado do yolo26n.pt (ver README, "Preparar").
NAMES_PATH = os.environ.get("NAMES_PATH", "./coco80.names")
# Defaults do ultralytics (DEFAULT_CFG): conf 0.25, iou 0.7, max_det 300.
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
IOU_THRESHOLD = float(os.environ.get("IOU_THRESHOLD", "0.7"))
MAX_DET = int(os.environ.get("MAX_DET", "300"))
# imgsz=640 é o imgsz default do modelo e o shape estático congelado no
# export ncnn (metadata.yaml) — precisa coincidir com o do export.
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "640"))
# ANNOTATE=0 devolve só as detecções (sem base64): para clientes que
# precisam do JSON leve ou que desenham a imagem no próprio front.
ANNOTATE = os.environ.get("ANNOTATE", "1").lower() not in ("0", "false", "no")
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "85"))
# ANNOTATE_MAX_SIDE=0 mantém a resolução de entrada. Com valor > 0, a
# imagem anotada é reduzida (e as coordenadas acompanham) para caber no
# teto de saída do nó (max_function_output_bytes, 8 MiB por default).
ANNOTATE_MAX_SIDE = int(os.environ.get("ANNOTATE_MAX_SIDE", "0"))
# N_THREADS=0 deixa o default do ncnn (que já abre mais de uma thread na
# TVBox); >0 fixa. MODELS.md §2 validou threads=4 no ARM.
N_THREADS = int(os.environ.get("N_THREADS", "0"))
# Restos do non_max_suppression do ultralytics (utils/nms.py): o corte
# antes do NMS e o offset que impede supressão entre classes diferentes.
MAX_NMS = int(os.environ.get("MAX_NMS", "30000"))
MAX_WH = np.float32(7680.0)

# Paleta do ultralytics 8.4.153 (utils/plotting.py::Colors.hexs) — a
# mesma do Annotator, para a anotação ter a cara do `yolo predict`.
_PALETTE = (
    "042AFF", "0BDBEB", "F3F3F3", "00DFB7", "111F68",
    "FF6FDD", "FF444F", "CCED00", "00F344", "BD00FF",
    "00B4FF", "DD00BA", "00FFFF", "26C000", "01FFB3",
    "7D24FF", "7B0068", "FF1B6C", "FC6D2F", "A2FF0B",
)


def _load_net(path):
    net = ncnn.Net()
    net.opt.use_vulkan_compute = False  # sem GPU no alvo; e o driver Vulkan custa cold
    if N_THREADS > 0:
        net.opt.num_threads = N_THREADS
    net.load_param(os.path.join(path, "model.ncnn.param"))
    net.load_model(os.path.join(path, "model.ncnn.bin"))
    return net


def _load_names(path):
    with open(path, encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]
    if not names:
        raise ValueError(f"lista de classes vazia em {path}")
    return names


# Ambos carregados uma vez só, no import — todo o ganho do pool de
# instâncias mínimas (RF-01, ADR-0005) depende disso: o cold start de
# carregar o modelo não pode acontecer a cada invocação.
net = _load_net(MODEL_PATH)
NAMES = _load_names(NAMES_PATH)


def _letterbox(bgr, new_shape=640, color=(114, 114, 114)):
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


def _nms(boxes, scores, iou_thres):
    """NMS gulosa, com supressão classe-a-classe já feita pelo offset do
    chamador — espelha torchvision.ops.nms, que é o que o
    non_max_suppression do ultralytics usa quando o torchvision está
    importado (utils/nms.py:160): ordena por confiança e suprime o que
    tiver IoU > limiar. `boxes` já vem com o offset de classe somado."""
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
    """Inferência completa sobre imagem BGR; devolve a lista de detecções
    [(class_id, conf, (x1, y1, x2, y2)), ...] em coordenadas da imagem
    original."""
    oh, ow = bgr.shape[:2]
    lb, _gain, (pad_x, pad_y) = _letterbox(bgr, IMAGE_SIZE)
    # ncnn espera RGB (o letterbox devolve BGR) normalizado em 0..1.
    chw = np.ascontiguousarray(
        (lb[:, :, ::-1].astype(np.float32) / np.float32(255.0)).transpose(2, 0, 1)
    )

    ex = net.create_extractor()
    ex.input("in0", ncnn.Mat(chw))
    _, out = ex.extract("out0")
    # out0 (4+nc, 8400) = [cx, cy, w, h, scores...] já em px-640, com o
    # decode embutido no grafo exportado. np.array copia: o buffer do Mat
    # pertence ao extractor.
    pred = np.array(out, dtype=np.float32).T  # (8400, 4+nc)
    del ex

    cls = pred[:, 4:]
    if cls.shape[1] != len(NAMES):
        raise ValueError(
            f"modelo tem {cls.shape[1]} classes, {NAMES_PATH} tem {len(NAMES)} "
            "(regere o arquivo a partir do .pt — ver README)"
        )
    # Melhor classe por caixa: o non_max_suppression roda em single_label
    # no predict (multi_label=False), então uma caixa = uma classe.
    cls_id = np.argmax(cls, axis=1)
    conf = cls[np.arange(cls.shape[0]), cls_id]
    keep = conf > np.float32(CONFIDENCE_THRESHOLD)
    if not keep.any():
        return []

    pred, cls_id, conf = pred[keep], cls_id[keep], conf[keep]
    # xywh -> xyxy (o NMS do ultralytics converte antes de suprimir).
    cx, cy, bw, bh = pred[:, 0], pred[:, 1], pred[:, 2], pred[:, 3]
    boxes = np.stack([cx - bw / 2, cy - bh / 2, cx + bw / 2, cy + bh / 2], axis=1)
    if boxes.shape[0] > MAX_NMS:
        top = np.argsort(-conf, kind="stable")[:MAX_NMS]
        boxes, cls_id, conf = boxes[top], cls_id[top], conf[top]
    # Offset de classe (max_wh=7680): caixas de classes diferentes nunca
    # se suprimem entre si (agnostic=False).
    idx = _nms(boxes + cls_id[:, None].astype(np.float32) * MAX_WH,
               conf, np.float32(IOU_THRESHOLD))[:MAX_DET]
    boxes, cls_id, conf = boxes[idx], cls_id[idx], conf[idx]

    # scale_boxes(padding=True, xywh=False) + clip_boxes: o pad sai de
    # todos os cantos, tudo divide pelo gain, e o resultado é preso na
    # imagem original. `xy` é uma cópia (o slice com lista de índices é
    # fancy-index), então o out= do np.clip escreve nela e volta para
    # `boxes` — clip direto em boxes[:, [0, 2]] seria descartado.
    gain = min(IMAGE_SIZE / oh, IMAGE_SIZE / ow)
    xy = (boxes[:, [0, 2]] - pad_x) / gain
    np.clip(xy, 0.0, float(ow), out=xy)
    boxes[:, [0, 2]] = xy
    xy = (boxes[:, [1, 3]] - pad_y) / gain
    np.clip(xy, 0.0, float(oh), out=xy)
    boxes[:, [1, 3]] = xy

    return [(int(c), float(s), tuple(float(v) for v in b)) for c, s, b in zip(cls_id, conf, boxes)]


def _bgr(cls_id):
    """Cor da classe como tupla BGR (o cv2 desenha em BGR)."""
    h = _PALETTE[int(cls_id) % len(_PALETTE)]
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return b, g, r


def _annotate(bgr, dets):
    """Desenha as caixas e o rótulo (classe + confiança) em cima de
    `bgr` (modifica in-place). Espelha o Annotator do ultralytics: mesma
    paleta, mesma espessura de linha (lw = max(round(sum(shape)/2*0.003),
    2)) e mesmo texto branco sobre a cor da classe."""
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
    """Aplica ANNOTATE_MAX_SIDE (0 = mantém). Devolve (imagem, detecções)
    com as coordenadas no sistema da imagem devolvida."""
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
        raise RuntimeError("cv2.imencode falhou")
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
        return json.dumps({"status": "error", "message": f"entrada inválida: {exc}"})

    # Mesmo decode dos dois exemplos: cv2 IMREAD_COLOR devolve BGR já
    # corrigido pela orientação do EXIF.
    img = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        return json.dumps({"status": "error", "message": "imagem inválida ou corrompida (não decodificável)"})

    t0 = time.perf_counter()
    try:
        dets = _predict(img)
    except Exception as exc:  # inferência pode falhar por imagem degenerada etc.
        return json.dumps({"status": "error", "message": f"falha na inferência: {exc}"})
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
            return json.dumps({"status": "error", "message": f"falha na anotação: {exc}"})
    out["detections"] = _detections_json(out_dets)
    out["inference_ms"] = round(inference_ms, 2)
    return json.dumps(out)
