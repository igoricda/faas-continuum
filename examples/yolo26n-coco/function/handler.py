"""Função de detecção de objetos (YOLO26n COCO, modelo completo) contra o
contrato da ADR-0008.

Exemplo de referência — par **amd64** do
[`yolo26n-coco-ncnn-slim`](../yolo26n-coco-ncnn-slim) (borda ARM): mesma
entrada, mesma saída JSON, só muda o backend do detector (torch/ultralytics
aqui, ncnn direto lá).

Modelo: `yolo26n.pt` — o YOLO26 nano **default do ultralytics, pré-treinado
no COCO** (80 classes, task=detect, 5,3 MB), via ultralytics 8.4.153 + torch.
Sem finetune, sem dataset próprio: é o modelo que o `ultralytics` baixa
sozinho no primeiro `YOLO("yolo26n.pt")` (assets v8.4.0).

O `rect=False` no `model(...)` é deliberado e é a única diferença em
relação ao default do `yolo predict`: ele força o letterbox **quadrado**
640×640 (padding 114), que é o shape estático congelado no export ncnn do
par slim. Sem ele o ultralytics usaria o retângulo mínimo (`rect=True`) e
os dois exemplos divergiriam no pré-processamento (mesma classe e mesma
contagem, confiança diferente na casa de 1e-2 — medido, ver README).

O modelo é carregado uma única vez, no import do módulo — é isso que o
watchdog-lite aproveita ao manter o processo vivo entre invocações (ver
docs/ADR-0008-contrato-execucao-funcao.md, item 1). `handle(req)` é
chamada repetidamente por index.py a cada requisição recebida.

Contrato de entrada/saída (input/result de InvocationRequest/
ExecutionResponse, modelo-dados-schemas.md, Seções 5 e 8):

    entrada:  {"image_base64": "<jpg ou png em base64>"}
    saída:    {"status": "success", "image_base64": "<jpg anotado>",
               "image_format": "jpeg", "image_size": {...},
               "detections": [...], "inference_ms": 12.3}
           ou {"status": "error", "message": "..."}

O desenho das caixas é feito aqui com cv2 (e não com `result.plot()`) para
que a imagem anotada saia **byte-idêntica** à do par slim ncnn: o
`plot()` do ultralytics desenha com PIL/fonte própria, que o par slim não
traz. O helper `_annotate` e a paleta são cópias idênticas nos dois
handlers (conferidas com diff).
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
# Defaults do ultralytics (DEFAULT_CFG): conf 0.25, iou 0.7, max_det 300.
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
IOU_THRESHOLD = float(os.environ.get("IOU_THRESHOLD", "0.7"))
MAX_DET = int(os.environ.get("MAX_DET", "300"))
# imgsz=640 é o imgsz default do modelo e o shape do export ncnn do par
# slim — os dois exemplos precisam inferir no mesmo tamanho.
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "640"))
# ANNOTATE=0 devolve só as detecções (sem base64): para clientes que
# precisam do JSON leve ou que desenham a imagem no próprio front.
ANNOTATE = os.environ.get("ANNOTATE", "1").lower() not in ("0", "false", "no")
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "85"))
# ANNOTATE_MAX_SIDE=0 mantém a resolução de entrada. Com valor > 0, a
# imagem anotada é reduzida (e as coordenadas acompanham) para caber no
# teto de saída do nó (max_function_output_bytes, 8 MiB por default) —
# útil em entrada 4K, que estoura o base64.
ANNOTATE_MAX_SIDE = int(os.environ.get("ANNOTATE_MAX_SIDE", "0"))

# Paleta do ultralytics 8.4.153 (utils/plotting.py::Colors.hexs) — a
# mesma do Annotator, para a anotação ter a cara do `yolo predict`.
_PALETTE = (
    "042AFF", "0BDBEB", "F3F3F3", "00DFB7", "111F68",
    "FF6FDD", "FF444F", "CCED00", "00F344", "BD00FF",
    "00B4FF", "DD00BA", "00FFFF", "26C000", "01FFB3",
    "7D24FF", "7B0068", "FF1B6C", "FC6D2F", "A2FF0B",
)

# Carregado uma vez só — todo o ganho do pool de instâncias mínimas
# (RF-01, ADR-0005) depende disso: o cold start de carregar o modelo
# não pode acontecer a cada invocação.
model = YOLO(MODEL_PATH)


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
        label = f"{model.names[cls_id]} {conf:.2f}"
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
        return json.dumps({"status": "error", "message": f"entrada inválida: {exc}"})

    # Mesmo decode dos dois exemplos: cv2 IMREAD_COLOR devolve BGR já
    # corrigido pela orientação do EXIF, e é isso que o ultralytics
    # consome (o predictor troca BGR->RGB e divide por 255).
    img = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        return json.dumps({"status": "error", "message": "imagem inválida ou corrompida (não decodificável)"})

    t0 = time.perf_counter()
    try:
        results = model(
            img,
            conf=CONFIDENCE_THRESHOLD,
            iou=IOU_THRESHOLD,
            max_det=MAX_DET,
            imgsz=IMAGE_SIZE,
            rect=False,  # letterbox quadrado — igual ao export ncnn do par slim
            verbose=False,
        )
        if not results:
            return json.dumps({"status": "error", "message": "modelo não retornou resultado (imagem inválida ou corrompida?)"})
        boxes = results[0].boxes
        dets = [
            (int(b.cls), float(b.conf), tuple(float(v) for v in b.xyxy[0].tolist()))
            for b in boxes
        ]
    except Exception as exc:  # inferência pode falhar por imagemdegenerada etc.
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
