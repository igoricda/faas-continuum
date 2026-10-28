"""Função ALPR (detecção de placa, backend ncnn) contra o contrato da ADR-0008.

Primeiro estágio do pipeline ALPR na borda (ARM aarch64): recebe a
imagem da cena e devolve as coordenadas da(s) placa(s). Usa o mesmo
treino do servidor exportado para ncnn em imgsz=640
(`best_ncnn_model/`: `model.ncnn.bin` + `model.ncnn.param`, 4.8 MB +
28 KB) via ultralytics + ncnn — MODELS.md §2. É o par de borda do
[`alpr`](../alpr) (servidor): mesma entrada e mesma saída JSON, só muda
o backend do detector.

Métricas no ARM (mesmos pesos do `.pt`, entrada 640, threads=4):
mAP50 0.995 / mAP50-95 0.984, infer ~222 ms em 4× Cortex-A55 (~4×
mais rápido que o export em 1280 e ~11× que torch CPU).

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

os.environ["YOLO_CONFIG_DIR"] = "/tmp/Ultralytics"
os.environ["YOLO_VERBOSE"] = "False"

from ultralytics import YOLO  # noqa: E402

MODEL_PATH = os.environ.get("MODEL_PATH", "./best_ncnn_model")
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
# imgsz=640 é o tamanho validado no ARM (MODELS.md §2, threads=4,
# infer ~222 ms — ~4× mais rápido que 1280 com mAP50 idêntico).
# O shape é congelado na exportação ncnn: o valor aqui só precisa
# coincidir com o do export (metadata.yaml).
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "640"))

# Carregado uma vez só — todo o ganho do pool de instâncias mínimas
# (RF-01, ADR-0005) depende disso: o cold start de carregar o modelo
# não pode acontecer a cada invocação. O diretório ncnn é detectado
# pelo backend ncnn do ultralytics (mesma API do .pt).
model = YOLO(MODEL_PATH)


def handle(req: str) -> str:
    try:
        data = json.loads(req)
        image_bytes = base64.b64decode(data["image_base64"], validate=True)
    except (KeyError, ValueError, TypeError, binascii.Error) as exc:
        return json.dumps({"status": "error", "message": f"entrada inválida: {exc}"})

    tmp_path = "/tmp/invocation_input.jpg"
    with open(tmp_path, "wb") as f:
        f.write(image_bytes)

    try:
        results = model(tmp_path, conf=CONFIDENCE_THRESHOLD, imgsz=IMAGE_SIZE, verbose=False)
        if not results:
            return json.dumps({"status": "error", "message": "modelo não retornou resultado (imagem inválida ou corrompida?)"})
        result = results[0]
    except Exception as exc:  # inferência pode falhar por imagem corrompida etc.
        return json.dumps({"status": "error", "message": f"falha na inferência: {exc}"})

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
