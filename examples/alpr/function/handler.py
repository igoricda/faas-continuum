"""Função ALPR (detecção de placa, modelo completo) contra o contrato da ADR-0008.

Primeiro estágio do pipeline ALPR no servidor/cloud (amd64, GPU):
recebe a imagem da cena e devolve as coordenadas da(s) placa(s). Usa o
modelo treinado em `igoricda/alpr-yolo-models`
(`musgd_def/weights/best.pt` — YOLO26n-OBB, 5.9 MB, classe única
`License-plate`), via ultralytics + torch (CUDA) — MODELS.md §1.

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

MODEL_PATH = os.environ.get("MODEL_PATH", "./best.pt")
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.25"))
# imgsz=1280 é o tamanho validado (MODELS.md §1: P 1.0 / R 1.0,
# mAP50 0.995 no data_calib, dataset 1280×720).
IMAGE_SIZE = int(os.environ.get("IMAGE_SIZE", "1280"))

# Carregado uma vez só — todo o ganho do pool de instâncias mínimas
# (RF-01, ADR-0005) depende disso: o cold start de carregar o modelo
# não pode acontecer a cada invocação.
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
