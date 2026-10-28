"""Função OCR (leitura da placa) contra o contrato da ADR-0008.

Segundo estágio do pipeline ALPR: recebe o recorte da placa (o warp de
perspectiva do quad OBB acontece no meio do pipeline, fora daqui) e
devolve a string da placa. Modelo único para ambas as arquiteturas —
`cct-xs-v2` finetuned (`tests/finetune/exported_xs/best.onnx`, 3.2 MB):
accuracy igual à do `cct-s-v2` com runtime muito menor (~4× no x86,
~11× no ARM — ver MODELS.md §1–§2).

O recognizer é carregado uma única vez, no import do módulo — é isso que
o watchdog-lite aproveita ao manter o processo vivo entre invocações
(ver docs/ADR-0008-contrato-execucao-funcao.md, item 1). `handle(req)` é
chamada repetidamente por index.py a cada requisição recebida.

Contrato de entrada/saída (input/result de InvocationRequest/
ExecutionResponse, modelo-dados-schemas.md, Seções 5 e 8):

    entrada:  {"image_base64": "<recorte da placa, jpg ou png em base64>"}
    saída:    {"status": "success", "plate": "<texto>", "region": "<região>"}
           ou {"status": "error", "message": "..."}
"""
import base64
import binascii
import io
import json
import os

import numpy as np
import onnxruntime as ort
from fast_plate_ocr import LicensePlateRecognizer
from PIL import Image

MODEL_PATH = os.environ.get("MODEL_PATH", "./best.onnx")
CONFIG_PATH = os.environ.get("CONFIG_PATH", "./cct_xs_v2_global_plate_config.yaml")

# Núcleos usados pelo ONNX Runtime. Validado com threads=4 no ARM
# (MODELS.md §2: xs 40/40, p50 36.4 ms); 0 = padrão do ORT (todos).
_ort_threads = int(os.environ.get("ORT_THREADS", "0"))
_sess_options = ort.SessionOptions()
if _ort_threads > 0:
    _sess_options.intra_op_num_threads = _ort_threads
    _sess_options.inter_op_num_threads = _ort_threads

# Carregado uma vez só — o cold start de carregar o modelo não pode
# acontecer a cada invocação (RF-01, ADR-0005). device="auto" usa CUDA
# quando o ORT -gpu está instalado e há GPU, senão CPU.
recognizer = LicensePlateRecognizer(
    onnx_model_path=MODEL_PATH,
    plate_config_path=CONFIG_PATH,
    device="auto",
    sess_options=_sess_options,
)


def handle(req: str) -> str:
    try:
        data = json.loads(req)
        image_bytes = base64.b64decode(data["image_base64"], validate=True)
    except (KeyError, ValueError, TypeError, binascii.Error) as exc:
        return json.dumps({"status": "error", "message": f"entrada inválida: {exc}"})

    try:
        # O modelo espera RGB (não BGR), NHWC uint8 [1,64,128,3]; o
        # resize/pad segue o config da placa e o /255 é dentro do grafo
        # (MODELS.md §3). O run() da lib aplica tudo isso a partir do array.
        crop = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        img = np.asarray(crop, dtype=np.uint8)
    except Exception as exc:
        return json.dumps({"status": "error", "message": f"imagem inválida ou corrompida: {exc}"})

    try:
        result = recognizer.run(img)[0]
    except Exception as exc:  # inferência pode falhar por recorte degenerado etc.
        return json.dumps({"status": "error", "message": f"falha na inferência: {exc}"})

    return json.dumps({"status": "success", "plate": result.plate, "region": result.region})
