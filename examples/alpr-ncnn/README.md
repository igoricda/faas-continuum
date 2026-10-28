# Exemplo: função ALPR borda (YOLO26-OBB via ncnn)

Primeiro estágio do pipeline ALPR na borda contra o contrato da
ADR-0008 (`docs/ADR-0008-contrato-execucao-funcao.md`). Recebe a imagem
da cena e devolve as coordenadas da(s) placa(s) em JSON. É o par de
borda do [`alpr`](../alpr) (servidor): mesma entrada e mesma saída, só
muda o backend do detector (ncnn em vez de torch/CUDA).

## Modelo (MODELS.md §2)

`best_ncnn_model/` (`model.ncnn.bin` + `model.ncnn.param`, 4.8 MB +
28 KB) — mesmos pesos do `best.pt`, exportados para ncnn em
`imgsz=640`. No ARM (4× Cortex-A55, `threads=4`): mAP50 0.995 /
mAP50-95 0.984 (vs 0.989 do export em 1280); infer ~222 ms/imagem no
val e ~270 ms por invocação (~4× mais rápido que o export em 1280 e
~11× que torch CPU no mesmo ARM).

Fora de escopo de propósito (MODELS.md §4): TFLite (mAP 0.0) e OCR via
ncnn (colapso do eixo de tokens) — o OCR na borda é ONNX Runtime, na
função [`ocr`](../ocr).

## Contrato de Prontidão (ADR-0008, Emenda 2026-09-14, §3)

O `index.py` implementa **bind-first + lazy-load** (mesmo padrão do
alpr/hello-world):

- `HTTPServer` faz `bind` em `127.0.0.1:$PORT` **antes** de importar
  `function.handler` (onde o modelo ncnn é carregado uma única vez).
- Thread daemon carrega `handle()` e sinaliza `threading.Event _ready`.
- **GET `/_/ready`**: `200 {"ready": true}` se pronto, `503` caso contrário.
- **POST `/_/ready`**: **sempre `404`**.
- **POST `/`**: `503` enquanto carrega, `200` + JSON válido após `_ready`.
- **GET desconhecido**: `404`.

## Estrutura

```
handler.py -> function/handler.py   # handle(req): modelo carregado 1x, chamado por invocação
index.py                            # servidor HTTP mínimo esperado pelo watchdog-lite
function/requirements.txt           # ultralytics==8.4.153 + ncnn + numpy<2 (torch+cpu no Dockerfile)
best_ncnn_model/                   # model.ncnn.bin + model.ncnn.param (ver "Preparar" abaixo)
Dockerfile                          # imagem final SOMENTE arm64, com watchdog-lite como principal
```

## Preparar

```bash
# a partir do treino musgd_def no repo LPR — o export fixa o shape de
# entrada (640), então o IMAGE_SIZE do handler/Dockerfile precisa
# coincidir com o imgsz do export:
yolo export task=obb model=runs/obb/musgd_def/weights/best.pt \
  format=ncnn imgsz=640 quantize=16
mkdir -p examples/alpr-ncnn/best_ncnn_model
cp runs/obb/musgd_def_640/weights/best_ncnn_model/model.ncnn.bin \
   runs/obb/musgd_def_640/weights/best_ncnn_model/model.ncnn.param \
   runs/obb/musgd_def_640/weights/best_ncnn_model/metadata.yaml \
   examples/alpr-ncnn/best_ncnn_model/
# metadata.yaml (task=obb, classe License-plate, imgsz 640) — OBRIGATÓRIO
# junto ao bin/param: sem ele o ultralytics carrega 999 classes genéricas
# e a detecção zera (verificado no Pi em 2026-09-19).

# compilar o watchdog-lite para a TVBox (arm64)
GOOS=linux GOARCH=arm64 go build -o examples/alpr-ncnn/watchdog-lite ./cmd/watchdog-lite
```

## Build e teste com Docker

```bash
cd examples/alpr-ncnn
docker buildx build --platform linux/arm64 -t faas-continuum/alpr-ncnn:latest .
docker run --rm -d --name alpr-ncnn-test \
  -v /tmp/alpr-ncnn-sockets:/run/faas-continuum-socket \
  faas-continuum/alpr-ncnn:latest
```

## Publicar (registry `igoricda`)

Tags publicadas: `docker.io/igoricda/alpr-ncnn:640` e `:latest` (mesmo
digest). Build **nativo na TVBox** (cross via qemu é lento); o contexto
precisa do `watchdog-lite` arm64 (`GOOS=linux GOARCH=arm64 go build
-o examples/alpr-ncnn/watchdog-lite ./cmd/watchdog-lite`).

Na TVBox via agente (min_instances=1): primeira invocação ~63 s
(readiness 29 s + primeira inferência, com push concorrente no box),
quente ~315 ms/imagem (medido 2026-09-23).

# monta o payload (imagem em base64) e invoca via socket Unix montado
python3 -c "
import base64, json
b64 = base64.b64encode(open('/caminho/para/uma/foto/de/placa.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json

curl -s --unix-socket /tmp/alpr-ncnn-sockets/handle.sock http://localhost/ -d @/tmp/payload.json
# -> {"status": "success", "detections": [{"class": "License-plate",
#      "confidence": 0.9, "obb": {"x": .., "y": .., "w": .., "h": ..,
#      "rotation_rad": ..}}]}
```

Invocação via agente: o nó espera o envelope `{"input": ...}`
(modelo-dados-schemas.md §5) e repassa só o conteúdo de `input` à
função — sem o envelope, `handle()` recebe corpo vazio e devolve
`{"status": "error", ...}`:

```bash
python3 -c "
import base64, json
b64 = base64.b64encode(open('/caminho/para/uma/foto/de/placa.jpg','rb').read()).decode()
print(json.dumps({'input': {'image_base64': b64}}))
" > /tmp/invoke.json

curl -s -X POST "$BASE_AGENTE/function/alpr-ncnn/invoke" \
  -H "Authorization: $INVOKE_TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/invoke.json
```

Limiar de confiança e tamanho de inferência saem de variáveis de
ambiente (`CONFIDENCE_THRESHOLD=0.25`, `IMAGE_SIZE=640` — o validado,
igual ao imgsz do export).

## Registro no agente (timeout por função)

O cold import no ARM lento + `ENV READY_TIMEOUT=90s` pedem teto próprio
de 120s (mesma regra do alpr: margem 30s ≥ max(10s, 20% de 90s);
`docs/config.md` e ADR-0008 Emenda 2026-09-14). Note que a inferência
ncnn em 640 leva ~270 ms/imagem na TVBox — o `timeout_ms` abaixo cobre
isso com folga para invocações isoladas:

```bash
curl -i -X POST "$BASE_AGENTE/function/alpr-ncnn" \
  -H "Authorization: $ADMIN_NÓ" -H "Content-Type: application/json" -d '{
    "image_ref": "faas-continuum/alpr-ncnn:latest",
    "min_instances": 1,
    "timeout_ms": 120000
  }'
# 201 → {"function":{...},"invoke_token":"..."}  GUARDE o invoke_token!
```
