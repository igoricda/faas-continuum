# Exemplo: função ALPR borda slim (ncnn direto, sem torch)

Par slim de [`alpr-ncnn`](../alpr-ncnn) contra o contrato da ADR-0008
(`docs/ADR-0008-contrato-execucao-funcao.md`): mesma entrada, mesma
saída, mesmo modelo (`best_ncnn_model/`, exportado em `imgsz=640`) —
mas **sem torch/ultralytics**: só `ncnn` + `numpy` + `opencv-headless`
(`function/requirements.txt`).

## Por que existe

Na TVBox (Cortex-A55 + eMMC), o `alpr-ncnn` paga ~63 s de cold: ~31 s
de import do stack torch/ultralytics + ~32 s de page-in no primeiro
forward — o torch é peso morto (a inferência roda em ncnn, mas o
ultralytics o importa incondicionalmente). O export ncnn já traz o
decode embutido no grafo (anchors/strides/sigmoid/cos-sin na cauda do
`model.ncnn.param`), então o handler replica em numpy: letterbox
(com o arredondamento ±0.1) → ncnn `in0`/`out0` → filtro de confiança
→ fast-NMS com IoU probabilístico → scale-back. Cold projetado na casa
dos ~10 s (**a confirmar na primeira medição no box**).

## Paridade (validada em x86, 2026-09-23)

Handler slim vs handler ultralytics, mesmos pesos, mesmas imagens:
**diff 0** em classe, confiança e nos 5 campos do OBB (`x,y,w,h,
rotation_rad`) — em 3 imagens reais de teste (1 detecção cada), 5
sintéticas (0 detecções, cobre letterbox wide/tall/padded) e 1 collage
2×2 (4 detecções via NMS probiou). Warm equivalente em x86 (86 ms vs
103 ms — NMS numpy ~20 ms mais lento, irrelevante ante os ~300 ms do
warm no ARM). Import 5,8× mais rápido mesmo com page cache quente
(0,32 s vs 1,85 s); venv 348 MB vs 1,6 GB. (Absolutos x86 medidos com
o host em economia de energia, ~2,6 GHz sem turbo — inflados cerca de
1,5–2× contra performance plena; as razões e a paridade, medidas sob
as mesmas condições, valem.)

## Contrato de Prontidão (igual ao alpr-ncnn)

`index.py` com bind-first + lazy-load **e warmup-at-boot** (`_warmup`:
inferência dummy antes de `_ready.set()`, fail-open, `SKIP_WARMUP=1`
desliga). Mesmos códigos: `GET /_/ready` 200/503, `POST /_/ready`
sempre 404, `POST /` 503 até pronto.

## Estrutura

```
function/handler.py       # pipeline ncnn+numpy, sem torch
function/requirements.txt # ncnn + numpy + opencv-headless
index.py                  # mesmo contrato do alpr-ncnn, com warmup
best_ncnn_model/         # cópia do trio do alpr-ncnn (bin+param+metadata)
Dockerfile                # SOMENTE arm64
```

## Build e publicação (registry `igoricda`)

```bash
# a partir da raiz do repositório:
GOOS=linux GOARCH=arm64 go build -o examples/alpr-ncnn-slim/watchdog-lite ./cmd/watchdog-lite
cp examples/alpr-ncnn/best_ncnn_model/* examples/alpr-ncnn-slim/best_ncnn_model/
docker buildx build --platform linux/arm64 \
  -t docker.io/igoricda/alpr-ncnn-slim:latest \
  -f examples/alpr-ncnn-slim/Dockerfile examples/alpr-ncnn-slim --push
```

Build **nativo na TVBox** de preferência (cross via qemu é lento).

## Registro no agente

Cold projetado ~10 s ⇒ teto próprio de 60 s (margem ~6×;
`READY_TIMEOUT=60s` na imagem; **recalibrar após a primeira medição**):

```bash
curl -i -X POST "$BASE_AGENTE/function/alpr-ncnn-slim" \
  -H "Authorization: $ADMIN_NÓ" -H "Content-Type: application/json" -d '{
    "image_ref": "docker.io/igoricda/alpr-ncnn-slim:latest",
    "min_instances": 0,
    "timeout_ms": 60000
  }'
```

Invocação: mesmo envelope `{"input": {"image_base64": ...}}` do
alpr-ncnn. Knobs extras: `IOU_THRESHOLD=0.7`, `MAX_DET=300` (padrões do
ultralytics; `CONFIDENCE_THRESHOLD=0.25`, `IMAGE_SIZE=640` como no
original).
