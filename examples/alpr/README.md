# Exemplo: função ALPR servidor (YOLO26-OBB, modelo completo)

Primeiro estágio do pipeline ALPR na cloud contra o contrato da
ADR-0008 (`docs/ADR-0008-contrato-execucao-funcao.md`). Recebe a imagem
da cena e devolve as coordenadas da(s) placa(s) em JSON. É o par
servidor do [`alpr-ncnn`](../alpr-ncnn) (borda ARM): mesma entrada e
mesma saída, só muda o backend do detector.

## Tags

| Tag | Torch | Tamanho | Quando usar |
|---|---|---|---|
| `igoricda/alpr:latest` (= `:gpu`) | CUDA (PyPI default, ~6 GB de `nvidia-*`) | ~12 GB | servidor **com** GPU |
| `igoricda/alpr:cpu` (`Dockerfile.cpu`) | +cpu (índice CPU do PyTorch) | ~2.5 GB | servidor **sem** GPU (pull muito menor, mesma saída) |

Tamanhos medidos 2026-09-23 (multistage sem matplotlib/polars; o grosso
é o torch: CUDA ~10 GB, +cpu ~2 GB).

O `:latest` é a variante GPU: com GPU visível o torch usa CUDA, sem
GPU cai para CPU automaticamente. Em host com NVIDIA Container Toolkit,
registre com `"gpu": true` (ver abaixo) para injetar o driver no
contêiner — sem isso o torch enxerga só CPU mesmo com GPU no host.

## Modelo (MODELS.md §1)

`musgd_def/weights/best.pt` — YOLO26n-OBB, 5.9 MB, classe única
`License-plate`, via ultralytics 8.4.153 + torch (CUDA). Detecção em
`imgsz=1280` (`yolo val task=obb`: **P 1.0 / R 1.0 / mAP50 0.995 /
mAP50-95 0.989**).

## Contrato de Prontidão (ADR-0008, Emenda 2026-09-14, §3)

O `index.py` implementa **bind-first + lazy-load** (mesmo padrão do
alpr/hello-world):

- `HTTPServer` faz `bind` em `127.0.0.1:$PORT` **antes** de importar
  `function.handler` (onde o modelo YOLO é carregado uma única vez).
- Thread daemon carrega `handle()` e sinaliza `threading.Event _ready`.
- **GET `/_/ready`**: `200 {"ready": true}` se pronto, `503` caso contrário.
- **POST `/_/ready`**: **sempre `404`**.
- **POST `/`**: `503` enquanto carrega, `200` + JSON válido após `_ready`.
- **GET desconhecido**: `404`.

## Estrutura

```
handler.py -> function/handler.py   # handle(req): modelo carregado 1x, chamado por invocação
index.py                            # servidor HTTP mínimo esperado pelo watchdog-lite
function/requirements.txt           # ultralytics==8.4.153 + numpy (torch/CUDA via dependência)
best.pt                             # pesos do modelo (ver "Preparar" abaixo)
Dockerfile                          # imagem final amd64, com watchdog-lite como processo principal
```

## Preparar

```bash
# a partir da raiz do repositório
curl -L -o examples/alpr/best.pt \
  https://github.com/igoricda/alpr-yolo-models/raw/refs/heads/main/musgd_def/weights/best.pt

# compilar o watchdog-lite para o servidor (amd64)
GOOS=linux GOARCH=amd64 go build -o examples/alpr/watchdog-lite ./cmd/watchdog-lite
```

## Build e teste com Docker

```bash
cd examples/alpr
docker build --platform linux/amd64 -t igoricda/alpr:gpu .

# com GPU (o torch da imagem já vem com CUDA; sem --gpus cai para CPU):
docker run --rm -d --name alpr-test --gpus all \
  -v /tmp/alpr-sockets:/run/faas-continuum-socket \
  faas-continuum/alpr:latest

# monta o payload (imagem em base64) e invoca via socket Unix montado
python3 -c "
import base64, json
b64 = base64.b64encode(open('/caminho/para/uma/foto/de/placa.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json

curl -s --unix-socket /tmp/alpr-sockets/handle.sock http://localhost/ -d @/tmp/payload.json
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

curl -s -X POST "$BASE_AGENTE/function/alpr/invoke" \
  -H "Authorization: $INVOKE_TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/invoke.json
```

Limiar de confiança e tamanho de inferência saem de variáveis de
ambiente (`CONFIDENCE_THRESHOLD=0.25`, `IMAGE_SIZE=1280` — o validado).

## Registro no agente (timeout por função)

O cold import `torch`/`ultralytics` (30–60s) cabe no default 90s do nó,
mas esse default zera a margem contra o `ENV READY_TIMEOUT=90s` deste
Dockerfile — registre a função com teto próprio de 120s (margem 30s ≥
max(10s, 20% de 90s); regra em `docs/config.md` e ADR-0008 Emenda 2026-09-14):

```bash
curl -i -X POST "$BASE_AGENTE/function/alpr" \
  -H "Authorization: $ADMIN_NÓ" -H "Content-Type: application/json" -d '{
    "image_ref": "faas-continuum/alpr:latest",
    "min_instances": 1,
    "timeout_ms": 120000
  }'
# 201 → {"function":{...},"invoke_token":"..."}  GUARDE o invoke_token!
```

Variante com GPU (host com driver NVIDIA + Container Toolkit
instalado): aponte para a tag `:gpu` (ou `:latest`) e passe
`"gpu": true` — o agente injeta o hook prestart da NVIDIA e
`NVIDIA_VISIBLE_DEVICES=all` no contêiner (default `false` = CPU-only):

```bash
curl -i -X POST "$BASE_AGENTE/function/alpr-gpu" \
  -H "Authorization: $ADMIN_NÓ" -H "Content-Type: application/json" -d '{
    "image_ref": "igoricda/alpr:gpu",
    "min_instances": 1,
    "timeout_ms": 120000,
    "gpu": true
  }'
```

Variante sem GPU (pull ~1 GB em vez de ~6 GB): `igoricda/alpr:cpu`,
sem `"gpu"`.
