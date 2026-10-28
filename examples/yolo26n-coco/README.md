# Exemplo: função de detecção YOLO26n COCO (modelo default, servidor amd64)

Detecção de objetos genérica contra o contrato da ADR-0008
(`docs/ADR-0008-contrato-execucao-funcao.md`): **recebe a imagem e devolve
a imagem anotada** (base64) + as detecções em JSON. É o par **amd64** do
[`yolo26n-coco-ncnn-slim`](../yolo26n-coco-ncnn-slim) (borda ARM): mesma
entrada, mesma saída, mesmos pesos — só muda o backend do detector.

Diferente do `alpr`, aqui **não** há finetune nem dataset próprio: o
modelo é o `yolo26n.pt` **default do ultralytics, pré-treinado no COCO**
(80 classes, `task=detect`), que o próprio ultralytics baixa no primeiro
`YOLO("yolo26n.pt")` (assets v8.4.0). Serve de função "canário" para
medir o framework com uma inferência que todo mundo reconhece.

## Modelo

`yolo26n.pt` — YOLO26 **nano**, pré-treinado no **COCO**, o default do
ultralytics (o próprio `YOLO("yolo26n.pt")` baixa de
`ultralytics/assets` v8.4.0). Diferente do `alpr`, aqui **não** há
finetune nem dataset próprio: serve de função "canário" para medir o
framework com uma inferência que todo mundo reconhece.

| Propriedade | Valor (verificado no `.pt`) |
|---|---|
| Formato | `.pt` (PyTorch), 5.544.453 B |
| `sha256` | `9b09cc8b…4fef` (fixado no commit que adiciona o exemplo) |
| Task / head | `detect` / `Detect`, saída `[cx, cy, w, h, 80 scores]` |
| Classes | 80 (COCO) — `model.names` |
| Parâmetros | 2,57 M (454 módulos) |
| `stride` | `[8, 16, 32]` ⇒ 8400 anchors em 640×640 |
| `imgsz` default | 640 |
| Runtime | ultralytics 8.4.153 + torch (CUDA via pip default) |

**Nenhum `yolo val` foi rodado aqui**: este exemplo valida *paridade
entre backends* (tabela abaixo), não mAP. Para mAP do modelo, consulte a
ficha do ultralytics ou rode `yolo val model=yolo26n.pt data=coco.yaml`
no ambiente de avaliação.

## Contrato de I/O

```
entrada:  {"image_base64": "<jpg ou png em base64>"}
saída:    {"status": "success",
           "image_base64": "<jpg anotado em base64>",
           "image_format": "jpeg",
           "image_size": {"width": 810, "height": 1080},
           "detections": [{"class": "bus", "confidence": 0.9031,
                           "box": {"x1":.., "y1":.., "x2":.., "y2":..}}, ...],
           "inference_ms": 16.0}
       ou {"status": "error", "message": "..."}
```

- As coordenadas estão em pixels da **imagem de entrada** (o mesmo sistema
  da imagem anotada devolvida).
- `image_base64` some quando `ANNOTATE=0` (aí a resposta tem ~590 B em vez
  de ~313 KB — medido com `bus.jpg`).
- `inference_ms` é só a inferência (pré-processamento + forward + NMS); o
  encode JPEG/base64 e o JSON ficam de fora.

Saída real (contêiner `:cpu`, `bus.jpg` 810×1080, `image_base64`
abreviado):

```json
{"status": "success",
 "image_base64": "/9j/4AAQSkZJRgABAQAAAQABAAD...",
 "image_format": "jpeg",
 "image_size": {"width": 810, "height": 1080},
 "detections": [
   {"class": "bus", "confidence": 0.903, "box": {"x1": 9.72, "y1": 230.64, "x2": 801.06, "y2": 748.01}},
   {"class": "person", "confidence": 0.8548, "box": {"x1": 47.04, "y1": 397.75, "x2": 237.68, "y2": 901.59}},
   {"class": "person", "confidence": 0.845, "box": {"x1": 223.57, "y1": 407.0, "x2": 345.86, "y2": 861.3}},
   {"class": "person", "confidence": 0.8373, "box": {"x1": 669.09, "y1": 387.31, "x2": 810.0, "y2": 880.1}},
   {"class": "person", "confidence": 0.5567, "box": {"x1": 0.0, "y1": 552.99, "x2": 59.66, "y2": 872.6}}],
 "inference_ms": 46.4}
```

### Atenção ao teto de 8 MiB do nó

`max_invoke_body_bytes` e `max_function_output_bytes` são 8 MiB
(`config.example.jsonc:63-65`). Uma foto 4K anotada estoura isso em
base64. Use `ANNOTATE_MAX_SIDE=1280` para entradas grandes: medido com
uma imagem 3840×2160, o JPEG anotado cai de 596.141 B para 102.058 B (e
as coordenadas acompanham a nova escala). Com `ANNOTATE=0` o problema
desaparece.

## O `rect=False` (único desvio do default do `yolo predict`)

O `yolo predict` usa `rect=True` por padrão: letterbox no **retângulo
mínimo** (`640×352` para 1280×720, `480×640` para 810×1080), o que é
mais rápido. O export ncnn do par slim é **estático em 640×640**, então
para os dois exemplos inferirem no mesmo shape o handler passa
`rect=False` e força o quadrado com padding 114.

Medido em `bus.jpg` (810×1080) e `zidane.jpg` (1280×720), `rect=False`
contra `rect=True` no mesmo `.pt`:

| imagem | classe | conf `rect=True` | conf `rect=False` | Δ |
|---|---|---|---|---|
| bus.jpg | bus | 0.8806 | 0.9031 | +0.023 |
| bus.jpg | person (2ª) | 0.8608 | 0.8450 | −0.016 |
| zidane.jpg | person (2ª) | 0.8533 | 0.7927 | −0.061 |

Mesma classe, mesma contagem, **confiança diferente na casa de 1e-2** —
por isso os dois handlers deste par rodam com `rect=False` e o README do
par slim mede a paridade com o mesmo pre-processamento.

## Paridade com o par slim ncnn (medido 2026-09-28, x86)

Mesmo host, mesmos inputs, `conf=0.25 imgsz=640 rect=False`:

| conjunto | imagens | detecções | mesma contagem | mesmas classes | max \|Δconf\| | max \|Δbox\| |
|---|---|---|---|---|---|---|
| fotos reais (picsum, 1280×720) | 20 | 40 | 20/20 | 20/20 | 0,0016 | 0,12 px |
| regressão sintética (preto, ruído, 1×1, 4K, retrato, panorâmica, collage 2×2, PNG alpha, cinza, JPEG q5) | 15 | 26 | 15/15 | 15/15 | 0,0036 | 1,94 px (4K) |

A imagem anotada sai **byte-idêntica** em 15/20 fotos reais e 6/15 da
regressão; nos casos restantes a diferença é a borda de uma caixa
deslocada 1 px pelo arredondamento (≤ 200 B de delta no JPEG). O mesmo
conjunto de entradas inválidas (JSON quebrado, chave faltando, base64
inválido, bytes que não são imagem, payload vazio) dá a mesma resposta de
erro nos dois.

Diferenças de latência neste host x86 (mediana de 5, warm, `bus.jpg`):
**pt 20-24 ms** de parede contra **ncnn slim 50-60 ms** — no desktop o
torch vence (oneDNN/AVX); a vantagem do slim é o **cold e o tamanho da
imagem**, não a throughput. Ver o README do par slim.

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

Verificado: bind em 522 ms, `/_/ready` → 503, `POST /` → 503, pronto
777 ms depois (x86, import 1,2 s).

## Estrutura

```
handler.py -> function/handler.py   # handle(req): modelo carregado 1x, chamado por invocação
index.py                            # servidor HTTP mínimo esperado pelo watchdog-lite
function/requirements.txt           # ultralytics==8.4.153 + numpy (torch/CUDA via dependência)
yolo26n.pt                          # pesos default do COCO (ver "Preparar" abaixo)
Dockerfile                          # imagem final amd64 (torch CUDA), watchdog-lite como principal
Dockerfile.cpu                      # variante torch +cpu (sem GPU)
```

## Preparar

```bash
# a partir da raiz do repositório
curl -L -o examples/yolo26n-coco/yolo26n.pt \
  https://github.com/ultralytics/assets/releases/download/v8.4.0/yolo26n.pt

# compilar o watchdog-lite para o servidor (amd64)
GOOS=linux GOARCH=amd64 go build -o examples/yolo26n-coco/watchdog-lite ./cmd/watchdog-lite
```

## Build e teste com Docker

```bash
cd examples/yolo26n-coco
docker build --platform linux/amd64 -t igoricda/yolo26n-coco:gpu .   # CUDA
docker build --platform linux/amd64 -t igoricda/yolo26n-coco:cpu \
  -f Dockerfile.cpu .                                                # +cpu
```

Tamanhos medidos 2026-09-28 neste host (o grosso é o torch; a camada de
`chown -R` duplica `/home/app/python` e vale ~1 GB da imagem):

| Tag | Torch | `/home/app/python` | Imagem |
|---|---|---|---|
| `yolo26n-coco:cpu` (Dockerfile.cpu) | 2.14.0+cpu | 1,1 GB | **2,46 GB** |
| `yolo26n-coco:gpu` (Dockerfile) | CUDA (PyPI default, `nvidia-*`) | — | não medido aqui; mesma stack do `alpr` (~6,5 GB) |

O `:latest`/`:gpu` é a variante CUDA: com GPU visível o torch usa CUDA,
sem GPU cai para CPU automaticamente. Em host com NVIDIA Container
Toolkit, registre com `"gpu": true` (ver abaixo) para injetar o driver.

```bash
# o processo da função roda como `app` (não-root): o bind mount do
# socket precisa permitir escrita, senão o watchdog morre com
# "listen unix ...: bind: permission denied" (modo 777 no teste local).
rm -rf /tmp/yolo-sockets && mkdir -p /tmp/yolo-sockets && chmod 777 /tmp/yolo-sockets
docker run --rm -d --name yolo-test \
  -v /tmp/yolo-sockets:/run/faas-continuum-socket \
  igoricda/yolo26n-coco:cpu

python3 -c "
import base64, json
b64 = base64.b64encode(open('/caminho/para/uma/foto.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json

curl -s --unix-socket /tmp/yolo-sockets/handle.sock http://localhost/ -d @/tmp/payload.json \
  | python3 -m json.tool | head
# -> {"status": "success", "image_base64": "/9j/4AAQ...", "image_format": "jpeg",
#      "image_size": {...}, "detections": [{"class": "bus", "confidence": 0.903, ...}],
#      "inference_ms": 46.4}
```

> **Bind mount do socket:** o processo roda como `app` (não-root), então o
> diretório do socket precisa permitir escrita — com o padrão `755` do
> `mkdir` o watchdog morre com `listen unix ...: bind: permission
> denied`. Daí o `chmod 777` acima. O agente faz o bind mount com a
> propriedade certa (ver `internal/containerruntime`).

O nó entrega o `image_base64` da resposta; para ver a imagem:

```bash
curl -s --unix-socket /tmp/yolo-sockets/handle.sock http://localhost/ -d @/tmp/payload.json \
  | python3 -c "
import base64, json, sys
r = json.load(sys.stdin)
open('/tmp/anotada.jpg','wb').write(base64.b64decode(r['image_base64']))
print(r['detections'])
"
```

Invocação via agente: o nó espera o envelope `{"input": ...}`
(modelo-dados-schemas.md §5) e repassa só o conteúdo de `input` à
função — sem o envelope, `handle()` recebe corpo vazio e devolve
`{"status": "error", ...}`:

```bash
python3 -c "
import base64, json
b64 = base64.b64encode(open('/caminho/para/uma/foto.jpg','rb').read()).decode()
print(json.dumps({'input': {'image_base64': b64}}))
" > /tmp/invoke.json

curl -s -X POST "$BASE_AGENTE/function/yolo26n-coco/invoke" \
  -H "Authorization: $INVOKE_TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/invoke.json
```

## Knobs (env)

| Variável | Padrão | Efeito |
|---|---|---|
| `MODEL_PATH` | `/home/app/yolo26n.pt` | pesos (qualquer `.pt` do ultralytics) |
| `CONFIDENCE_THRESHOLD` | `0.25` | DEFAULT_CFG `conf` |
| `IOU_THRESHOLD` | `0.7` | DEFAULT_CFG `iou` (NMS) |
| `MAX_DET` | `300` | DEFAULT_CFG `max_det` |
| `IMAGE_SIZE` | `640` | DEFAULT_CFG `imgsz`; tem de bater com o export ncnn do par slim |
| `ANNOTATE` | `1` | `0` devolve só as detecções (resposta ~590 B) |
| `JPEG_QUALITY` | `85` | qualidade do jpg anotado (tamanho do payload) |
| `ANNOTATE_MAX_SIDE` | `0` | `>0` reduz a imagem anotada (e as coordenadas) para caber no teto de 8 MiB |

## Registro no agente (timeout por função)

O cold import `torch`/`ultralytics` (30–60 s) cabe no default 90 s do
nó, mas esse default zera a margem contra o `ENV READY_TIMEOUT=90s`
deste Dockerfile — registre com teto próprio de 120 s (margem 30 s ≥
max(10 s, 20% de 90 s); regra em `docs/config.md` e ADR-0008 Emenda
2026-09-14):

```bash
curl -i -X POST "$BASE_AGENTE/function/yolo26n-coco" \
  -H "Authorization: $ADMIN_NÓ" -H "Content-Type: application/json" -d '{
    "image_ref": "docker.io/igoricda/yolo26n-coco:cpu",
    "min_instances": 1,
    "timeout_ms": 120000
  }'
# 201 → {"function":{...},"invoke_token":"..."}  GUARDE o invoke_token!
```

Variante com GPU (host com driver NVIDIA + Container Toolkit
instalado): aponte para a tag `:gpu` e passe `"gpu": true` — o agente
injeta o hook prestart da NVIDIA e `NVIDIA_VISIBLE_DEVICES=all` no
contêiner (default `false` = CPU-only):

```bash
curl -i -X POST "$BASE_AGENTE/function/yolo26n-coco-gpu" \
  -H "Authorization: $ADMIN_NÓ" -H "Content-Type: application/json" -d '{
    "image_ref": "docker.io/igoricda/yolo26n-coco:gpu",
    "min_instances": 1,
    "timeout_ms": 120000,
    "gpu": true
  }'
```

## Latência medida (este host, 2026-09-28)

`bus.jpg` (810×1080), 5 detecções, anotação ligada:

| caminho | número | tempo |
|---|---|---|
| `handle()` local, warm | mediana de 5 | **20-24 ms** (inference_ms 12-16) |
| via socket Unix no contêiner `:cpu` | 3 invocações | 59-91 ms de parede |
| 1ª invocação (oneDNN aquecendo) | 1 | 3,8 s |

O salto entre as duas linhas é o custo de borda: base64 de 313 KB de
saída + JSON + socket + proxy, e o host sob carga. Para latência de
inferência puro compare sempre o `inference_ms`.

TVBox/Cortex-A55 não foi medido: a imagem é amd64. O par arm64 da mesma
função está em [`yolo26n-coco-ncnn-slim`](../yolo26n-coco-ncnn-slim).

## O que foi verificado (2026-09-28)

| Verificação | Resultado |
|---|---|
| `py_compile` nos dois handlers + `index.py` | OK |
| Paridade `.pt` vs ncnn slim em 35 imagens | mesma contagem e classes em 35/35 (tabela acima) |
| Entradas inválidas (5 casos) | mesma resposta de erro nos dois |
| `ANNOTATE=0` / `ANNOTATE_MAX_SIDE=1280` / `MAX_DET` / `CONFIDENCE_THRESHOLD` | OK, coordenadas acompanham a escala |
| `index.py`: bind antes do load, 503 → 200, `POST /_/ready` 404, GET desconhecido 404 | OK |
| Contêiner `:cpu` ponta a ponta (watchdog-lite + socket Unix) | `GET /_/ready` 200, `POST /` 200 com 5 detecções e JPEG de 312.192 B em base64 |
| `yolo val` / mAP | **não roda** (fora do escopo deste exemplo) |
