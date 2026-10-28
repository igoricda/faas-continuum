# Exemplo: função de detecção YOLO26n COCO na borda (ncnn slim, sem torch)

Par **arm64** de [`yolo26n-coco`](../yolo26n-coco) (servidor amd64)
contra o contrato da ADR-0008
(`docs/ADR-0008-contrato-execucao-funcao.md`): **mesma entrada, mesma
saída JSON e mesmos pesos** (o `yolo26n.pt` do COCO exportado para ncnn) —
mas **sem torch/ultralytics**: só `ncnn` + `numpy` + `opencv-headless`
(`function/requirements.txt`), num `python:3.11-slim`.

## Modelo

`yolo26n_ncnn_model/` — export do `yolo26n.pt` default do COCO para ncnn
em `imgsz=640` (`quantize=16` opcional, ver "Preparar"):

| Arquivo | Tamanho | Papel |
|---|---|---|
| `model.ncnn.bin` | 9.736.936 B (9,3 MiB) | pesos fp32 |
| `model.ncnn.param` | 26.404 B | grafo |
| `metadata.yaml` | 1.509 B | procedência do export (task, imgsz, 80 nomes) — **não é lido em runtime** |
| `coco80.names` (na raiz do exemplo) | 621 B | os 80 nomes, em texto puro |

`sha256`: `model.ncnn.bin` = `d4d6cf5b817d1aa7…d555`,
`model.ncnn.param` = `1d224cb99489e996…6217`, `coco80.names` =
`bd17f1ee35d5f3c8…730a`. São os arquivos deste commit; um export
refeito gera bytes diferentes (o `metadata.yaml` inclui a data do
exportador).

O `metadata.yaml` fica na imagem como registro do export (mesma razão do
`alpr-ncnn`), mas o handler **não** o parseia: sem `pyyaml` no runtime,
os 80 nomes de classe vêm de `coco80.names` (texto puro, 80 linhas,
gerado do `.pt` — ver "Preparar"). O handler aborta com mensagem clara
se a contagem não bater com a do modelo.

`out0` do grafo exportado sai como **(84, 8400) = [cx, cy, w, h, 80
scores]** em px-640: o decode (anchors, strides, sigmoid) já está no
fim do `model.ncnn.param` (`Concat cat_22` + `Sigmoid sigmoid_0`). O
handler só replica o que vem **depois** do forward.

## Por que existe

O torch é peso morto no caminho ncnn (o ultralytics o importa
incondicionalmente mesmo quando a inferência é ncnn). Tirando os dois, a
imagem de borda fica em **811 MB** em vez de 2,46 GB (o par amd64 +cpu) e
o `import` do handler cai de 1,2 s para 0,13 s neste host — no ARM/eMMC
a diferença é ordens de grandeza maior (o `alpr-ncnn-slim` mediu ~63 s →
~10 s projetado na TVBox).

O ganho aqui **não** é throughput: neste x86 o ncnn é ~2,4× mais lento que
o torch+oneDNN (50-60 ms vs 20-24 ms de parede, warm). É tamanho de
imagem, cold e RAM. Para throughput no ARM, o `alpr-ncnn` mediu ncnn
2,55× mais rápido que torch CPU no mesmo box (MODELS.md §2) — **para
este modelo ainda não medimos na TVBox**.

## Paridade com o par .pt (medido 2026-09-28, x86)

Mesmo host, mesmos inputs, `conf=0.25 imgsz=640 rect=False` (o par
amd64 passa `rect=False` justamente para bater com o shape estático
deste export — ver o README dele):

| conjunto | imagens | detecções | mesma contagem | mesmas classes | max \|Δconf\| | max \|Δbox\| |
|---|---|---|---|---|---|---|
| fotos reais (picsum, 1280×720) | 20 | 40 | 20/20 | 20/20 | 0,0016 | 0,12 px |
| regressão sintética (preto, ruído, 1×1, 4K, retrato, panorâmica, collage 2×2, PNG alpha, cinza, JPEG q5) | 15 | 26 | 15/15 | 15/15 | 0,0036 | 1,94 px (4K) |
| contêiner arm64 (qemu) vs contêiner amd64, `bus.jpg` | 1 | 5 | 1/1 | 1/1 | 0,0048 | 2,09 px |

A última linha é a comparação entre as **imagens de verdade** (as duas
publicadas, mesmo `bus.jpg`): mesmas 5 detecções, mesmas classes, mesma
ordem, `bus` a 0,9030 (amd64) contra 0,9038 (arm64). O delta maior que o
do x86 vem dos kernels aarch64 do ncnn (NEON, ordem de acumulação
diferente) — a emulação qemu é exata em ponto flutuante, então o número
é o aarch64 real, mesmo que o **tempo** medido por ela não signifique
nada (ver "Latência medida").

A **imagem anotada sai byte-idêntica** em 15/20 fotos reais (mesma
paleta, mesma espessura, mesmo JPEG — o helper `_annotate` é cópia
literal entre os dois handlers, conferida com diff). Nos 5 restantes a
diferença é a borda de uma caixa 1 px deslocada pelo arredondamento
(delta ≤ 200 B no JPEG, e a diferença de pixel fica numa faixa de ~17 px
de largura).

O mesmo conjunto de entradas inválidas (JSON quebrado, chave faltando,
base64 inválido, bytes que não são imagem, payload vazio) dá a mesma
resposta de erro nos dois handlers.

## Contrato de I/O

Idêntico ao par amd64 — ver o contrato completo lá. Resumo:

```
entrada:  {"image_base64": "<jpg ou png em base64>"}
saída:    {"status": "success", "image_base64": "<jpg anotado>",
           "image_format": "jpeg", "image_size": {...},
           "detections": [{"class": ..., "confidence": ..., "box": {...}}, ...],
           "inference_ms": 42.0}
       ou {"status": "error", "message": "..."}
```

## Contrato de Prontidão (ADR-0008, Emenda 2026-09-14, §3)

`index.py` com bind-first + lazy-load **e warmup-at-boot** (`_warmup`:
inferência dummy antes de `_ready.set()`, fail-open, `SKIP_WARMUP=1`
desliga). Mesmos códigos: `GET /_/ready` 200/503, `POST /_/ready`
sempre 404, `POST /` 503 até pronto, `GET` desconhecido 404.

> Nota: durante a janela de readiness o log do contêiner mostra
> `BrokenPipeError` traceado pelo `socketserver` quando o poll do
> watchdog recebe o 503 e fecha a conexão. É cosmético e vem do
> `index.py` compartilhado com o `alpr`/`alpr-ncnn-slim` (o watchdog
> segue pollando e a instância fica pronta) — não é regressão deste
> exemplo.

## Estrutura

```
function/handler.py       # pipeline ncnn+numpy, sem torch
function/requirements.txt # ncnn + numpy + opencv-headless
index.py                  # mesmo contrato do alpr-ncnn, com warmup
yolo26n_ncnn_model/       # trio do export ncnn (bin+param+metadata)
coco80.names              # 80 nomes de classe (texto puro, do .pt)
Dockerfile                # SOMENTE arm64
```

## Preparar

```bash
# a partir da raiz do repositório. O export congela o shape de entrada
# (640×640), então o IMAGE_SIZE do handler/Dockerfile precisa coincidir
# com o imgsz do export.
yolo export model=yolo26n.pt format=ncnn imgsz=640
# ou, para metade do tamanho do .bin com penalidade pequena de
# precisão (medida em x86: mesmas classes/contagens, max Δconf 0.0067,
# max Δbox 0.8 px, 12/20 imagens byte-idênticas):
#   yolo export model=yolo26n.pt format=ncnn imgsz=640 quantize=16

mkdir -p examples/yolo26n-coco-ncnn-slim/yolo26n_ncnn_model
cp yolo26n_ncnn_model/model.ncnn.bin \
   yolo26n_ncnn_model/model.ncnn.param \
   yolo26n_ncnn_model/metadata.yaml \
   examples/yolo26n-coco-ncnn-slim/yolo26n_ncnn_model/

# os nomes de classe vêm do .pt, em texto puro (o handler não tem pyyaml
# para ler o metadata.yaml):
python3 -c "
from ultralytics import YOLO
names = YOLO('yolo26n.pt').names
assert list(names) == list(range(80))
open('examples/yolo26n-coco-ncnn-slim/coco80.names','w').write(
    ''.join(names[i] + '\n' for i in range(80)))
"

# compilar o watchdog-lite para a TVBox (arm64)
GOOS=linux GOARCH=arm64 go build -o examples/yolo26n-coco-ncnn-slim/watchdog-lite ./cmd/watchdog-lite
```

Se trocar de modelo, regere o `coco80.names` junto — o handler aborta
com `modelo tem N classes, ... tem M` se a contagem não bater.

O `imgsz=640` do export é o que amarra os dois exemplos: o
`model.ncnn.param` não grava a shape de entrada de forma declarativa
(o `Input in0` é shape-less), mas o grafo é construído para 8400
anchors (80²+40²+20²) e o `metadata.yaml` registra `imgsz: [640, 640]`.
Os dois handlers passam `IMAGE_SIZE=640` e letterbox quadrado por causa
disso — se o `.pt` usasse o retângulo mínimo do `yolo predict`
(`rect=True`) e o ncnn o quadrado, os dois divergiriam no
pré-processamento (Δconf de até 0,06, medido; ver o README do par
amd64).

## Build e teste com Docker

```bash
cd examples/yolo26n-coco-ncnn-slim
docker buildx build --platform linux/arm64 -t faas-continuum/yolo26n-coco-ncnn-slim:latest .
```

Tamanho medido 2026-09-28 (build cross via qemu; `/home/app/python` com
ncnn 11 MB + numpy 51 MB + opencv-headless 80 MB):

| Item | Tamanho |
|---|---|
| Imagem final | **811 MB** (o `chown -R` duplica ~200 MB de camada) |
| `/home/app/python` | 207 MB |
| `yolo26n_ncnn_model/` | 9,3 MB |

Compare com o par amd64 +cpu: 2,46 GB. É a mesma pegada de imagem do
`alpr-ncnn` (~1 GB com ultralytics) espremida sem torch.

```bash
# o processo roda como `app` (não-root): o bind mount do socket precisa
# permitir escrita, senão "listen unix ...: bind: permission denied".
rm -rf /tmp/yolo-arm-sockets && mkdir -p /tmp/yolo-arm-sockets && chmod 777 /tmp/yolo-arm-sockets
docker run --rm -d --name yolo-arm \
  -v /tmp/yolo-arm-sockets:/run/faas-continuum-socket \
  faas-continuum/yolo26n-coco-ncnn-slim:latest

python3 -c "
import base64, json
b64 = base64.b64encode(open('/caminho/para/uma/foto.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json
curl -s --unix-socket /tmp/yolo-arm-sockets/handle.sock http://localhost/ -d @/tmp/payload.json
```

## Publicar (registry `igoricda`)

Tags: `docker.io/igoricda/yolo26n-coco-ncnn-slim:latest`. Build
**nativo na TVBox** de preferência (cross via qemu é lento); o contexto
precisa do `watchdog-lite` arm64 (ver "Preparar").

## Registro no agente (timeout por função)

O cold do slim é de segundos (sem torch), então o `ENV READY_TIMEOUT=60s`
dá margem folgada e o `timeout_ms` pode ser bem mais apertado que no par
amd64 — que ainda paga 30-60 s de import. Para uma TVBox (Cortex-A55),
comece conservador em 60 s e recalibre na primeira medição:

```bash
curl -i -X POST "$BASE_AGENTE/function/yolo26n-coco-ncnn-slim" \
  -H "Authorization: $ADMIN_NÓ" -H "Content-Type: application/json" -d '{
    "image_ref": "docker.io/igoricda/yolo26n-coco-ncnn-slim:latest",
    "min_instances": 1,
    "timeout_ms": 60000
  }'
# 201 → {"function":{...},"invoke_token":"..."}  GUARDE o invoke_token!
```

Invocação: mesmo envelope `{"input": {"image_base64": ...}}` do par
amd64; o nó repassa só o `input` ao handler.

## Knobs (env)

Os mesmos do par amd64, mais dois deste lado:

| Variável | Padrão | Efeito |
|---|---|---|
| `MODEL_PATH` | `/home/app/yolo26n_ncnn_model` | diretório do export ncnn |
| `NAMES_PATH` | `/home/app/coco80.names` | nomes de classe (texto puro) |
| `N_THREADS` | `0` | `0` = default do ncnn; `>0` fixa (MODELS.md §2 validou 4 no ARM) |
| `CONFIDENCE_THRESHOLD` | `0.25` | DEFAULT_CFG `conf` |
| `IOU_THRESHOLD` | `0.7` | DEFAULT_CFG `iou` (NMS) |
| `MAX_DET` | `300` | DEFAULT_CFG `max_det` |
| `IMAGE_SIZE` | `640` | tem de bater com o `imgsz` do export |
| `ANNOTATE` | `1` | `0` devolve só as detecções |
| `JPEG_QUALITY` | `85` | qualidade do jpg anotado |
| `ANNOTATE_MAX_SIDE` | `0` | `>0` reduz a imagem anotada p/ caber no teto de 8 MiB |

## Latência medida

- **x86 (execução local do handler ncnn):** warm 50-60 ms de parede,
  `inference_ms` 42-52 (mediana de 5, `bus.jpg` com 5 detecções).
  `import` do handler 0,13 s.
- **contêiner arm64 sob qemu (funcional, NÃO é número de ARM):** a
  invocação responde HTTP 200 com as 5 detecções corretas; o
  `inference_ms` de 14,6 s é quase todo overhead de emulação (o binário
  aarch64 roda instrução-por-instrução). Sirve para provar que a imagem
  sobe e responde, não para dimensionar a TVBox.
- **ARM (TVBox):** **não medido para este modelo ainda.** O `alpr-ncnn`
  (mesmo backend, YOLO26-OBB 640) mediu ~270 ms/invocação quente na
  TVBox como referência de ordem de grandeza; o `yolo26n` COCO tem 80
  classes em vez de 1, então o NMS custa um pouco mais, com a mesma rede
  nano.

## O que foi verificado (2026-09-28)

| Verificação | Resultado |
|---|---|
| `py_compile` no handler + `index.py` | OK |
| Contêiner arm64 (qemu) ponta a ponta: `GET /_/ready` 200, `POST /_/ready` 404, `POST /` 200 | 5 detecções, mesmas classes do contêiner amd64 |
| Paridade vs par `.pt` (20 fotos reais + 15 sintéticas + contêiner arm64) | mesma contagem e classes em 36/36; tabela acima |
| `ANNOTATE=0` | resposta de 587 B (pt) / 584 B (slim) sem `image_base64` |
| Build da imagem | 811 MB, `/home/app/python` 207 MB |

O `ncnn` do requirements puxa `opencv-python` (não-headless) + tqdm +
requests + portalocker como dependências declaradas; o handler usa só
`ncnn.Net`/`ncnn.Mat` do módulo compilado, então os dois módulos extras
(`ncnn/model`, `ncnn/utils`) nunca são importados. Eles ficam na imagem
por compatibilidade com o `alpr-ncnn-slim` — uma otimização
possível é trocar `opencv-python-headless` por um `pip install --no-deps`
do ncnn, o que cortaria ~64 MB de `opencv_python.libs` (Qt5/ffmpeg) e a
dependência de `libgl1`; **não foi aplicado aqui** para não divergir do
requirements do `alpr-ncnn-slim` (validado na TVBox) sem medir de novo
no box.
