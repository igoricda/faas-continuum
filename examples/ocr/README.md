# Exemplo: função OCR (leitura da placa)

Segundo estágio do pipeline ALPR contra o contrato da ADR-0008
(`docs/ADR-0008-contrato-execucao-funcao.md`). Recebe o **recorte da
placa** (o warp de perspectiva do quad OBB acontece no meio do pipeline,
fora daqui) e devolve a string da placa em JSON.

## Modelo (MODELS.md §1–§2)

Imagem **multi-arquitetura** (amd64 + arm64) com o **mesmo** modelo:
`cct-xs-v2` finetuned (`tests/finetune/exported_xs/best.onnx`, 3.2 MB,
`onnxruntime`) — accuracy igual à do `cct-s-v2` (plate_acc 0.9864 vs
0.9868, char_acc 0.9985) com runtime muito menor (~4× no x86, ~11× no
ARM). O `s` fica como fallback fora da imagem.

Entrada do modelo: **RGB (não BGR)**, NHWC uint8 `[1,64,128,3]`, `/255`
dentro do grafo. Saídas: `plate` (10 slots) + `region`, decodificadas
com o alfabeto `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_`, pad `_`
(ver `tests/finetune/arm/ocr_ort.py` como referência).

Stack mínima (MODELS.md §5, "OCR, server sidecar"): `onnxruntime`,
`numpy`, `opencv-headless` — mais `fast-plate-ocr` (pré/pós-processa
exatamente como o treino) e `pillow`. Sem torch, sem tensorflow, sem
ultralytics.

> Nota: no venv on-device a receita validada usa `apt python3-opencv` +
> stub (MODELS.md §6, `yoloscripts/arm/setup_arm_venv.sh`). Dentro do
> Docker isso não é preciso: `opencv-python-headless==4.10.0.84` tem
> wheel `manylinux_aarch64` no PyPI (verificado na resolução para
> cp311-aarch64), então o `pip install` puro funciona nas duas
> arquiteturas.

## Contrato de Prontidão (ADR-0008, Emenda 2026-09-14, §3)

O `index.py` implementa **bind-first + lazy-load** (mesmo padrão do
alpr/hello-world):

- `HTTPServer` faz `bind` em `127.0.0.1:$PORT` **antes** de importar
  `function.handler` (onde o recognizer é carregado uma única vez).
- Thread daemon carrega `handle()` e sinaliza `threading.Event _ready`.
- **GET `/_/ready`**: `200 {"ready": true}` se pronto, `503` caso contrário.
- **POST `/_/ready`**: **sempre `404`**.
- **POST `/`**: `503` enquanto carrega, `200` + JSON válido após `_ready`.
- **GET desconhecido**: `404`.

## Estrutura

```
handler.py -> function/handler.py   # handle(req): recognizer 1x, chamado por invocação
index.py                            # servidor HTTP mínimo esperado pelo watchdog-lite
function/requirements.txt           # fast-plate-ocr + onnxruntime + opencv-headless + numpy + pillow
best.onnx                           # pesos xs finetuned (ver "Preparar" abaixo)
cct_xs_v2_global_plate_config.yaml  # config da placa (alfabeto, slots, regiões)
Dockerfile                          # imagem final multi-arch, com watchdog-lite como processo principal
```

## Preparar

```bash
# a partir da raiz do repositório — o modelo e o config vivem no repo LPR
# (caminhos repo-relativos pós-clone, ver MODELS.md §1):
cp <LPR>/tests/finetune/exported_xs/best.onnx examples/ocr/best.onnx
cp <LPR>/tests/finetune/config/cct_xs_v2_global_plate_config.yaml \
  examples/ocr/cct_xs_v2_global_plate_config.yaml

# compilar o watchdog-lite para as duas arquiteturas (o Dockerfile
# multi-arch copia watchdog-lite-$TARGETARCH — ver Dockerfile)
GOOS=linux GOARCH=amd64 go build -o examples/ocr/watchdog-lite-amd64 ./cmd/watchdog-lite
GOOS=linux GOARCH=arm64 go build -o examples/ocr/watchdog-lite-arm64 ./cmd/watchdog-lite
```

## Build e teste com Docker

```bash
cd examples/ocr
docker buildx build --platform linux/amd64,linux/arm64 \
  -t faas-continuum/ocr:latest .
docker run --rm -d --name ocr-test \
  -v /tmp/ocr-sockets:/run/faas-continuum-socket \
  faas-continuum/ocr:latest
```

## Publicar multi-arch (registry `igoricda`)

Tag publicada: `docker.io/igoricda/ocr:latest` (manifesto amd64+arm64).
Build emulado via qemu é lento — o fluxo usado é build nativo por arch
+ manifesto:

```bash
# amd64 (no servidor): build + push da arch
docker build --platform linux/amd64 -t igoricda/ocr:amd64 -f examples/ocr/Dockerfile examples/ocr
docker push igoricda/ocr:amd64
# arm64 (na TVBox, contexto com watchdog-lite-arm64): mesmo, tag :arm64
# manifesto único combinando as duas archs:
docker buildx imagetools create -t igoricda/ocr:latest \
  igoricda/ocr:amd64 docker.io/igoricda/ocr:arm64
```

Na TVBox via agente: primeira invocação ~440 ms (init da sessão ORT),
quente ~50 ms por recorte (medido 2026-09-23, `ORT_THREADS` default).

# monta o payload (RECORTE da placa em base64) e invoca via socket Unix montado
python3 -c "
import base64, json
b64 = base64.b64encode(open('/caminho/para/o/recorte-da-placa.jpg','rb').read()).decode()
print(json.dumps({'image_base64': b64}))
" > /tmp/payload.json

curl -s --unix-socket /tmp/ocr-sockets/handle.sock http://localhost/ -d @/tmp/payload.json
# -> {"status": "success", "plate": "ABC1D23", "region": "Brazil"}

Invocação via agente: o nó espera o envelope `{"input": ...}`
(modelo-dados-schemas.md §5) e repassa só o conteúdo de `input` à
função — sem o envelope, `handle()` recebe corpo vazio e devolve
`{"status": "error", ...}`:

```bash
python3 -c "
import base64, json
b64 = base64.b64encode(open('/caminho/para/o/recorte-da-placa.jpg','rb').read()).decode()
print(json.dumps({'input': {'image_base64': b64}}))
" > /tmp/invoke.json

curl -s -X POST "$BASE_AGENTE/function/ocr/invoke" \
  -H "Authorization: $INVOKE_TOKEN" -H "Content-Type: application/json" \
  -d @/tmp/invoke.json
```
```

Variante GPU (servidor com CUDA), sem tocar no requirements:

```bash
docker buildx build --build-arg ONNXRUNTIME_PKG=onnxruntime-gpu \
  -t faas-continuum/ocr:gpu -f examples/ocr/Dockerfile examples/ocr
```

No ARM, o número de núcleos do ORT sai de `ORT_THREADS` (default 0 =
padrão do ORT; 4 é o valor validado em `tests/ARM_VALIDATION.md`):

```bash
docker run --rm -d --name ocr-test -e ORT_THREADS=4 \
  -v /tmp/ocr-sockets:/run/faas-continuum-socket \
  faas-continuum/ocr:latest
```

## Registro no agente (timeout por função)

O cold import aqui (cv2/onnxruntime/fast-plate-ocr) é de segundos — o
default 90s do nó limpa `ENV READY_TIMEOUT=90s` com margem, então basta:

```bash
curl -i -X POST "$BASE_AGENTE/function/ocr" \
  -H "Authorization: $ADMIN_NÓ" -H "Content-Type: application/json" -d '{
    "image_ref": "faas-continuum/ocr:latest",
    "min_instances": 1
  }'
# 201 → {"function":{...},"invoke_token":"..."}  GUARDE o invoke_token!
```
