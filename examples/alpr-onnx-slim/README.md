# ALPR na névoa — backend ONNX Runtime (faas-continuum, amd64)

Detector de placa (classe única `License-plate`, OBB probiou) para nós
**x86** (névoa, ex. `ccsc-lscdesktop6`), seguindo o contrato de execução
da ADR-0008 como os demais exemplos: `watchdog-lite` é o PID 1, sobe o
`FPROCESS` e faz proxy reverso HTTP das invocações pelo socket Unix.

Família ALPR:

| Exemplo / imagem | Backend | Nó-alvo |
|---|---|---|
| `examples/alpr` | torch + ultralytics | GPU/servidor |
| `examples/alpr-ncnn-slim` | ncnn | borda ARM (TVBox) |
| `azure_functions/alpr` (slim ACA) | onnxruntime | nuvem (Container Apps) |
| **este** | onnxruntime | névoa x86 (.94) |

O handler é o **mesmo** de `azure_functions/alpr` (paridade com o handler
torch já validada: mesmo N/classe/rank, conf ±0.002, OBB ±1px em 5
imagens). Aqui só muda o envelope de execução local: `index.py`
(loopback do watchdog-lite, `/_/ready` + warmup + `_unwrap`) e a imagem
com `watchdog-lite` embutido, sem o host do Azure Functions.

## Entrada / saída (contrato da ADR-0008)

```jsonc
// entrada (input cru do nó; {"input": ...} também é aceito)
{"image_base64": "<jpg ou png em base64>"}

// saída
{"status": "success", "detections": [{"class": "License-plate",
  "confidence": 0.9, "obb": {"x": 0, "y": 0, "w": 0, "h": 0, "rotation_rad": 0}}]}
{"status": "error", "message": "..."}
```

## Build (a partir da raiz do repositório)

```sh
GOOS=linux GOARCH=amd64 go build -o examples/alpr-onnx-slim/watchdog-lite ./cmd/watchdog-lite
docker buildx build --platform linux/amd64 \
  -t igoricda/alpr-onnx-slim:latest \
  -f examples/alpr-onnx-slim/Dockerfile examples/alpr-onnx-slim
```

`watchdog-lite` é gitignored (`/examples/*/watchdog-lite`) — cada
checkout gera o seu.

## Smoke local (sem agente)

```sh
mkdir -p /tmp/sock && chmod 777 /tmp/sock
docker run -d --name alpr-smoke -v /tmp/sock:/run/faas-continuum-socket \
  igoricda/alpr-onnx-slim:latest
# POST de azure_functions/testdata/alpr-payload.json no socket:
#   /run/faas-continuum-socket/handle.sock  (ver clientes dos outros exemplos)
```

## Deploy no nó (quando o caso ALPR rodar)

1. Registrar a função no agente (nome sugerido `alpr`, categoria `lpr`)
   com `timeout_ms`/`idle_timeout` do experimento;
2. incluir `"alpr":"lpr"` no `-function-categories` do agente (o nó
   anuncia a categoria ao Árbitro);
3. `ctr -n faas-continuum images pull docker.io/igoricda/alpr-onnx-slim:latest`
   (pre-pull opcional) e reiniciar o agente para reler o registry.
