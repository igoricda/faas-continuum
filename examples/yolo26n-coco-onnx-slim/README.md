# YOLO26n COCO na névoa — backend ONNX Runtime (faas-continuum, amd64)

Função de detecção geral YOLO26n (80 classes COCO) para o nó **névoa**
(x86/amd64, ex. `ccsc-lscdesktop6`), seguindo o contrato de execução da
ADR-0008 como os demais exemplos: `watchdog-lite` é o PID 1, sobe o
`FPROCESS` e faz proxy reverso HTTP das invocações que chegam pelo socket
Unix montado pelo agente.

É o terceiro membro da família YOLO26n:

| Exemplo | Backend | Nó-alvo | Cold observado |
|---|---|---|---|
| `examples/yolo26n-coco` | torch CPU + ultralytics | servidor x86 | ~20–30 s |
| `examples/yolo26n-coco-ncnn-slim` | ncnn | borda ARM (TVBox) | ~1–2 s |
| **este** | onnxruntime | névoa x86 (.94) | **~0,8–1,5 s** |

O handler/pós-processamento é o **mesmo** de `azure_functions/yolo26n-coco`
(port 1:1 do par ncnn-slim; paridade com torch/ncnn já validada lá — mesmo
N/classe/rank, conf ±0,002, OBB ±1 px). O que muda aqui é só o envelope de
execução local: `index.py` (loopback do watchdog-lite, com `/_/ready` e
`_unwrap`) no lugar do shim do ACA, e a imagem com `watchdog-lite` embutido
sem o host do Azure Functions.

## Build (a partir da raiz do repositório)

```sh
GOOS=linux GOARCH=amd64 go build -o examples/yolo26n-coco-onnx-slim/watchdog-lite ./cmd/watchdog-lite
docker buildx build --platform linux/amd64 \
  -t igoricda/yolo26n-coco-onnx-slim:latest \
  -f examples/yolo26n-coco-onnx-slim/Dockerfile examples/yolo26n-coco-onnx-slim
```

O binário `watchdog-lite` é compilado FORA do Docker para a arquitetura do
nó (ADR-0001) — ele é gitignored (`/examples/*/watchdog-lite`), então cada
checkout precisa gerar o seu.

## Contrato (ADR-0008)

- `FPROCESS="python3 index.py"`, `UPSTREAM_PORT=8081`,
  `SOCKET_PATH=/run/faas-continuum-socket/handle.sock`, `READY_TIMEOUT=60s`.
- `GET /_/ready` responde 503 até o `handler` (sessão ORT) carregar e o
  warmup dummy rodar; 200 depois. O socket Unix só abre após o 200.
- O agente posta o input **cru** em `POST /` (`invoke_instance.go`);
  `{"input": ...}` também é aceito (desembrulhado no `index.py`).
- Saída: JSON de `handle()` (`{"status":"success", ...}` ou
  `{"status":"error", ...}`), teto de 8 MiB do nó.

## Smoke local (sem agente)

```sh
mkdir -p /tmp/sock && chmod 777 /tmp/sock
docker run -d --name onnx-smoke -v /tmp/sock:/run/faas-continuum-socket \
  igoricda/yolo26n-coco-onnx-slim:latest
docker exec onnx-smoke python3 -c '
import http.client, socket, json
class U(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/run/faas-continuum-socket/handle.sock")
c = U("x"); c.request("GET", "/_/ready"); print(c.getresponse().status)'
```

Saída esperada no cold (log do watchdog-lite): `fprocess pronto em
127.0.0.1:8081` em menos de ~2 s, seguido de `index.py: warmup OK`.

## Deploy no nó

O nó resolve a imagem por referência OCI no provisionamento
(`containerruntime.PullImage`). Para trocar a imagem da função sem re-registrar:

1. Editar `image_ref` no registry local (`/var/lib/faas-continuum/functions.json`,
   mantendo `timeout_ms`/`idle_timeout`/`min_instances`/`max_instances`) — ou
   `PUT /function/yolo26n-coco` na API de gestão do agente;
2. `ctr -n faas-continuum images pull docker.io/igoricda/yolo26n-coco-onnx-slim:latest`
   (pre-pull opcional, evita pagar o pull no primeiro provisionamento);
3. reiniciar o agente (`systemctl restart faas-agente`) para reler o registry.
