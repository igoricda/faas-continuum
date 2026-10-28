"""Processo HTTP mínimo esperado pelo watchdog-lite (ADR-0008): escuta em
127.0.0.1:$PORT e chama handle() a cada requisição recebida. O
watchdog-lite fala com este processo por loopback interno ao contêiner —
nenhuma configuração de rede além disso é necessária.

Bind-first + lazy-load: o socket é aberto ANTES do import pesado de
function.handler (que carrega o modelo ncnn via ultralytics uma única
vez) — esse import roda numa thread de fundo, seguido de uma inferência
dummy de warmup (_warmup), e só então sinaliza _ready; até lá
GET /_/ready e POST / respondem 503."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

_ready = threading.Event()
_handle = None


def _warmup(handle_fn):
    """Inferência dummy best-effort antes de sinalizar prontidão.

    O primeiro `model(...)` paga ~30 s no ARM (page-in das .so nativas
    — libtorch/libopencv/libncnn — no primeiro forward, platô 2 do cold
    start). Rodar esse custo aqui, ainda com `_ready` desligado, move o
    platô para dentro do `readiness_ms`: a primeira invocação real paga
    só warm (~300 ms) em vez de readiness + 32 s de function. Fail-open
    por desenho: qualquer falha só loga e a prontidão segue — warmup
    nunca pode travar uma instância saudável.
    """
    if os.environ.get("SKIP_WARMUP", "").lower() in ("1", "true", "yes"):
        print("index.py: warmup pulado (SKIP_WARMUP)", flush=True)
        return
    try:
        import base64

        import cv2
        import numpy as np

        img = np.zeros((64, 64, 3), dtype=np.uint8)
        ok, buf = cv2.imencode(".jpg", img)
        if not ok:
            raise RuntimeError("cv2.imencode falhou no warmup")
        payload = json.dumps(
            {"image_base64": base64.b64encode(buf.tobytes()).decode("utf-8")}
        )
        handle_fn(payload)
        print("index.py: warmup OK", flush=True)
    except Exception as exc:  # fail-open: loga e segue para _ready
        print(f"index.py: warmup falhou (fail-open): {exc}", flush=True)


def _load():
    global _handle
    from function.handler import handle as _h

    _handle = _h
    try:
        _warmup(_h)
    finally:
        _ready.set()


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/_/ready":
            if _ready.is_set():
                self._send_json(200, {"ready": True})
            else:
                self._send_json(503, {"ready": False})
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/_/ready":
            self.send_response(404)
            self.end_headers()
            return
        if not _ready.is_set():
            self._send_json(503, {"ready": False})
            return
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")
        result = _handle(body)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(result.encode("utf-8"))

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    threading.Thread(target=_load, daemon=True).start()
    port = int(os.environ.get("PORT", "8081"))
    print(f"index.py: escutando em 127.0.0.1:{port}", flush=True)
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
