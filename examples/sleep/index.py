"""Processo HTTP mínimo esperado pelo watchdog-lite (ADR-0008): escuta em
127.0.0.1:$PORT e chama handle() a cada requisição recebida. O
watchdog-lite fala com este processo por loopback interno ao contêiner —
nenhuma configuração de rede além disso é necessária.

Bind-first + lazy-load: o socket é aberto ANTES do import pesado de
function.handler (que roda numa thread de fundo e sinaliza _ready); até
lá GET /_/ready e POST / respondem 503."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

_ready = threading.Event()
_handle = None


def _load():
    global _handle
    from function.handler import handle as _h

    _handle = _h
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
