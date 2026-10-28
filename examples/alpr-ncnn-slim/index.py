"""Minimal HTTP process expected by the watchdog: it listens on
127.0.0.1:$PORT and calls handle() on every request received. The watchdog
talks to this process over the loopback interface of the container, so no
other network configuration is needed.

Bind first and lazy load: the socket is opened BEFORE the heavy import of
function.handler, which loads the ncnn model directly once. That import runs
in a background thread, followed by a dummy inference, and only then is the
ready event signaled; until then the readiness route and the post route answer
with a rejection."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

_ready = threading.Event()
_handle = None


def _warmup(handle_fn):
    """Best effort dummy inference before the readiness is signaled.

    The first forward pays the page-in of the native libraries on their first
    use. Paying that cost here, while the ready event is still unset, moves
    the plateau into the readiness measurement, so the first real invocation
    pays only the warm path instead of the readiness plus the cold function.
    It fails open by design: a failure is only logged and the readiness still
    follows, since the warmup can never block a healthy instance.
    """
    if os.environ.get("SKIP_WARMUP", "").lower() in ("1", "true", "yes"):
        print("index.py: warmup skipped (SKIP_WARMUP)", flush=True)
        return
    try:
        import base64

        import cv2
        import numpy as np

        img = np.zeros((64, 64, 3), dtype=np.uint8)
        ok, buf = cv2.imencode(".jpg", img)
        if not ok:
            raise RuntimeError("cv2.imencode failed during the warmup")
        payload = json.dumps(
            {"image_base64": base64.b64encode(buf.tobytes()).decode("utf-8")}
        )
        handle_fn(payload)
        print("index.py: warmup OK", flush=True)
    except Exception as exc:  # Fail open: log it and signal the readiness.
        print(f"index.py: warmup failed (fail open): {exc}", flush=True)


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
    print(f"index.py: listening on 127.0.0.1:{port}", flush=True)
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
