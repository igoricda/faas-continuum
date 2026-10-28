"""Hello world function.

It has no heavy state to load, unlike the detection examples that load a
model at import time, so it exercises the execution contract itself, the
watchdog, the HTTP process and the handler, with a minimal cold start.

Input and output contract:

    input:   a raw string, the body of the request; an empty body becomes
             "world"
    output:  valid JSON, which is mandatory, since the node rejects with a
             bad gateway any answer that is not JSON:
             {"message": "Hello, <input>!"}
"""

import json


def handle(req: str) -> str:
    name = req.strip() or "world"
    return json.dumps({"message": f"Hello, {name}!"})
