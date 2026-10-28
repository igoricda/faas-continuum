"""Sleep function.

It has no heavy state to load, like the hello world example, so it exercises
invocations with a controlled duration: the sleep is the workload, which is
useful to test timeouts, objectives and scaling without depending on a model
or on any input and output.

Input and output contract, with the status and message pattern of the other
examples, where the output is valid JSON because the node rejects with a bad
gateway any answer that is not JSON:

    input:   a bare number in the body ("3") or the JSON {"seconds": 3}
    output:  {"status": "success", "message": "slept for X seconds"}
             or {"status": "error", "message": "..."}
"""

import json
import time


def handle(req: str) -> str:
    try:
        data = json.loads(req)
    except ValueError as exc:
        return json.dumps({"status": "error", "message": f"invalid input: {exc}"})

    # It accepts both the bare number and the object {"seconds": N}, since the
    # input of an invocation request is an object, but a bare number is valid
    # JSON as well.
    if isinstance(data, dict):
        seconds = data.get("seconds")
    else:
        seconds = data

    # A boolean is a subclass of the integer in Python, so it has to be
    # excluded first.
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        return json.dumps({
            "status": "error",
            "message": f"invalid input: expected a number of seconds, got {type(seconds).__name__}",
        })
    if seconds < 0:
        return json.dumps({"status": "error", "message": "invalid input: the number of seconds cannot be negative"})

    time.sleep(seconds)
    return json.dumps({"status": "success", "message": f"slept for {seconds:g} seconds"})
