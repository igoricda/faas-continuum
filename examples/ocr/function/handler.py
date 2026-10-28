"""OCR function, which reads the license plate.

It is the second stage of the license plate pipeline: it receives the crop
of the plate, since the perspective warp of the oriented box happens in the
middle of the pipeline and not here, and it returns the text of the plate.
The model is the same for both architectures, the small variant of the
recognition model, which is 3.2 MB: it reaches the same accuracy as the
larger variant with a much smaller runtime, about four times smaller on x86
and about eleven times smaller on ARM.

The recognizer is loaded once, at the import of the module, which is what the
watchdog exploits by keeping the process alive between invocations. The
handler is called repeatedly by the HTTP process on every request received.

Input and output contract:

    input:   {"image_base64": "<crop of the plate, jpg or png in base64>"}
    output:  {"status": "success", "plate": "<text>", "region": "<region>"}
             or {"status": "error", "message": "..."}
"""
import base64
import binascii
import io
import json
import os

import numpy as np
import onnxruntime as ort
from fast_plate_ocr import LicensePlateRecognizer
from PIL import Image

MODEL_PATH = os.environ.get("MODEL_PATH", "./best.onnx")
CONFIG_PATH = os.environ.get("CONFIG_PATH", "./cct_xs_v2_global_plate_config.yaml")

# The cores used by the ONNX runtime. It was validated with four threads on
# ARM, with forty of forty correct readings and a median latency of 36.4 ms;
# zero means the default of the runtime, which uses every core.
_ort_threads = int(os.environ.get("ORT_THREADS", "0"))
_sess_options = ort.SessionOptions()
if _ort_threads > 0:
    _sess_options.intra_op_num_threads = _ort_threads
    _sess_options.inter_op_num_threads = _ort_threads

# Loaded only once, since the cold start of loading the model cannot happen
# on every invocation. The automatic device uses CUDA when the GPU build of
# the runtime is installed and a GPU is present, and the CPU otherwise.
recognizer = LicensePlateRecognizer(
    onnx_model_path=MODEL_PATH,
    plate_config_path=CONFIG_PATH,
    device="auto",
    sess_options=_sess_options,
)


def handle(req: str) -> str:
    try:
        data = json.loads(req)
        image_bytes = base64.b64decode(data["image_base64"], validate=True)
    except (KeyError, ValueError, TypeError, binascii.Error) as exc:
        return json.dumps({"status": "error", "message": f"invalid input: {exc}"})

    try:
        # The model expects RGB and not BGR, with the channels last and the
        # values as bytes; the resize and the padding follow the configuration
        # of the plate and the normalization by 255 is inside the graph. The
        # run of the library applies all of that from the array.
        crop = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        img = np.asarray(crop, dtype=np.uint8)
    except Exception as exc:
        return json.dumps({"status": "error", "message": f"invalid or corrupted image: {exc}"})

    try:
        result = recognizer.run(img)[0]
    except Exception as exc:  # The inference can fail on a degenerate crop.
        return json.dumps({"status": "error", "message": f"inference failed: {exc}"})

    return json.dumps({"status": "success", "plate": result.plate, "region": result.region})
