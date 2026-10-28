"""Função Hello World contra o contrato da ADR-0008.

Não tem estado pesado para carregar (diferente do alpr, que carrega
o modelo no import) — serve para exercitar o contrato em si
(watchdog-lite + index.py + handle()) com cold start mínimo.

Contrato de entrada/saída:

    entrada:  string crua (corpo da requisição); vazia vira "world"
    saída:    JSON válido (obrigatório — o agente rejeita com 502 qualquer
              resposta que não seja JSON, ADR-0008 item 3):
              {"message": "Hello, <entrada>!"}
"""

import json


def handle(req: str) -> str:
    name = req.strip() or "world"
    return json.dumps({"message": f"Hello, {name}!"})
