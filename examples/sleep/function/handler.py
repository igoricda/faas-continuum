"""Função Sleep contra o contrato da ADR-0008.

Sem estado pesado para carregar (como o hello-world) — serve para
exercitar invocações com duração controlada: o tempo de sleep é o
workload, útil para testar timeouts, SLO e escalonamento sem depender
de modelo ou I/O.

Contrato de entrada/saída (padrão status/message do alpr; a saída
é JSON válido por obrigação de contrato — o agente rejeita com 502
qualquer resposta que não seja JSON, ADR-0008 item 3):

    entrada:  número cru no corpo ("3") ou JSON {"seconds": 3}
    saída:    {"status": "success", "message": "slept for X seconds"}
           ou {"status": "error", "message": "..."}
"""

import json
import time


def handle(req: str) -> str:
    try:
        data = json.loads(req)
    except ValueError as exc:
        return json.dumps({"status": "error", "message": f"entrada inválida: {exc}"})

    # Aceita tanto o número cru quanto o objeto {"seconds": N} — o campo
    # input de InvocationRequest é object (modelo-dados-schemas.md §5),
    # mas um número solto também é JSON válido.
    if isinstance(data, dict):
        seconds = data.get("seconds")
    else:
        seconds = data

    # bool é subclasse de int em Python — precisa ser excluído antes.
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        return json.dumps({
            "status": "error",
            "message": f"entrada inválida: esperado número de segundos, recebido {type(seconds).__name__}",
        })
    if seconds < 0:
        return json.dumps({"status": "error", "message": "entrada inválida: segundos não pode ser negativo"})

    time.sleep(seconds)
    return json.dumps({"status": "success", "message": f"slept for {seconds:g} seconds"})
