"""
Simulação de ingestão STREAMING (tempo quas real).

Gera eventos sintéticos de atualização (nova medição de desempenho,
atualizaçoo de indicador/meta por municipio), um de cada vez, com um
intervalo entre eles.
Reproduzindo o padrão de chegada que um producer Kafka real teria. Cada evento é gravado direto na
camada Bronze, simulando um sink de streaming.
"""
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.utils.paths import BRONZE_DIR

EVENT_TYPES = [
    "atualizacao_indicador_municipio",
    "atualizacao_meta_municipio",
    "nova_medicao_desempenho",
]

UFS_EXEMPLO = ["SP", "RJ", "MG", "BA", "CE", "RS", "PR", "PE", "PA", "AM"]


def generate_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "uf_sigla": random.choice(UFS_EXEMPLO),
        "ano": 2024,
        "taxa_alfabetizacao": round(random.uniform(30, 90), 2),
        "meta_pactuada": round(random.uniform(50, 85), 2),
    }


def append_event_to_bronze(event: dict) -> Path:
    ingestion_date = datetime.now(timezone.utc).date().isoformat()
    out_dir = BRONZE_DIR / "streaming_eventos" / f"ingestion_date={ingestion_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"evento_{event['event_id']}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(event, f, ensure_ascii=False, indent=2)
    return out_path


def run(n_events: int, interval_seconds: float) -> None:
    print(f"Iniciando simulação: {n_events} eventos, intervalo de {interval_seconds}s")
    for i in range(1, n_events + 1):
        event = generate_event()
        path = append_event_to_bronze(event)
        print(f"({i}/{n_events}) evento={event['event_type']} uf={event['uf_sigla']} -> {path.name}")
        if i < n_events:
            time.sleep(interval_seconds)
    print("Simulação concluída.")


if __name__ == "__main__":
    run(n_events=15, interval_seconds=1.0)