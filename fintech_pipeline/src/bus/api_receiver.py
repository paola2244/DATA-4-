"""
api_receiver.py — Receptor HTTP genérico para el pipeline fintech.

Acepta mensajes de cualquier tipo (eventos, métricas, registros, logs, alertas)
a través del endpoint /ingest y los encola en el EventBus compartido.
El BronzeConsumer los escribe a Parquet; el tipo de mensaje determina
en qué sub-carpeta de Bronze se almacena.

Arranque:
    uvicorn src.bus.api_receiver:app --port 8000 --reload

Endpoints:
    POST /ingest            → Ingesta un mensaje de cualquier tipo (202 Accepted)
    POST /eventos           → Alias legacy — redirige a /ingest (backward compat)
    GET  /health            → Estado del bus y consumidor
    GET  /pipeline/status   → Estadísticas detalladas de bus, consumer y trigger
    POST /pipeline/run      → Fuerza ejecución inmediata de Silver + Gold
    DELETE /pipeline/flush  → Procesa el batch pendiente inmediatamente
"""

import asyncio
import os
import sys
from typing import Any, Dict

from fastapi import FastAPI, HTTPException

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.bus.event_bus_asyncio import BronzeConsumer, EventBus
from src.bus.pipeline_trigger import PipelineTrigger

# ── Singletons compartidos ─────────────────────────────────────────────────────
# Se crean una vez al importar el módulo y los comparten todos los endpoints.

_bus = EventBus(maxsize=1000)
_trigger = PipelineTrigger(auto_trigger=True, min_intervalo_segundos=60)
_consumer = BronzeConsumer(
    _bus,
    batch_size=50,
    flush_interval_segundos=10,
    trigger=_trigger,
)

# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Fintech Ingest Receiver",
    description=(
        "Receptor HTTP genérico del pipeline fintech. "
        "Acepta eventos, métricas, registros, logs y alertas via POST /ingest. "
        "Abre /docs para ver los endpoints disponibles."
    ),
    version="3.0",
)


@app.on_event("startup")
async def startup():
    """Arranca el BronzeConsumer como tarea background cuando inicia uvicorn."""
    asyncio.create_task(_consumer.start())
    print("[Receiver] BronzeConsumer iniciado en background")
    print("[Receiver] Listo para recibir mensajes en POST /ingest")


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.post("/ingest", status_code=202, tags=["Ingesta"])
async def ingestar_mensaje(mensaje: Dict[str, Any]):
    """
    Endpoint genérico de ingesta. Acepta cualquier tipo de mensaje del bus:
    event, metric, record, log, alert.

    El BronzeConsumer detecta el tipo mediante `msg_type` (envelope estándar)
    o la presencia de `detail.payload` (formato legacy fintech) y elige
    la lógica de almacenamiento apropiada.

    Retorna 202 Accepted inmediatamente — el procesamiento ocurre en background.
    """
    await _bus.publish(mensaje)
    return {
        "status": "accepted",
        "msg_type": mensaje.get("msg_type", "legacy_event"),
        "queue_pending": _bus.pending,
        "total_received": _bus.stats()["total_published"],
    }


@app.post("/eventos", status_code=202, tags=["Ingesta"])
async def recibir_evento_legacy(evento: Dict[str, Any]):
    """
    Alias de backward compatibility para /ingest.
    Acepta el formato legacy (detail.payload) que usaba el pipeline original.
    Nuevas integraciones deben usar POST /ingest con el envelope estándar.
    """
    await _bus.publish(evento)
    return {
        "status": "accepted",
        "msg_type": "legacy_event",
        "queue_pending": _bus.pending,
        "total_received": _bus.stats()["total_published"],
    }


@app.get("/health", tags=["Monitoreo"])
async def health():
    """Estado rápido del receptor y el bus."""
    return {
        "status": "ok",
        "bus_pending": _bus.pending,
        "consumer_batches": _consumer.stats()["batches_guardados"],
        "consumer_eventos": _consumer.stats()["eventos_guardados"],
    }


@app.get("/pipeline/status", tags=["Pipeline"])
async def pipeline_status():
    """Estadísticas completas: bus, consumidor Bronze y trigger Silver/Gold."""
    return {
        "bus": _bus.stats(),
        "bronze_consumer": _consumer.stats(),
        "trigger": _trigger.stats(),
    }


@app.post("/pipeline/run", tags=["Pipeline"])
async def run_pipeline():
    """
    Fuerza la ejecución inmediata de Silver → Gold, ignorando el throttling.

    Útil para solicitar un refresh manual después de recibir muchos eventos
    o para cerrar el pipeline al final de una sesión de pruebas.
    """
    ejecutado = _trigger.trigger(force=True)
    return {
        "status": "triggered" if ejecutado else "ya_en_ejecucion",
        "runs_completados": _trigger.runs_completados,
    }


@app.delete("/pipeline/flush", tags=["Pipeline"])
async def flush_queue():
    """
    Fuerza el procesamiento inmediato de todos los eventos pendientes en la cola.
    Útil al final de una sesión de pruebas para no perder el último batch parcial.

    Nota: retorna inmediatamente; el flush ocurre en background.
    """
    pendientes = _bus.pending
    if pendientes == 0:
        return {"status": "cola_vacia", "eventos_procesados": 0}

    # El consumer procesará el batch en el próximo ciclo de flush
    # Forzar trigger posterior para que Silver/Gold vean los nuevos datos
    _trigger.trigger(force=True)
    return {
        "status": "flush_solicitado",
        "eventos_en_cola": pendientes,
        "nota": "El batch se guardará en el próximo ciclo del consumer (~10s)",
    }
