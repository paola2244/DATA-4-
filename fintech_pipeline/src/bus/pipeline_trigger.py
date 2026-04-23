"""
PipelineTrigger: dispara Silver → Gold después de que BronzeConsumer escribe un batch.

Corre los pipelines en un thread separado (threading) para no bloquear el
event loop de asyncio donde vive el bus. Incluye throttling: no ejecuta
Silver/Gold más de una vez cada `min_intervalo_segundos` para no sobrecargar
la máquina cuando llegan muchos batches seguidos.

Uso desde BronzeConsumer:
    trigger = PipelineTrigger(auto_trigger=True, min_intervalo_segundos=30)
    consumer = BronzeConsumer(bus, trigger=trigger)

Uso manual:
    trigger.trigger(force=True)
    trigger.wait_for_completion(timeout=120)
"""

import os
import sys
import threading
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


class PipelineTrigger:
    """
    Ejecuta Silver → Gold en background después de cada batch Bronze.

    Args:
        auto_trigger:            Si False, trigger() siempre hace no-op (útil para tests).
        min_intervalo_segundos:  Tiempo mínimo entre ejecuciones. Evita saturar disco/CPU.
    """

    def __init__(self, auto_trigger: bool = True, min_intervalo_segundos: int = 30):
        self.auto_trigger = auto_trigger
        self.min_intervalo = min_intervalo_segundos
        self._last_run: datetime | None = None
        self._running = False
        self._lock = threading.Lock()
        self._done_event = threading.Event()
        self.runs_completados = 0

    def trigger(self, force: bool = False) -> bool:
        """
        Lanza Silver → Gold si las condiciones lo permiten.

        Args:
            force: Ignora throttling y el flag auto_trigger. Útil al final del pipeline.

        Returns:
            True si se lanzó la ejecución, False si se saltó.
        """
        if not self.auto_trigger and not force:
            return False

        ahora = datetime.now()
        with self._lock:
            if not force and self._last_run:
                segundos_desde_ultimo = (ahora - self._last_run).total_seconds()
                if segundos_desde_ultimo < self.min_intervalo:
                    print(
                        f"   [Trigger] Saltando Silver/Gold "
                        f"(último hace {segundos_desde_ultimo:.0f}s, mínimo {self.min_intervalo}s)"
                    )
                    return False

            if self._running:
                print("   [Trigger] Silver/Gold ya está corriendo, se omite este trigger")
                return False

            self._running = True
            self._done_event.clear()

        thread = threading.Thread(target=self._ejecutar, daemon=False)
        thread.start()
        return True

    def wait_for_completion(self, timeout: int = 120) -> bool:
        """
        Bloquea hasta que el trigger actual termine o se alcance el timeout.

        Returns:
            True si completó, False si hubo timeout.
        """
        return self._done_event.wait(timeout=timeout)

    def _ejecutar(self) -> None:
        inicio = datetime.now()
        try:
            print("\n   [Trigger] ── Iniciando Silver ──────────────────────")
            from src.silver.pipeline_silver import ejecutar_pipeline_silver
            ejecutar_pipeline_silver()
            
                        # 🔥 SUBIR SILVER
            from src.ingesta.uploader_s3 import subir_parquets
            print("☁️ Subiendo a s3...")
            subir_parquets("data/silver", "silver")

            print("   [Trigger] ── Iniciando Gold ────────────────────────")
            from src.gold.pipeline_gold import ejecutar_pipeline_gold
            ejecutar_pipeline_gold()
            
            print("☁️ Subiendo a s3...")
            subir_parquets("data/gold", "gold")

            duracion = (datetime.now() - inicio).total_seconds()
            self.runs_completados += 1
            self._last_run = datetime.now()
            print(f"   [Trigger] Silver + Gold completados en {duracion:.1f}s\n")

        except Exception as e:
            print(f"   [Trigger] ERROR en Silver/Gold: {e}")

        finally:
            self._running = False
            self._done_event.set()

    def stats(self) -> dict:
        return {
            "auto_trigger": self.auto_trigger,
            "runs_completados": self.runs_completados,
            "activo_ahora": self._running,
            "ultimo_run": self._last_run.isoformat() if self._last_run else None,
        }
