"""
run_agent.py - CLI interactivo para el agente de consultas Gold.

Uso:
    python -X utf8 src/agent/run_agent.py
    python -X utf8 src/agent/run_agent.py --pregunta "Top 5 usuarios por gasto"
    python -X utf8 src/agent/run_agent.py --demo

Requiere:
    ANTHROPIC_API_KEY en .env o variable de entorno del sistema.
"""

import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.agent.agent import agent_query, reset_agent
from src.agent.schema import FEW_SHOT_EXAMPLES

PREGUNTAS_DEMO = [
    "¿Cuáles son los 5 usuarios con mayor gasto total en COP?",
    "¿Qué segmento de usuarios tiene la mayor tasa de transacciones fallidas?",
    "¿Cuál es la distribución de eventos por tipo?",
    "¿Cuántos usuarios llevan más de 7 días sin realizar ninguna transacción?",
]

SEPARADOR = "=" * 65


def imprimir_resultado(pregunta: str, resultado: dict) -> None:
    """Imprime el resultado de una consulta de forma legible en consola."""
    print(f"\n{SEPARADOR}")
    print(f"  PREGUNTA: {pregunta}")
    print(SEPARADOR)

    if resultado.get("error"):
        print(f"\n  ERROR: {resultado['error']}")
        return

    print(f"\n{resultado['answer']}")

    if resultado.get("sql"):
        print(f"\n  SQL ejecutado:")
        for linea in resultado["sql"].strip().splitlines():
            print(f"    {linea}")

    if resultado.get("data") is not None and not resultado["data"].empty:
        df = resultado["data"]
        print(f"\n  Datos ({len(df)} filas x {len(df.columns)} columnas):")
        print(df.to_string(index=False, max_rows=20))

    if resultado.get("chart_b64"):
        print(f"\n  Grafico generado (consulta gold/graficos/ para verlo)")

    print(f"\n{SEPARADOR}\n")


def modo_interactivo() -> None:
    """Bucle de conversacion interactivo en consola."""
    print(f"\n{SEPARADOR}")
    print("  AGENTE FINTECH — Consultas en Lenguaje Natural")
    print("  Modelo: Claude Sonnet 4.5 | Datos: Capa Gold")
    print(f"{SEPARADOR}")
    print("  Escribe tu pregunta en espanol y presiona Enter.")
    print("  Comandos: 'salir' para terminar | 'reset' para nueva sesion")
    print("  'demo' para ejecutar preguntas de ejemplo")
    print(f"{SEPARADOR}\n")

    while True:
        try:
            entrada = input("Pregunta> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n\n  Hasta luego!")
            break

        if not entrada:
            continue

        if entrada.lower() in ("salir", "exit", "quit"):
            print("\n  Hasta luego!")
            break

        if entrada.lower() == "reset":
            reset_agent()
            print("  Historial de conversacion limpiado.\n")
            continue

        if entrada.lower() == "demo":
            for pregunta in PREGUNTAS_DEMO:
                resultado = agent_query(pregunta)
                imprimir_resultado(pregunta, resultado)
            continue

        resultado = agent_query(entrada)
        imprimir_resultado(entrada, resultado)


def modo_demo() -> None:
    """Ejecuta las preguntas de demo y sale."""
    print(f"\n{SEPARADOR}")
    print("  MODO DEMO — Ejecutando preguntas de ejemplo")
    print(f"{SEPARADOR}\n")
    for pregunta in PREGUNTAS_DEMO:
        resultado = agent_query(pregunta)
        imprimir_resultado(pregunta, resultado)


def modo_pregunta_directa(pregunta: str) -> None:
    """Ejecuta una sola pregunta y sale."""
    resultado = agent_query(pregunta)
    imprimir_resultado(pregunta, resultado)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Agente de consultas en lenguaje natural sobre datos Gold"
    )
    parser.add_argument(
        "--pregunta",
        type=str,
        help="Pregunta directa a responder (no interactivo)",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Ejecutar preguntas de demostración y salir",
    )
    args = parser.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "\n  ERROR: ANTHROPIC_API_KEY no está configurada.\n"
            "  Agrega ANTHROPIC_API_KEY=sk-ant-... a tu archivo .env\n"
        )
        sys.exit(1)

    if args.demo:
        modo_demo()
    elif args.pregunta:
        modo_pregunta_directa(args.pregunta)
    else:
        modo_interactivo()
