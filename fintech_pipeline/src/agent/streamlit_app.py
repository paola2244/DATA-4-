"""
streamlit_app.py - Dashboard de consultas en lenguaje natural sobre datos Gold.

Uso:
    streamlit run src/agent/streamlit_app.py

Requiere:
    ANTHROPIC_API_KEY en .env o variable de entorno del sistema.
"""

import base64
import os
import sys

import streamlit as st

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from dotenv import load_dotenv

load_dotenv()

# ── Configuración de página ───────────────────────────────────────────────────
st.set_page_config(
    page_title="Agente Fintech Gold",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Verificar API Key antes de importar el agente ────────────────────────────
if not os.environ.get("ANTHROPIC_API_KEY"):
    st.error(
        "**ANTHROPIC_API_KEY no configurada.**\n\n"
        "Agrega `ANTHROPIC_API_KEY=sk-ant-...` a tu archivo `.env` y reinicia la app."
    )
    st.stop()

from src.agent.agent import agent_query, reset_agent
from src.agent.schema import FEW_SHOT_EXAMPLES, GOLD_SCHEMA

# ── Estado de sesión ─────────────────────────────────────────────────────────
if "historial" not in st.session_state:
    st.session_state.historial = []

if "cargando" not in st.session_state:
    st.session_state.cargando = False

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🏦 Agente Fintech")
    st.caption("Capa Gold | Claude Sonnet 4.5")
    st.divider()

    st.subheader("Preguntas de ejemplo")
    ejemplos = [e["question"] for e in FEW_SHOT_EXAMPLES]
    for ejemplo in ejemplos:
        if st.button(ejemplo, use_container_width=True, key=f"btn_{ejemplo[:20]}"):
            st.session_state.pregunta_rapida = ejemplo

    st.divider()

    if st.button("🗑️ Nueva conversación", use_container_width=True):
        st.session_state.historial = []
        reset_agent()
        st.rerun()

    st.divider()
    with st.expander("📋 Esquema Gold"):
        st.markdown(GOLD_SCHEMA)

    st.divider()
    st.caption(
        "Modelo: `claude-sonnet-4-5`\n\n"
        "Tablas: `gold_user_360` · `gold_daily_metrics` · `gold_event_summary`"
    )

# ── Contenido principal ───────────────────────────────────────────────────────
st.title("Consultas en Lenguaje Natural — Datos Fintech")
st.caption(
    "Haz preguntas sobre los datos de la capa Gold. "
    "El agente genera SQL, ejecuta la consulta y te da un análisis de negocio."
)

# Renderizar historial de conversación
for entrada in st.session_state.historial:
    with st.chat_message("user"):
        st.write(entrada["pregunta"])

    with st.chat_message("assistant", avatar="🏦"):
        st.write(entrada["respuesta"])

        cols = st.columns([1, 1]) if entrada.get("sql") or entrada.get("data") is not None else [st]

        if entrada.get("sql"):
            with cols[0]:
                with st.expander("🔍 SQL ejecutado"):
                    st.code(entrada["sql"], language="sql")

        if entrada.get("data") is not None and not entrada["data"].empty:
            with cols[1] if len(cols) > 1 else cols[0]:
                with st.expander(f"📊 Datos ({len(entrada['data'])} filas)"):
                    st.dataframe(entrada["data"], use_container_width=True)

        if entrada.get("chart_b64"):
            st.image(
                base64.b64decode(entrada["chart_b64"]),
                use_column_width=True,
            )

# ── Input de pregunta ─────────────────────────────────────────────────────────
pregunta_sidebar = st.session_state.pop("pregunta_rapida", None)

pregunta = st.chat_input(
    "Escribe tu pregunta en español...",
    disabled=st.session_state.cargando,
) or pregunta_sidebar

if pregunta:
    # Mostrar mensaje del usuario
    with st.chat_message("user"):
        st.write(pregunta)

    # Procesar con el agente
    with st.chat_message("assistant", avatar="🏦"):
        with st.spinner("Analizando datos..."):
            resultado = agent_query(pregunta)

        if resultado.get("error"):
            st.error(f"Error: {resultado['error']}")
        else:
            st.write(resultado["answer"])

            cols = st.columns([1, 1]) if resultado.get("sql") or resultado.get("data") is not None else [st]

            if resultado.get("sql"):
                with cols[0]:
                    with st.expander("🔍 SQL ejecutado"):
                        st.code(resultado["sql"], language="sql")

            if resultado.get("data") is not None and not resultado["data"].empty:
                with cols[1] if len(cols) > 1 else cols[0]:
                    with st.expander(f"📊 Datos ({len(resultado['data'])} filas)"):
                        st.dataframe(resultado["data"], use_container_width=True)

            if resultado.get("chart_b64"):
                st.image(
                    base64.b64decode(resultado["chart_b64"]),
                    use_column_width=True,
                )

    # Guardar en historial
    st.session_state.historial.append({
        "pregunta":  pregunta,
        "respuesta": resultado.get("answer", ""),
        "sql":       resultado.get("sql"),
        "data":      resultado.get("data"),
        "chart_b64": resultado.get("chart_b64"),
    })
