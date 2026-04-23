import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[0]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import streamlit as st

# ── Configuración de página ───────────────────────────────────────────────────
st.set_page_config(
    page_title="Fintech 360 — Analista de Datos",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.metric-card {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 8px;
    border-left: 4px solid #1B4F72;
}
.stChatMessage {
    border-radius: 10px;
}
</style>
""", unsafe_allow_html=True)

# ── Constantes ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
COLOR_PRINCIPAL = "#1B4F72"
COLOR_ROJO = "#C0392B"
COLOR_VERDE = "#2ECC71"

PREGUNTAS_SUGERIDAS = [
    "Dame el resumen ejecutivo de la plataforma",
    "¿Cuál es el segmento más rentable?",
    "¿Qué campaña lanzarías este mes?",
    "¿Cuántos usuarios llevan más de 30 días sin transaccionar?",
    "¿Qué ciudad tiene mayor potencial de crecimiento?",
    "Analiza la tasa de fallos de pago",
    "¿Cuál es el merchant con más oportunidad de alianza?",
]


# ── Carga de datos (cacheada) ─────────────────────────────────────────────────
@st.cache_data
def cargar_datos():
    df_360    = pd.read_parquet(ROOT / "data/gold/gold_user_360.parquet")
    df_daily  = pd.read_parquet(ROOT / "data/gold/gold_daily_metrics.parquet")
    df_events = pd.read_parquet(ROOT / "data/gold/gold_event_summary.parquet")
    return df_360, df_daily, df_events


# ── Helpers de gráfico ────────────────────────────────────────────────────────
def _fig(figsize=(6, 4)):
    fig, ax = plt.subplots(figsize=figsize)
    ax.spines[["top", "right"]].set_visible(False)
    return fig, ax


def _show(fig):
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)


# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 1 — DASHBOARD DE ANÁLISIS
# ══════════════════════════════════════════════════════════════════════════════
def pagina_dashboard():
    df_360, df_daily, df_events = cargar_datos()

    st.title("📊 Dashboard de Análisis")
    st.caption("Métricas consolidadas de la capa Gold — datos en tiempo real")
    st.divider()

    # ── SECCIÓN 1: KPIs ──────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("👥 Total Usuarios", f"{len(df_360):,}")
    with k2:
        vol_m = df_360["total_amount_cop"].sum() / 1_000_000
        st.metric("💰 Volumen Total COP", f"${vol_m:,.1f}M")
    with k3:
        ticket = df_360["avg_ticket"].mean()
        st.metric("🎫 Ticket Promedio", f"${ticket:,.0f}")
    with k4:
        fallo_pct = df_360["failure_rate"].mean() * 100
        st.metric("⚠️ Tasa de Fallo Promedio", f"{fallo_pct:.1f}%")

    st.divider()

    # ── SECCIÓN 2: Volumen por Segmento + Usuarios por Ciudad ────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Volumen por Segmento (COP)")
        datos = df_360.groupby("user_segment")["total_amount_cop"].sum().sort_values(ascending=True)
        fig, ax = _fig()
        ax.barh(datos.index, datos.values / 1_000_000, color=COLOR_PRINCIPAL)
        ax.set_xlabel("Millones COP")
        _show(fig)

    with col2:
        st.subheader("Usuarios por Ciudad")
        datos = df_360.groupby("city")["user_id"].count().sort_values(ascending=False)
        fig, ax = _fig()
        ax.bar(datos.index, datos.values, color=COLOR_PRINCIPAL)
        ax.set_ylabel("Usuarios")
        ax.tick_params(axis="x", rotation=30)
        _show(fig)

    # ── SECCIÓN 3: Top Merchants + Canal preferido ───────────────────────────
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Top 10 Merchants más populares")
        datos = df_360["top_merchant"].value_counts().head(10).sort_values(ascending=True)
        # Excluir None / NaN
        datos = datos[datos.index.notna() & (datos.index != "None")]
        fig, ax = _fig()
        ax.barh(datos.index.astype(str), datos.values, color=COLOR_PRINCIPAL)
        ax.set_xlabel("Usuarios")
        _show(fig)

    with col4:
        st.subheader("Distribución por Canal preferido")
        datos = df_360["preferred_channel"].value_counts()
        fig, ax = _fig((6, 4))
        wedge_colors = [COLOR_PRINCIPAL, COLOR_VERDE, "#2980B9", "#F39C12"]
        ax.pie(
            datos.values,
            labels=datos.index,
            autopct="%1.1f%%",
            colors=wedge_colors[: len(datos)],
            startangle=90,
        )
        _show(fig)

    # ── SECCIÓN 4: Ticket por Segmento + Tasa de Fallo por Segmento ─────────
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("Ticket Promedio por Segmento")
        datos = df_360.groupby("user_segment")["avg_ticket"].mean().sort_values(ascending=False)
        fig, ax = _fig()
        ax.bar(datos.index, datos.values, color=COLOR_PRINCIPAL)
        ax.set_ylabel("COP")
        ax.tick_params(axis="x", rotation=20)
        _show(fig)

    with col6:
        st.subheader("Tasa de Fallo por Segmento (%)")
        datos = (df_360.groupby("user_segment")["failure_rate"].mean() * 100).sort_values(ascending=False)
        fig, ax = _fig()
        ax.bar(datos.index, datos.values, color=COLOR_ROJO)
        ax.set_ylabel("%")
        ax.tick_params(axis="x", rotation=20)
        _show(fig)

    # ── SECCIÓN 5: Histograma de gasto ───────────────────────────────────────
    st.subheader("Distribución de Gasto Total por Usuario")
    gasto = df_360.loc[df_360["total_amount_cop"] > 0, "total_amount_cop"]
    fig, ax = _fig((12, 4))
    ax.hist(gasto / 1_000_000, bins=30, color=COLOR_PRINCIPAL, edgecolor="white")
    ax.set_xlabel("Gasto total (Millones COP)")
    ax.set_ylabel("Usuarios")
    ax.spines[["top", "right"]].set_visible(False)
    _show(fig)


# ══════════════════════════════════════════════════════════════════════════════
# PÁGINA 2 — CHAT CON EL AGENTE
# ══════════════════════════════════════════════════════════════════════════════
def pagina_chat():
    # CORRECCIÓN 5 — el agente se inicializa UNA sola vez para toda la sesión,
    # independientemente de cuántas veces se cambie de página.
    if "agente" not in st.session_state:
        with st.spinner("🔄 Cargando agente y datos..."):
            from agent.agent import crear_agente
            st.session_state.agente = crear_agente()

    # CORRECCIÓN 4 — mensaje de bienvenida la primera vez
    if "mensajes" not in st.session_state:
        st.session_state.mensajes = [{
            "rol": "assistant",
            "texto": (
                "👋 Hola, soy tu analista de datos fintech. "
                "Puedo responder preguntas sobre los usuarios, "
                "segmentos, transacciones y tendencias de la "
                "plataforma. ¿En qué te puedo ayudar hoy?\n\n"
                "💡 Tip: Usa las preguntas sugeridas del panel "
                "izquierdo para empezar."
            ),
        }]

    st.title("💬 Chat con el Agente Fintech")
    st.caption("Haz preguntas en lenguaje natural sobre los datos de la plataforma")
    st.divider()

    charts_dir = ROOT / "outputs" / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    # Renderizar historial
    for msg in st.session_state.mensajes:
        with st.chat_message(msg["rol"]):
            st.markdown(msg["texto"])
            if msg.get("grafico") and Path(msg["grafico"]).exists():
                st.image(msg["grafico"], use_container_width=True)

    # Input
    pregunta_sidebar = st.session_state.pop("pregunta_rapida", None)
    pregunta = st.chat_input("Escribe tu pregunta aquí...") or pregunta_sidebar

    if pregunta:
        st.session_state.mensajes.append({"rol": "user", "texto": pregunta, "grafico": None})
        with st.chat_message("user"):
            st.markdown(pregunta)

        # CORRECCIÓN 3 — manejo de errores con mensajes claros por tipo
        with st.chat_message("assistant", avatar="🏦"):
            try:
                import time
                with st.spinner("🔍 Analizando datos..."):
                    antes = {f: f.stat().st_mtime for f in charts_dir.glob("*.png")}
                    respuesta = st.session_state.agente(pregunta)
                    texto = str(respuesta)
                    time.sleep(0.5)
                    despues = {f: f.stat().st_mtime for f in charts_dir.glob("*.png")}
                    nuevos = [f for f in despues if f not in antes or despues[f] > antes[f]]
                    grafico = str(sorted(nuevos, key=lambda f: f.stat().st_mtime)[-1]) if nuevos else None

                st.markdown(texto)
                if grafico and Path(grafico).exists():
                    st.image(grafico, use_container_width=True)

                st.session_state.mensajes.append({"rol": "assistant", "texto": texto, "grafico": grafico})

            except Exception as e:
                err = str(e).lower()
                if "timed out" in err or "timeout" in err:
                    texto = (
                        "⏱️ El modelo tardó demasiado en responder. "
                        "Intenta con una pregunta más corta o simple. "
                        "Por ejemplo: '¿Cuántos usuarios hay?' o "
                        "'¿Cuál es el segmento más rentable?'"
                    )
                elif "connection" in err:
                    texto = (
                        "🔌 No se puede conectar con Ollama. "
                        "Verifica que Ollama esté corriendo con: ollama serve"
                    )
                else:
                    texto = f"❌ Error: {str(e)[:200]}"

                st.markdown(texto)
                st.session_state.mensajes.append({"rol": "assistant", "texto": texto, "grafico": None})


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 🏦 Fintech 360")
    st.markdown("#### Plataforma de Análisis")
    st.divider()

    pagina = st.radio(
        "Navegación",
        ["📊 Dashboard de Análisis", "💬 Chat con el Agente"],
        label_visibility="collapsed",
    )

    st.divider()

    if pagina == "💬 Chat con el Agente":
        st.markdown("#### 💬 Preguntas sugeridas")
        for p in PREGUNTAS_SUGERIDAS:
            if st.button(p, key=f"sug_{p[:30]}", use_container_width=True):
                st.session_state["pregunta_rapida"] = p

        st.divider()
        if st.button("🗑️ Limpiar conversación", use_container_width=True):
            st.session_state.mensajes = []
            st.rerun()

    st.markdown(
        "<p style='color:gray;font-size:0.78rem;'>"
        "⚠️ Información confidencial y agregada. Solo para uso interno."
        "</p>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# ROUTER
# ══════════════════════════════════════════════════════════════════════════════
if pagina == "📊 Dashboard de Análisis":
    pagina_dashboard()
else:
    pagina_chat()

# Ejecutar con: streamlit run src/agent/app.py
# Desde la carpeta: fintech_pipeline/
