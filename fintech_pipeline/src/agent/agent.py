"""
agent.py - Agente Fintech con Ollama via HTTP directo.

Expone agent_query() como punto de entrada principal para consultas
en lenguaje natural sobre la capa Gold.
"""

import os
import json
import datetime
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv

from strands import Agent, tool
from strands.models import BedrockModel

from pathlib import Path as _Path
import sys as _sys
_sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))
from agent.schema import SYSTEM_PROMPT

load_dotenv()

# ── Directorio de gráficos ───────────────────────────────────────────────────
def _get_charts_dir():
    from pathlib import Path
    charts_dir = Path(__file__).resolve().parents[2] / "outputs" / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    return charts_dir


_CORP_BLUE  = "#1B4F72"
_CORP_GREEN = "#2ECC71"
_PALETTE    = [_CORP_BLUE, _CORP_GREEN, "#2980B9", "#27AE60", "#F39C12",
               "#8E44AD", "#E74C3C", "#16A085"]


def _save_chart(titulo: str) -> str:
    """Guarda la figura actual y retorna la ruta."""
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    nombre = titulo.lower().replace(" ", "_").replace("/", "-")[:40]
    ruta = _get_charts_dir() / f"{nombre}_{ts}.png"
    plt.tight_layout()
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    plt.close()
    return str(ruta)


# ── Conexión DuckDB ──────────────────────────────────────────────────────────
_conn = None
_tablas_cargadas = {}


def _get_conn():
    global _conn, _tablas_cargadas
    if _conn is not None:
        return _conn
    import duckdb
    from pathlib import Path
    _conn = duckdb.connect()
    ROOT = Path(__file__).resolve().parents[2]
    tablas = {
        "gold_user_360":      ROOT / "data/gold/gold_user_360.parquet",
        "gold_daily_metrics": ROOT / "data/gold/gold_daily_metrics.parquet",
        "gold_event_summary": ROOT / "data/gold/gold_event_summary.parquet",
    }
    for nombre, ruta in tablas.items():
        if ruta.exists():
            df = pd.read_parquet(ruta)
            _conn.register(nombre, df)
            _tablas_cargadas[nombre] = True
            print(f"  ✅ Tabla '{nombre}' cargada — {len(df):,} filas")
        else:
            _tablas_cargadas[nombre] = False
            print(f"  ⚠️  '{nombre}' no encontrada en {ruta}")
    return _conn


# ── Herramientas del agente ──────────────────────────────────────────────────
@tool
def listar_tablas() -> str:
    """Lista las tablas disponibles en la base de datos."""
    return (
        "🔒 La estructura interna del sistema es confidencial y no "
        "puede ser compartida. Puedo ayudarte con análisis, métricas "
        "agregadas, insights de negocio o recomendaciones. "
        "¿Qué análisis te gustaría realizar?"
    )


@tool
def consultar_sql(query: str) -> str:
    """Ejecuta una consulta SQL y retorna los resultados como texto."""
    sql_upper = query.upper().strip()

    _COLUMNAS_SENSIBLES = ("USER_NAME", "USER_EMAIL", "USER_AGE")
    for col in _COLUMNAS_SENSIBLES:
        if col in sql_upper:
            return (
                f"⛔ Consulta bloqueada: la columna '{col.lower()}' contiene "
                "datos personales y no puede ser expuesta. "
                "Por favor reformula la consulta usando solo datos agregados o anonimizados."
            )

    if (
        "SELECT *" in sql_upper
        and "WHERE" not in sql_upper
        and "LIMIT" not in sql_upper
    ):
        return (
            "⛔ Consulta bloqueada: SELECT * sin filtros ni LIMIT podría exponer "
            "registros masivos. Por políticas de privacidad no puedo compartir "
            "tablas completas. ¿Te gustaría un análisis agregado o resumen estadístico?"
        )

    _OPERACIONES_ESCRITURA = ("DROP ", "DELETE ", "UPDATE ", "INSERT ", "ALTER ", "CREATE ", "TRUNCATE ")
    for op in _OPERACIONES_ESCRITURA:
        if sql_upper.startswith(op) or f" {op.strip()} " in sql_upper:
            return f"⛔ Operación '{op.strip()}' no permitida. Solo se permiten consultas de lectura."

    conn = _get_conn()
    try:
        df = conn.execute(query).fetchdf()
        _COLS_SENSIBLES = {"user_name", "user_email", "user_age"}
        cols_a_eliminar = [c for c in df.columns if c.lower() in _COLS_SENSIBLES]
        if cols_a_eliminar:
            df = df.drop(columns=cols_a_eliminar)
        if len(df) > 100:
            df = df.head(100)
            return df.to_string(index=False) + "\n\n⚠️  Resultado truncado a 100 filas."
        return df.to_string(index=False)
    except Exception as e:
        return f"Error al ejecutar SQL: {e}"


@tool
def grafico_barras(query: str, titulo: str = "Gráfico de Barras") -> str:
    """Genera un gráfico de barras a partir de una consulta SQL y guarda el PNG."""
    conn = _get_conn()
    try:
        df = conn.execute(query).fetchdf()
        if df.empty:
            return "No hay datos para graficar."
        col_x = df.columns[0]
        col_y = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(df[col_x].astype(str), pd.to_numeric(df[col_y], errors="coerce"),
               color=_PALETTE[:len(df)])
        ax.set_title(titulo, fontsize=14, color=_CORP_BLUE, fontweight="bold")
        ax.set_xlabel(col_x)
        ax.set_ylabel(col_y)
        ax.tick_params(axis="x", rotation=30)
        ruta = _save_chart(titulo)
        return f"✅ Gráfico de barras guardado en: {ruta}\nDatos:\n{df.to_string(index=False)}"
    except Exception as e:
        return f"Error: {e}"


@tool
def grafico_distribucion(query: str, titulo: str = "Distribución") -> str:
    """Genera un histograma de distribución a partir de una consulta SQL y guarda el PNG."""
    conn = _get_conn()
    try:
        df = conn.execute(query).fetchdf()
        if df.empty:
            return "No hay datos para graficar."
        num_cols = df.select_dtypes(include="number").columns
        col = num_cols[0] if len(num_cols) > 0 else df.columns[0]
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.hist(pd.to_numeric(df[col], errors="coerce").dropna(),
                bins=20, color=_CORP_BLUE, edgecolor="white")
        ax.set_title(titulo, fontsize=14, color=_CORP_BLUE, fontweight="bold")
        ax.set_xlabel(col)
        ax.set_ylabel("Frecuencia")
        ruta = _save_chart(titulo)
        return f"✅ Gráfico de distribución guardado en: {ruta}\nDatos:\n{df.to_string(index=False)}"
    except Exception as e:
        return f"Error: {e}"


@tool
def grafico_segmentos(query: str, titulo: str = "Segmentos") -> str:
    """Genera un gráfico de pie por segmentos a partir de una consulta SQL y guarda el PNG."""
    conn = _get_conn()
    try:
        df = conn.execute(query).fetchdf()
        if df.empty:
            return "No hay datos para graficar."
        col_label = df.columns[0]
        col_val   = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.pie(
            pd.to_numeric(df[col_val], errors="coerce").fillna(0),
            labels=df[col_label].astype(str),
            autopct="%1.1f%%",
            colors=_PALETTE[:len(df)],
            startangle=90,
        )
        ax.set_title(titulo, fontsize=14, color=_CORP_BLUE, fontweight="bold")
        ruta = _save_chart(titulo)
        return f"✅ Gráfico de segmentos guardado en: {ruta}\nDatos:\n{df.to_string(index=False)}"
    except Exception as e:
        return f"Error: {e}"


@tool
def grafico_funnel_eventos(query: str, titulo: str = "Funnel de Eventos") -> str:
    """Genera un gráfico de funnel (barras horizontales) y guarda el PNG."""
    conn = _get_conn()
    try:
        df = conn.execute(query).fetchdf()
        if df.empty:
            return "No hay datos para graficar."
        col_label = df.columns[0]
        col_val   = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        df_sorted = df.sort_values(col_val, ascending=True)
        fig, ax = plt.subplots(figsize=(10, max(4, len(df) * 0.6)))
        bars = ax.barh(df_sorted[col_label].astype(str),
                       pd.to_numeric(df_sorted[col_val], errors="coerce"),
                       color=_CORP_BLUE)
        ax.bar_label(bars, padding=4, fontsize=9)
        ax.set_title(titulo, fontsize=14, color=_CORP_BLUE, fontweight="bold")
        ax.set_xlabel(col_val)
        ruta = _save_chart(titulo)
        return f"✅ Gráfico funnel guardado en: {ruta}\nDatos:\n{df_sorted.to_string(index=False)}"
    except Exception as e:
        return f"Error: {e}"


@tool
def grafico_tendencia_diaria(query: str, titulo: str = "Tendencia Diaria") -> str:
    """Genera un gráfico de línea de tendencia diaria y guarda el PNG."""
    conn = _get_conn()
    try:
        df = conn.execute(query).fetchdf()
        if df.empty:
            return "No hay datos para graficar."
        col_x = df.columns[0]
        col_y = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(df[col_x].astype(str), pd.to_numeric(df[col_y], errors="coerce"),
                color=_CORP_BLUE, linewidth=2, marker="o", markersize=4)
        ax.fill_between(range(len(df)),
                        pd.to_numeric(df[col_y], errors="coerce"),
                        alpha=0.1, color=_CORP_GREEN)
        ax.set_title(titulo, fontsize=14, color=_CORP_BLUE, fontweight="bold")
        ax.set_xlabel(col_x)
        ax.set_ylabel(col_y)
        ax.tick_params(axis="x", rotation=30)
        plt.xticks(range(len(df)), df[col_x].astype(str))
        ruta = _save_chart(titulo)
        return f"✅ Gráfico de tendencia guardado en: {ruta}\nDatos:\n{df.to_string(index=False)}"
    except Exception as e:
        return f"Error: {e}"


@tool
def perfil_usuario_360(user_id: str) -> str:
    """Retorna el perfil completo 360 de un usuario dado su ID."""
    conn = _get_conn()
    try:
        df = conn.execute(
            "SELECT * FROM gold_user_360 WHERE user_id = ?", [user_id]
        ).fetchdf()
        if df.empty:
            return f"No se encontró el usuario '{user_id}'."
        return df.to_string(index=False)
    except Exception as e:
        return f"Error: {e}"


@tool
def sugerir_campanas(segmento: str = "") -> str:
    """Sugiere campañas de marketing basadas en segmentos de usuarios."""
    conn = _get_conn()
    try:
        sql = "SELECT * FROM gold_user_360 LIMIT 10"
        if segmento:
            sql = f"SELECT * FROM gold_user_360 WHERE segment = '{segmento}' LIMIT 10"
        df = conn.execute(sql).fetchdf()
        return f"Sugerencias para segmento '{segmento}':\n{df.to_string(index=False)}"
    except Exception as e:
        return f"Error: {e}"


@tool
def resumen_ejecutivo() -> str:
    """Genera un resumen ejecutivo con métricas clave del negocio."""
    conn = _get_conn()
    try:
        resultado = []
        for tabla in ["gold_daily_metrics", "gold_event_summary", "gold_user_360"]:
            if _tablas_cargadas.get(tabla):
                df = conn.execute(f"SELECT COUNT(*) as filas FROM {tabla}").fetchdf()
                resultado.append(f"{tabla}: {df['filas'][0]:,} registros")
        return "Resumen ejecutivo:\n" + "\n".join(resultado) if resultado else "No hay datos disponibles."
    except Exception as e:
        return f"Error: {e}"


@tool
def consultar_databricks(sql: str) -> str:
    """
    Ejecuta una consulta SQL sobre el catálogo Databricks fintech.
    Usa esta tool cuando el usuario pida datos del catálogo Databricks
    o cuando quiera consultar la tabla gold_user_360 en Databricks.
    Solo ejecuta SELECT, nunca INSERT, UPDATE, DELETE ni DROP.

    Args:
        sql: Consulta SQL a ejecutar sobre el catálogo Databricks
    Returns:
        Resultado de la consulta en formato texto
    """
    import time

    # ── TRAZA 1: Tool invocada ───────────────────────────────────────────
    ts_inicio = time.time()
    print("\n" + "="*60)
    print("🔷 DATABRICKS TOOL — INVOCADA")
    print(f"   Timestamp : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   SQL recibido:\n   {sql}")
    print("="*60)

    try:
        from databricks import sql as dbsql

        host      = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
        token     = os.getenv("DATABRICKS_TOKEN", "")
        http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
        catalog   = os.getenv("DATABRICKS_CATALOG", "fintech_pipeline")
        schema    = os.getenv("DATABRICKS_SCHEMA", "fintech")

        # ── TRAZA 2: Credenciales ────────────────────────────────────────
        print(f"🔑 Credenciales cargadas:")
        print(f"   HOST      : {host[:40]}..." if len(host) > 40 else f"   HOST      : {host}")
        print(f"   TOKEN     : {token[:8]}..." if token else "   TOKEN     : ⚠️ VACÍO")
        print(f"   HTTP_PATH : {http_path}")
        print(f"   CATALOG   : {catalog}")
        print(f"   SCHEMA    : {schema}")

        if not host or not token or not http_path:
            msg = (
                "⚠️ Faltan credenciales de Databricks en el archivo .env.\n"
                "Configura: DATABRICKS_HOST, DATABRICKS_TOKEN, "
                "DATABRICKS_HTTP_PATH"
            )
            print(f"❌ CREDENCIALES INCOMPLETAS — abortando")
            print("="*60 + "\n")
            return msg

        # ── TRAZA 3: Validar SQL ─────────────────────────────────────────
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith("SELECT"):
            msg = (
                "🔒 Solo se permiten consultas SELECT en Databricks. "
                "No se permiten operaciones de escritura."
            )
            print(f"❌ SQL BLOQUEADO — no es SELECT")
            print("="*60 + "\n")
            return msg

        # ── TRAZA 4: Conectando ──────────────────────────────────────────
        print(f"\n⏳ Conectando a Databricks...")
        t_conexion = time.time()

        with dbsql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
            catalog=catalog,
            schema=schema,
        ) as conn:
            print(f"✅ Conexión establecida en {time.time() - t_conexion:.2f}s")

            with conn.cursor() as cursor:
                # ── TRAZA 5: Ejecutando query ────────────────────────────
                print(f"\n⚡ Ejecutando query...")
                t_query = time.time()
                cursor.execute(sql)
                resultado = cursor.fetchall()
                columnas = [desc[0] for desc in cursor.description]
                t_total_query = time.time() - t_query

                # ── TRAZA 6: Resultado ───────────────────────────────────
                print(f"✅ Query ejecutado en {t_total_query:.2f}s")
                print(f"   Filas retornadas : {len(resultado)}")
                print(f"   Columnas         : {', '.join(columnas)}")

                if not resultado:
                    print("⚠️  Sin resultados")
                    print("="*60 + "\n")
                    return "La consulta no retornó resultados."

                lineas = [" | ".join(columnas)]
                lineas.append("-" * len(lineas[0]))
                for fila in resultado[:50]:
                    lineas.append(" | ".join(str(v) for v in fila))

                total = len(resultado)
                resumen = "\n".join(lineas)
                if total > 50:
                    resumen += (
                        f"\n\n⚠️ Resultado truncado a 50 filas "
                        f"(total: {total:,} filas)."
                    )

                t_total = time.time() - ts_inicio
                print(f"\n✅ DATABRICKS TOOL — COMPLETADA en {t_total:.2f}s total")
                print("="*60 + "\n")
                return resumen

    except ImportError:
        msg = (
            "❌ El conector databricks-sql-connector no está instalado.\n"
            "Corre: pip install databricks-sql-connector"
        )
        print(f"❌ ImportError: databricks-sql-connector no instalado")
        print("="*60 + "\n")
        return msg

    except Exception as e:
        t_total = time.time() - ts_inicio
        msg = f"❌ Error al conectar con Databricks ({t_total:.2f}s): {str(e)}"
        print(f"❌ EXCEPCIÓN: {str(e)}")
        print(f"   Tiempo transcurrido: {t_total:.2f}s")
        print("="*60 + "\n")
        return msg


# ── Agente ───────────────────────────────────────────────────────────────────
def crear_agente() -> Agent:
    """Crea el agente usando Ollama via HTTP directo."""
    print("\n🔄 Cargando datos del pipeline...")
    _get_conn()

    import requests as _req
    import json as _json

    class OllamaDirectModel:
        def __init__(self, model_id="mistral"):
            self.model_id = model_id
            self.stateful = False
            self.config = {"model_id": model_id, "max_tokens": 4096}
            self._tools_registry = {}

        def _build_chat(self, messages, system_prompt=None):
            chat = []
            if system_prompt:
                chat.append({"role": "system", "content": system_prompt})
            for m in messages:
                role = m.get("role", "user")
                content = m.get("content", "")
                if isinstance(content, list):
                    content = " ".join(
                        c.get("text", "") for c in content if isinstance(c, dict)
                    )
                chat.append({"role": role, "content": content})
            return chat

        def _inject_tools(self, chat):
            if not self._tools_registry:
                return chat
            tools_desc = (
                "\n\nTIENES ACCESO A ESTAS FUNCIONES. "
                "Cuando el usuario pida un análisis que "
                "requiera una de estas funciones, responde ÚNICAMENTE "
                "con este formato JSON sin texto adicional:\n"
                "{\"tool\": \"nombre_tool\", \"args\": {\"param\": \"valor\"}}\n\n"
                "Funciones disponibles:\n"
            )
            for nombre, fn in self._tools_registry.items():
                doc = (getattr(fn, "__doc__", "") or "").strip()[:200]
                tools_desc += f"- {nombre}: {doc}\n"
            if chat and chat[0]["role"] == "system":
                chat[0]["content"] += tools_desc
            else:
                chat.insert(0, {"role": "system", "content": tools_desc})
            return chat

        def _call_chat(self, chat):
            resp = _req.post(
                "http://localhost:11434/api/chat",
                json={"model": self.model_id, "messages": chat, "stream": False},
                timeout=300,
            )
            resp.raise_for_status()
            return resp.json().get("message", {}).get("content", "")

        def _maybe_invoke_tool(self, text, original_messages=None):
            stripped = text.strip()
            import re
            json_match = re.search(
                r'\{[^{}]*"tool"[^{}]*\}', stripped, re.DOTALL
            )
            if not json_match:
                return text
            try:
                call = json.loads(json_match.group(0))
                tool_name = call.get("tool", "")
                args = call.get("args", {})

                print(f"\n🔧 TOOL DETECTADA: {tool_name}")
                print(f"   Args recibidos: {args}")

                fn = self._tools_registry.get(tool_name)
                if not fn:
                    print(f"   ❌ Tool '{tool_name}' no encontrada")
                    return text

                import inspect
                params = list(inspect.signature(fn).parameters.keys())
                print(f"   Parámetros esperados: {params}")
                if len(params) == 1 and params[0] not in args:
                    primer_valor = list(args.values())[0] if args else ""
                    args = {params[0]: primer_valor}
                    print(f"   Args normalizados: {args}")

                print(f"   ⚡ Ejecutando {tool_name}({args})...")
                resultado = fn(**args)
                print(f"   ✅ Tool ejecutada. Resultado preview: {str(resultado)[:150]}...")

                print(f"   🗣️ Generando respuesta en lenguaje natural...")
                prompt_natural = (
                    f"Eres un analista senior de datos fintech. "
                    f"Basándote en estos datos obtenidos:\n\n"
                    f"{resultado}\n\n"
                    f"Responde en español de forma profesional usando "
                    f"esta estructura:\n"
                    f"📊 RESUMEN: respuesta directa y concisa\n"
                    f"🔍 ANÁLISIS: qué muestran los datos con números\n"
                    f"💡 INSIGHT CLAVE: qué significa para el negocio\n"
                    f"🎯 RECOMENDACIÓN: acción concreta a tomar\n\n"
                    f"NO menciones nombres de tablas, bases de datos "
                    f"ni detalles técnicos internos."
                )
                respuesta_natural = self._call_chat([
                    {"role": "system", "content": prompt_natural},
                    {"role": "user", "content": "Genera el análisis."}
                ])
                return respuesta_natural

            except Exception as e:
                print(f"   ❌ Error: {e}")
                return text

        async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs):
            chat = self._build_chat(messages, system_prompt)
            chat = self._inject_tools(chat)
            text = self._call_chat(chat)
            text = self._maybe_invoke_tool(text, original_messages=chat)
            yield {"messageStart": {"role": "assistant"}}
            yield {"contentBlockStart": {"contentBlockIndex": 0, "start": {"text": ""}}}
            yield {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"text": text}}}
            yield {"contentBlockStop": {"contentBlockIndex": 0}}
            yield {"messageStop": {"stopReason": "end_turn"}}
            yield {"metadata": {"usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}}}

        def get_config(self):
            return self.config

    _tools_list = [
        consultar_sql,
        perfil_usuario_360,
        sugerir_campanas,
        resumen_ejecutivo,
        consultar_databricks,
    ]

    model = OllamaDirectModel(model_id="llama3.2")
    model._tools_registry = {getattr(t, "__name__", str(t)): t for t in _tools_list}

    agente = Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=_tools_list,
    )
    print("✅ Agente Fintech listo.")
    return agente


# ── API pública ──────────────────────────────────────────────────────────────
_agent = None


def agent_query(pregunta: str) -> str:
    global _agent
    if _agent is None:
        _agent = crear_agente()

    import os

    tiene_databricks = all([
        os.getenv("DATABRICKS_HOST"),
        os.getenv("DATABRICKS_TOKEN"),
        os.getenv("DATABRICKS_HTTP_PATH")
    ])

    palabras_datos = [
        "cuántos", "cuantos", "cuál", "cual", "cuáles", "cuales",
        "dame", "muéstrame", "muestrame", "dime", "lista",
        "total", "promedio", "segmento", "ciudad", "usuario",
        "transaccion", "transacción", "gasto", "volumen",
        "merchant", "canal", "fallo", "activo", "inactivo",
        "rentable", "campaña", "campaña", "resumen", "análisis",
        "analisis", "top", "mayor", "menor", "más", "menos"
    ]

    es_consulta_datos = any(
        p in pregunta.lower() for p in palabras_datos
    )

    if tiene_databricks and es_consulta_datos:
        print(f"\n🎯 Redirigiendo automáticamente a Databricks...")

        pregunta_lower = pregunta.lower()

        if any(p in pregunta_lower for p in ["cuántos usuarios", "cuantos usuarios", "total usuarios"]):
            sql = "SELECT COUNT(*) as total_usuarios FROM gold_user_360"

        elif any(p in pregunta_lower for p in ["por segmento", "segmentos", "cada segmento"]):
            sql = """SELECT user_segment,
                        COUNT(*) as usuarios,
                        ROUND(AVG(total_amount_cop),0) as ticket_promedio,
                        ROUND(AVG(failure_rate)*100,1) as tasa_fallo_pct
                     FROM gold_user_360
                     GROUP BY user_segment
                     ORDER BY usuarios DESC"""

        elif any(p in pregunta_lower for p in ["por ciudad", "ciudades", "cada ciudad"]):
            sql = """SELECT city,
                        COUNT(*) as usuarios,
                        ROUND(SUM(total_amount_cop)/1000000,2) as volumen_M_cop
                     FROM gold_user_360
                     GROUP BY city
                     ORDER BY volumen_M_cop DESC"""

        elif any(p in pregunta_lower for p in ["merchant", "comercio", "tienda"]):
            sql = """SELECT top_merchant, COUNT(*) as usuarios
                     FROM gold_user_360
                     WHERE top_merchant IS NOT NULL
                     GROUP BY top_merchant
                     ORDER BY usuarios DESC
                     LIMIT 10"""

        elif any(p in pregunta_lower for p in ["inactivo", "dormido", "sin transac"]):
            sql = """SELECT user_segment, COUNT(*) as usuarios_inactivos
                     FROM gold_user_360
                     WHERE days_since_last_tx > 30
                     GROUP BY user_segment
                     ORDER BY usuarios_inactivos DESC"""

        elif any(p in pregunta_lower for p in ["fallo", "fallido", "error de pago"]):
            sql = """SELECT user_segment,
                        ROUND(AVG(failure_rate)*100,1) as tasa_fallo_pct,
                        SUM(failed_transactions) as total_fallos
                     FROM gold_user_360
                     GROUP BY user_segment
                     ORDER BY tasa_fallo_pct DESC"""

        elif any(p in pregunta_lower for p in ["rentable", "mayor gasto", "top usuario"]):
            sql = """SELECT user_segment,
                        ROUND(AVG(avg_ticket),0) as ticket_promedio,
                        ROUND(SUM(total_amount_cop)/1000000,2) as volumen_M_cop
                     FROM gold_user_360
                     GROUP BY user_segment
                     ORDER BY volumen_M_cop DESC"""

        elif any(p in pregunta_lower for p in ["canal", "app", "web"]):
            sql = """SELECT preferred_channel, COUNT(*) as usuarios
                     FROM gold_user_360
                     GROUP BY preferred_channel
                     ORDER BY usuarios DESC"""

        else:
            sql = """SELECT user_segment,
                        COUNT(*) as usuarios,
                        ROUND(SUM(total_amount_cop)/1000000,2) as volumen_M_cop,
                        ROUND(AVG(avg_ticket),0) as ticket_promedio
                     FROM gold_user_360
                     GROUP BY user_segment"""

        datos = consultar_databricks(sql)

        modelo = _agent.model
        prompt_natural = (
            f"Eres un analista senior de datos fintech colombiano. "
            f"El usuario preguntó: '{pregunta}'\n\n"
            f"Los datos obtenidos son:\n{datos}\n\n"
            f"Responde en español de forma profesional con esta estructura:\n"
            f"📊 RESUMEN: respuesta directa a la pregunta\n"
            f"🔍 ANÁLISIS: interpretación de los números\n"
            f"💡 INSIGHT CLAVE: qué significa para el negocio\n"
            f"🎯 RECOMENDACIÓN: acción concreta a tomar\n\n"
            f"No menciones nombres de tablas ni detalles técnicos."
        )
        try:
            respuesta = modelo._call_chat([
                {"role": "system", "content": prompt_natural},
                {"role": "user", "content": "Genera el análisis profesional."}
            ])
            return respuesta
        except Exception as e:
            return f"Datos de Databricks:\n{datos}"

    respuesta = _agent(pregunta)
    if hasattr(respuesta, "message"):
        return respuesta.message.get("content", [{}])[0].get("text", str(respuesta))
    return str(respuesta)


def reset_agent():
    global _agent
    _agent = None


# ── Modo interactivo ─────────────────────────────────────────────────────────
def main():
    agente = crear_agente()
    print("\nModo interactivo — escribe 'salir' para terminar.\n")
    while True:
        try:
            pregunta = input("Tú: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nHasta luego.")
            break
        if pregunta.lower() in ("salir", "exit", "quit"):
            print("Hasta luego.")
            break
        if not pregunta:
            continue
        respuesta = agente(pregunta)
        if hasattr(respuesta, "message"):
            texto = respuesta.message.get("content", [{}])[0].get("text", str(respuesta))
        else:
            texto = str(respuesta)
        print(f"\nAgente: {texto}\n")


if __name__ == "__main__":
    main()
