"""
Pipeline de la Capa Gold.

Construye la tabla gold_user_360 y las tablas de soporte
a partir de los datos Silver.

Tablas generadas:
    - gold_user_360.parquet      → Visión 360 por usuario (tabla principal)
    - gold_daily_metrics.parquet → Métricas agregadas por día
    - gold_event_summary.parquet → Resumen por tipo de evento

Uso:
    python src/gold/pipeline_gold.py
    (Requiere que Silver esté generado primero)
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTES
# ═══════════════════════════════════════════════════════════════════════════

# Eventos que representan transacciones financieras reales (mueven dinero)
EVENTOS_TRANSACCIONALES = ["PAYMENT_MADE", "PURCHASE_MADE", "TRANSFER_SENT"]

# Fecha de referencia para calcular "días desde última transacción"
HOY = datetime.now().date()


# ═══════════════════════════════════════════════════════════════════════════
# LECTURA DE SILVER
# ═══════════════════════════════════════════════════════════════════════════

def leer_silver(carpeta_silver: str = "data/silver") -> pd.DataFrame:
    """Lee el archivo Silver y lo retorna como DataFrame."""
    ruta = os.path.join(carpeta_silver, "silver_events.parquet")
    
    if not os.path.exists(ruta):
        raise FileNotFoundError(
            f"No se encontró Silver en {ruta}\n"
            f"Ejecuta primero: python src/silver/pipeline_silver.py"
        )
    
    df = pd.read_parquet(ruta)
    print(f"   ✅ Silver leído: {len(df):,} registros, {df['user_id'].nunique()} usuarios")
    return df


# ═══════════════════════════════════════════════════════════════════════════
# TABLA 1 — gold_user_360
# ═══════════════════════════════════════════════════════════════════════════

def construir_user_360(silver: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la tabla principal gold_user_360.
    Una fila = un usuario con toda su información consolidada.
    
    Proceso:
        1. Datos demográficos (primer registro del usuario)
        2. Métricas de todos los eventos
        3. Métricas de transacciones exitosas (solo PAYMENT, PURCHASE, TRANSFER)
        4. Top merchant, categoría y canal
        5. Columnas calculadas (failure_rate, days_since_last_tx)
    """
    
    # ── 1. Datos demográficos del usuario ──────────────────────────────────
    demograficos = (
        silver
        .sort_values("timestamp")            # Ordenar por tiempo
        .groupby("user_id")
        .agg(
            user_name=("user_name", "first"),
            user_email=("user_email", "first"),
            user_age=("user_age", "first"),
            user_segment=("user_segment", "first"),
            city=("location_city", "first"),
            total_events=("event_id", "count"),
            failed_transactions=("is_failed", "sum"),
            last_event_date=("timestamp", "max"),
        )
        .reset_index()
    )
    
    # ── 2. Métricas de transacciones exitosas ──────────────────────────────
    # Solo PAYMENT_MADE, PURCHASE_MADE, TRANSFER_SENT (no MONEY_ADDED ni FAILED)
    silver_tx = silver[
        silver["event"].isin(EVENTOS_TRANSACCIONALES) &
        ~silver["is_failed"]
    ].copy()
    
    if len(silver_tx) == 0:
        print("   ⚠️  No hay transacciones exitosas para calcular métricas Gold")
        metricas_tx = pd.DataFrame(columns=[
            "user_id", "total_transactions", "total_amount_cop",
            "total_amount_usd", "avg_ticket", "balance_current",
            "last_transaction_date"
        ])
    else:
        # Ordenar para que 'last' en balance_current sea el más reciente
        silver_tx = silver_tx.sort_values("timestamp")
        
        metricas_tx = (
            silver_tx
            .groupby("user_id")
            .agg(
                total_transactions=("event_id", "count"),
                total_amount_cop=("amount_cop", "sum"),
                total_amount_usd=("amount_usd", "sum"),
                avg_ticket=("amount_cop", "mean"),
                balance_current=("balance_after", "last"),
                last_transaction_date=("timestamp", "max"),
            )
            .reset_index()
        )
        
        # Redondear columnas monetarias
        for col in ["total_amount_cop", "total_amount_usd", "avg_ticket", "balance_current"]:
            metricas_tx[col] = metricas_tx[col].round(2)
    
    # ── 3. Top merchant por usuario ────────────────────────────────────────
    top_merchant = (
        silver_tx
        .dropna(subset=["merchant"])
        .groupby("user_id")["merchant"]
        .agg(lambda x: x.value_counts().index[0] if len(x) > 0 else None)
        .reset_index()
        .rename(columns={"merchant": "top_merchant"})
    )
    
    # ── 4. Top categoría por usuario ───────────────────────────────────────
    top_categoria = (
        silver_tx
        .dropna(subset=["category"])
        .groupby("user_id")["category"]
        .agg(lambda x: x.value_counts().index[0] if len(x) > 0 else None)
        .reset_index()
        .rename(columns={"category": "top_category"})
    )
    
    # ── 5. Canal y dispositivo preferido (sobre TODOS los eventos) ─────────
    canal_preferido = (
        silver
        .dropna(subset=["channel"])
        .groupby("user_id")["channel"]
        .agg(lambda x: x.value_counts().index[0])
        .reset_index()
        .rename(columns={"channel": "preferred_channel"})
    )
    
    dispositivo_preferido = (
        silver
        .dropna(subset=["device"])
        .groupby("user_id")["device"]
        .agg(lambda x: x.value_counts().index[0])
        .reset_index()
        .rename(columns={"device": "preferred_device"})
    )
    
    # ── 6. Unir todo ───────────────────────────────────────────────────────
    gold = demograficos.copy()
    gold = gold.merge(metricas_tx,         on="user_id", how="left")
    gold = gold.merge(top_merchant,        on="user_id", how="left")
    gold = gold.merge(top_categoria,       on="user_id", how="left")
    gold = gold.merge(canal_preferido,     on="user_id", how="left")
    gold = gold.merge(dispositivo_preferido, on="user_id", how="left")
    
    # ── 7. Columnas calculadas ─────────────────────────────────────────────
    
    # Tasa de fallo = fallos / (transacciones exitosas + fallos)
    gold["total_transactions"] = gold["total_transactions"].fillna(0)
    gold["failure_rate"] = (
        gold["failed_transactions"] /
        (gold["total_transactions"] + gold["failed_transactions"])
    ).round(4)
    gold["failure_rate"] = gold["failure_rate"].fillna(0.0)
    
    # Días desde la última transacción
    def calcular_dias(last_tx):
        if pd.isna(last_tx):
            return None
        if hasattr(last_tx, "date"):
            last_tx = last_tx.date()
        try:
            return (HOY - last_tx).days
        except Exception:
            return None
    
    gold["days_since_last_tx"] = gold["last_transaction_date"].apply(calcular_dias)
    
    # ── 8. Ordenar columnas en el orden lógico definido en el diccionario ──
    orden_columnas = [
        "user_id", "user_name", "user_email", "user_age", "user_segment", "city",
        "total_events", "total_transactions", "total_amount_cop", "total_amount_usd",
        "avg_ticket", "failed_transactions", "failure_rate",
        "balance_current", "top_merchant", "top_category",
        "preferred_channel", "preferred_device",
        "last_transaction_date", "last_event_date", "days_since_last_tx"
    ]
    columnas_existentes = [c for c in orden_columnas if c in gold.columns]
    gold = gold[columnas_existentes]
    
    print(f"   ✅ gold_user_360: {len(gold):,} usuarios | {len(gold.columns)} columnas")
    return gold


# ═══════════════════════════════════════════════════════════════════════════
# TABLA 2 — gold_daily_metrics
# ═══════════════════════════════════════════════════════════════════════════

def construir_daily_metrics(silver: pd.DataFrame) -> pd.DataFrame:
    """
    Métricas agregadas por día para el dashboard de tendencias.
    """
    daily = (
        silver
        .groupby("date")
        .agg(
            total_events=("event_id", "count"),
            total_transactions=("is_transactional", "sum"),
            total_amount_cop=("amount_cop", "sum"),
            failed_count=("is_failed", "sum"),
            unique_users=("user_id", "nunique"),
        )
        .reset_index()
    )
    
    daily["total_amount_cop"] = daily["total_amount_cop"].round(2)
    daily = daily.sort_values("date").reset_index(drop=True)
    
    print(f"   ✅ gold_daily_metrics: {len(daily)} días")
    return daily


# ═══════════════════════════════════════════════════════════════════════════
# TABLA 3 — gold_event_summary
# ═══════════════════════════════════════════════════════════════════════════

def construir_event_summary(silver: pd.DataFrame) -> pd.DataFrame:
    """
    Resumen por tipo de evento para los KPIs del dashboard principal.
    """
    summary = (
        silver
        .groupby("event")
        .agg(
            count=("event_id", "count"),
            success_count=("is_failed", lambda x: (~x).sum()),
            failed_count=("is_failed", "sum"),
        )
        .reset_index()
    )
    
    total = summary["count"].sum()
    summary["pct_of_total"] = (summary["count"] / total * 100).round(2)
    summary = summary.sort_values("count", ascending=False).reset_index(drop=True)
    
    print(f"   ✅ gold_event_summary: {len(summary)} tipos de evento")
    return summary


# ═══════════════════════════════════════════════════════════════════════════
# GUARDAR TABLAS GOLD
# ═══════════════════════════════════════════════════════════════════════════

def guardar_gold(
    user_360: pd.DataFrame,
    daily: pd.DataFrame,
    summary: pd.DataFrame,
    carpeta_gold: str = "data/gold"
) -> dict:
    """Guarda las tres tablas Gold en formato Parquet."""
    os.makedirs(carpeta_gold, exist_ok=True)
    
    rutas = {}
    tablas = {
        "gold_user_360.parquet":      user_360,
        "gold_daily_metrics.parquet": daily,
        "gold_event_summary.parquet": summary,
    }
    
    for nombre, df in tablas.items():
        ruta = os.path.join(carpeta_gold, nombre)
        df.to_parquet(ruta, index=False, compression="snappy", engine="pyarrow")
        tamano_kb = os.path.getsize(ruta) / 1024
        print(f"   ✅ {nombre}: {len(df):,} filas | {tamano_kb:.1f} KB")
        rutas[nombre] = ruta
    
    return rutas


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

def ejecutar_pipeline_gold(
    carpeta_silver: str = "data/silver",
    carpeta_gold: str = "data/gold"
) -> dict:
    """
    Ejecuta el pipeline completo Silver → Gold.
    
    Returns:
        Diccionario con las rutas de los archivos Gold generados
    """
    print("=" * 60)
    print("🥇 INICIANDO PIPELINE — CAPA GOLD")
    print("=" * 60)
    
    print("\n📌 PASO 1: Leyendo Silver...")
    silver = leer_silver(carpeta_silver)
    
    print("\n📌 PASO 2: Construyendo gold_user_360...")
    user_360 = construir_user_360(silver)
    
    print("\n📌 PASO 3: Construyendo gold_daily_metrics...")
    daily = construir_daily_metrics(silver)
    
    print("\n📌 PASO 4: Construyendo gold_event_summary...")
    summary = construir_event_summary(silver)
    
    print("\n📌 PASO 5: Guardando tablas Gold...")
    rutas = guardar_gold(user_360, daily, summary, carpeta_gold)
    
    # Resumen con métricas reales del dataset
    print("\n" + "=" * 60)
    print("✅ PIPELINE GOLD COMPLETADO")
    print(f"   Usuarios en gold_user_360:  {len(user_360):,}")
    top_user = user_360.nlargest(1, "total_amount_cop").iloc[0]
    print(f"   Usuario con más gasto:       {top_user['user_id']} "
          f"({top_user['total_amount_cop']:,.0f} COP)")
    print(f"   Ticket promedio global:      "
          f"{user_360['avg_ticket'].mean():,.0f} COP")
    print(f"   Usuarios con fallos > 3:     "
          f"{(user_360['failed_transactions'] > 3).sum()}")
    print("=" * 60)
    
    return {"user_360": user_360, "daily": daily, "summary": summary, "rutas": rutas}


if __name__ == "__main__":
    ejecutar_pipeline_gold()