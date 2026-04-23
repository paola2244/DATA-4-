"""
Verifica que todas las capas del pipeline están correctas.
Ejecutar después de src/run_pipeline.py.

Uso:
    python verificar_pipeline_completo.py
"""

import pandas as pd
import os

print("=" * 60)
print("🔍 VERIFICACIÓN COMPLETA DEL PIPELINE")
print("=" * 60)

errores = []

# ── BRONZE ──────────────────────────────────────────────────────────────
import glob
bronze_files = glob.glob("data/bronze/events/**/*.parquet", recursive=True)
print(f"\n🥉 BRONZE")
if not bronze_files:
    errores.append("❌ Bronze: No hay archivos Parquet")
else:
    bronze = pd.concat([pd.read_parquet(f) for f in bronze_files])
    print(f"   Archivos: {len(bronze_files)}")
    print(f"   Registros: {len(bronze):,}")
    assert "event_id" in bronze.columns, "Falta event_id"
    assert "ingestion_timestamp" in bronze.columns, "Falta ingestion_timestamp"
    assert "batch_id" in bronze.columns, "Falta batch_id"
    print(f"   ✅ Estructura correcta")

# ── SILVER ──────────────────────────────────────────────────────────────
print(f"\n⚗️  SILVER")
ruta_silver = "data/silver/silver_events.parquet"
if not os.path.exists(ruta_silver):
    errores.append("❌ Silver: No existe silver_events.parquet")
else:
    silver = pd.read_parquet(ruta_silver)
    print(f"   Registros: {len(silver):,}")
    print(f"   Columnas: {len(silver.columns)}")
    
    reqs = ["event_id", "event", "is_failed", "is_transactional",
            "amount_cop", "amount_usd", "timestamp", "date",
            "ip_is_private", "geo_source"]
    for col in reqs:
        assert col in silver.columns, f"Falta columna: {col}"
    
    failed = silver["is_failed"].sum()
    con_monto = silver["amount_cop"].notna().sum()
    print(f"   Eventos fallidos: {failed:,}")
    print(f"   Con monto (COP): {con_monto:,}")
    print(f"   Con amount_usd: {silver['amount_usd'].notna().sum():,}")
    print(f"   ✅ Estructura correcta")

# ── GOLD ─────────────────────────────────────────────────────────────────
print(f"\n🥇 GOLD")
ruta_gold = "data/gold/gold_user_360.parquet"
if not os.path.exists(ruta_gold):
    errores.append("❌ Gold: No existe gold_user_360.parquet")
else:
    gold = pd.read_parquet(ruta_gold)
    print(f"   Usuarios: {len(gold):,}")
    print(f"   Columnas: {len(gold.columns)}")
    
    reqs_gold = ["user_id", "user_name", "total_transactions", "total_amount_cop",
                 "total_amount_usd", "avg_ticket", "failed_transactions",
                 "failure_rate", "top_merchant", "preferred_channel",
                 "days_since_last_tx"]
    for col in reqs_gold:
        assert col in gold.columns, f"Falta columna Gold: {col}"
    
    top = gold.nlargest(3, "total_amount_cop")[
        ["user_id", "user_name", "total_transactions", "total_amount_cop", "top_merchant"]
    ]
    print(f"\n   🏆 Top 3 usuarios por gasto:")
    print(top.to_string(index=False))
    print(f"\n   ✅ Estructura correcta")

# ── TABLAS DE SOPORTE ────────────────────────────────────────────────────
print(f"\n📊 TABLAS DE SOPORTE GOLD")
for tabla in ["gold_daily_metrics.parquet", "gold_event_summary.parquet"]:
    ruta = f"data/gold/{tabla}"
    if os.path.exists(ruta):
        df = pd.read_parquet(ruta)
        print(f"   ✅ {tabla}: {len(df)} filas")
    else:
        errores.append(f"❌ Falta: {tabla}")

# ── RESULTADO FINAL ──────────────────────────────────────────────────────
print("\n" + "=" * 60)
if errores:
    print("❌ VERIFICACIÓN FALLIDA:")
    for e in errores:
        print(f"   {e}")
else:
    print("✅ TODAS LAS CAPAS VERIFICADAS CORRECTAMENTE")
    print("   El pipeline está listo para la Fase 3 (Agente Inteligente)")
print("=" * 60)