"""
Pipeline de la Capa Silver.

Transforma los datos Bronze en datos limpios, tipados y enriquecidos.

Reglas aplicadas:
    - Parsear timestamp a datetime con timezone UTC
    - Convertir amount a float (estaba como int en el JSON)
    - Agregar amount_usd usando ExchangeRate API (con fallback)
    - Resolver geolocalización: payload.city es fuente primaria
      (100% de IPs son privadas en el dataset)
    - Marcar registros fallidos con is_failed = True
    - Eliminar columnas redundantes de Bronze
    - Guardar en Parquet particionado por fecha del evento

Uso:
    python src/silver/pipeline_silver.py
"""

import os
import sys
import glob
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════════════

# Tasa de respaldo COP→USD (actualizar si hay cambios grandes)
# Abril 2026: 1 USD ≈ 4,150 COP
TASA_RESPALDO_COP_USD = 1 / 4150

# Tipos de eventos que representan transacciones financieras reales
# (excluye registros, actualizaciones de perfil, etc.)
EVENTOS_TRANSACCIONALES = {
    "PAYMENT_MADE",
    "PURCHASE_MADE",
    "TRANSFER_SENT",
    "MONEY_ADDED",
    "PAYMENT_FAILED",
}

# Columnas de Bronze que NO pasan a Silver (redundantes o sin valor analítico)
COLUMNAS_A_ELIMINAR = [
    "source",
    "detailType",
    "event_type",
    "transaction_type",
    "event_entity",
    "event_version",
    "account_status",
    "money_source",
    "updated_city",
    "updated_segment",
    "ingestion_date",      # Reemplazado por 'date' derivado del timestamp
]


# ═══════════════════════════════════════════════════════════════════════════
# SERVICIO DE TASAS DE CAMBIO
# ═══════════════════════════════════════════════════════════════════════════

class ExchangeRateService:
    """
    Obtiene la tasa COP→USD con caché de 1 hora.
    Si la API falla, usa la tasa de respaldo sin interrumpir el pipeline.
    """
    
    def __init__(self):
        self._tasa: float = None
        self._ts_cache: float = None
        self._ttl: int = 3600   # 1 hora
    
    def tasa_cop_usd(self) -> float:
        """Retorna la tasa COP→USD actual."""
        # Caché válido?
        if self._tasa and self._ts_cache:
            if (time.time() - self._ts_cache) < self._ttl:
                return self._tasa
        
        # Intentar API
        try:
            r = requests.get(
                "https://open.er-api.com/v6/latest/COP",
                timeout=8
            )
            r.raise_for_status()
            tasa = r.json()["rates"]["USD"]
            self._tasa = tasa
            self._ts_cache = time.time()
            print(f"   ✅ [ExchangeRate] 1 COP = {tasa:.8f} USD (API)")
            return tasa
        except Exception:
            print(f"   ⚠️  [ExchangeRate] API no disponible → tasa de respaldo: "
                  f"1 COP = {TASA_RESPALDO_COP_USD:.8f} USD")
            return TASA_RESPALDO_COP_USD
    
    def convertir(self, monto_cop: float) -> float:
        return round(monto_cop * self.tasa_cop_usd(), 4)


# Instancia global (se reutiliza el caché en todo el pipeline)
fx = ExchangeRateService()


# ═══════════════════════════════════════════════════════════════════════════
# TRANSFORMACIONES PASO A PASO
# ═══════════════════════════════════════════════════════════════════════════

def paso1_leer_bronze(carpeta_bronze: str) -> pd.DataFrame:
    """
    Lee todos los archivos Parquet de Bronze en un solo DataFrame.
    
    Args:
        carpeta_bronze: Ruta a data/bronze/events/
    
    Returns:
        DataFrame con todos los eventos de Bronze concatenados
    """
    patron = os.path.join(carpeta_bronze, "**", "*.parquet")
    archivos = glob.glob(patron, recursive=True)
    
    if not archivos:
        raise FileNotFoundError(
            f"No se encontraron archivos Parquet en {carpeta_bronze}\n"
            f"Ejecuta primero: python src/bronze/pipeline_bronze.py"
        )
    
    print(f"   📂 Leyendo {len(archivos)} archivo(s) Parquet de Bronze...")
    df = pd.concat([pd.read_parquet(f) for f in archivos], ignore_index=True)
    print(f"   ✅ {len(df):,} registros cargados de Bronze")
    return df


def paso2_limpiar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte campos al tipo de dato correcto.
    
    Transformaciones:
        - timestamp: string ISO → datetime con timezone UTC
        - amount: int → float (permite valores decimales futuros)
        - balance_before/after: int → float
        - installments: object → int (con manejo de nulos)
        - user_email: string → lowercase + strip
        - date: extraído del timestamp (para particionado)
    """
    df = df.copy()
    
    # timestamp → datetime UTC
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    
    # Extraer columna de fecha (para particionado y consultas por día)
    df["date"] = df["timestamp"].dt.date
    
    # Cantidades monetarias → float
    for col in ["amount", "balance_before", "balance_after", "initial_balance"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    
    # installments → int (rellenar nulos con 1 — pago único)
    if "installments" in df.columns:
        df["installments"] = (
            pd.to_numeric(df["installments"], errors="coerce")
            .fillna(1)
            .astype("Int64")   # Int64 (con mayúscula) soporta nulos
        )
    
    # email → lowercase + strip espacios
    if "user_email" in df.columns:
        df["user_email"] = df["user_email"].str.lower().str.strip()
    
    print(f"   ✅ Tipos de datos corregidos")
    return df


def paso3_agregar_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega columnas calculadas (flags) que facilitan el análisis.
    
    Columnas nuevas:
        - is_failed: True si el evento terminó en FAILED
        - is_transactional: True si el evento mueve dinero
        - ip_is_private: True si la IP es del rango privado
        - geo_source: 'payload_location' o 'ipapi' según origen de geo
    """
    df = df.copy()
    
    # Flag de fallo
    df["is_failed"] = df["event_status"] == "FAILED"
    
    # Flag de transaccionalidad
    df["is_transactional"] = df["event"].isin(EVENTOS_TRANSACCIONALES)
    
    # Flag de IP privada
    # Para este dataset: 100% son privadas (192.168.x.x)
    # Para eventos del ecommerce pueden llegar IPs públicas
    df["ip_is_private"] = df["ip"].fillna("").str.startswith(
        ("192.168.", "10.", "172.16.", "172.17.", "172.18.",
         "172.19.", "172.20.", "172.21.", "172.22.", "172.23.",
         "172.24.", "172.25.", "172.26.", "172.27.", "172.28.",
         "172.29.", "172.30.", "172.31.")
    )
    
    # Fuente de geolocalización
    df["geo_source"] = df["ip_is_private"].map(
        {True: "payload_location", False: "ipapi"}
    )
    
    print(f"   ✅ Flags agregados:")
    print(f"      is_failed: {df['is_failed'].sum()} registros")
    print(f"      is_transactional: {df['is_transactional'].sum()} registros")
    print(f"      ip_is_private: {df['ip_is_private'].sum()} / {len(df)} registros")
    return df


def paso4_enriquecer_geolocalización(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resuelve la geolocalización de cada evento.
    
    Estrategia (en orden de prioridad):
        1. Si IP es pública → llamar a ipapi.co
        2. Si IP es privada (caso del dataset actual) → usar payload.location_city
        3. Si location_city es null → usar user_city (campo city del payload)
        4. Fallback final → 'Colombia' para country, 'Desconocida' para city
    
    Para el dataset actual: 100% caerá en el caso 2 o 3.
    Para eventos del ecommerce con IPs públicas: caerá en caso 1.
    """
    df = df.copy()
    
    # Resolver location_city
    # Jerarquía: location_city → user_city (payload.city) → 'Desconocida'
    df["location_city"] = (
        df["location_city"]
        .fillna(df["user_city"])
        .fillna("Desconocida")
    )
    
    # Resolver location_country
    df["location_country"] = df["location_country"].fillna("Colombia")
    
    # Para IPs públicas (del ecommerce simulado): llamar a ipapi
    ips_publicas = df[~df["ip_is_private"] & df["ip"].notna()]["ip"].unique()
    
    if len(ips_publicas) > 0:
        print(f"   🌐 Resolviendo {len(ips_publicas)} IPs públicas con ipapi...")
        cache_ipapi = {}
        
        for ip in ips_publicas:
            if ip in cache_ipapi:
                continue
            try:
                time.sleep(0.5)  # Rate limit: max 2 req/s en plan gratuito
                r = requests.get(f"https://ipapi.co/{ip}/json/", timeout=6)
                data = r.json()
                if "error" not in data:
                    cache_ipapi[ip] = {
                        "city": data.get("city", ""),
                        "country": data.get("country_name", "")
                    }
            except Exception:
                pass
        
        # Aplicar los resultados de ipapi donde aplique
        for ip, geo in cache_ipapi.items():
            mask = (df["ip"] == ip) & (~df["ip_is_private"])
            if geo["city"]:
                df.loc[mask, "location_city"] = geo["city"]
            if geo["country"]:
                df.loc[mask, "location_country"] = geo["country"]
    else:
        print(f"   📍 Geolocalización: usando payload.city "
              f"(todas las IPs son privadas)")
    
    return df


def paso5_enriquecer_moneda(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega la columna amount_usd convirtiendo desde COP.
    
    Solo aplica a registros que tienen amount (1,423 de 2,000).
    Los eventos sin monto (USER_REGISTERED, USER_PROFILE_UPDATED)
    quedrán con amount_usd = null, lo cual es correcto.
    """
    df = df.copy()
    
    # Obtener tasa una sola vez (el servicio la cachea)
    tasa = fx.tasa_cop_usd()
    
    # Calcular amount_usd solo donde existe amount
    df["amount_usd"] = df["amount"].apply(
        lambda x: round(x * tasa, 4) if pd.notna(x) else None
    )
    
    registros_convertidos = df["amount_usd"].notna().sum()
    print(f"   ✅ amount_usd calculado para {registros_convertidos:,} registros")
    print(f"      Tasa usada: 1 COP = {tasa:.8f} USD")
    return df


def paso6_renombrar_y_seleccionar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas para consistencia y elimina las redundantes.
    
    Bronze usa nombres como 'amount' (ambiguo).
    Silver usa 'amount_cop' (explícito sobre la moneda).
    """
    df = df.copy()
    
    # Renombrar para mayor claridad
    renombres = {
        "amount": "amount_cop",          # Explícito: COP
        "user_id": "user_id",            # Sin cambio
        "is_duplicate": "bronze_is_duplicate",  # Preservar para auditoría
    }
    df = df.rename(columns=renombres)
    
    # Eliminar columnas redundantes de Bronze
    columnas_a_eliminar = [c for c in COLUMNAS_A_ELIMINAR if c in df.columns]
    df = df.drop(columns=columnas_a_eliminar)
    
    print(f"   ✅ Columnas Silver finales: {len(df.columns)}")
    return df


def paso7_guardar_silver(df: pd.DataFrame, carpeta_silver: str) -> str:
    """
    Guarda el DataFrame Silver en Parquet particionado por fecha del evento.
    
    Nota: Silver se particiona por la fecha DEL EVENTO (no de ingesta).
    Esto permite consultar Silver por períodos de negocio de forma eficiente.
    """
    os.makedirs(carpeta_silver, exist_ok=True)
    
    # Guardar como un solo archivo Silver (para este proyecto)
    # En producción con millones de registros, se particionaría por date
    ruta = os.path.join(carpeta_silver, "silver_events.parquet")
    
    df.to_parquet(ruta, index=False, compression="snappy", engine="pyarrow")
    
    tamano_mb = os.path.getsize(ruta) / (1024 * 1024)
    print(f"   ✅ Silver guardado: {ruta}")
    print(f"      Filas: {len(df):,} | Tamaño: {tamano_mb:.2f} MB")
    return ruta


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

def ejecutar_pipeline_silver(
    carpeta_bronze: str = "data/bronze/events",
    carpeta_silver: str = "data/silver"
) -> pd.DataFrame:
    """
    Ejecuta el pipeline completo Bronze → Silver.
    
    Returns:
        DataFrame Silver listo para ser consumido por el pipeline Gold
    """
    print("=" * 60)
    print("⚗️  INICIANDO PIPELINE — CAPA SILVER")
    print("=" * 60)
    
    print("\n📌 PASO 1: Leyendo datos de Bronze...")
    df = paso1_leer_bronze(carpeta_bronze)
    
    print("\n📌 PASO 2: Limpiando tipos de datos...")
    df = paso2_limpiar_tipos(df)
    
    print("\n📌 PASO 3: Agregando flags...")
    df = paso3_agregar_flags(df)
    
    print("\n📌 PASO 4: Resolviendo geolocalización...")
    df = paso4_enriquecer_geolocalización(df)
    
    print("\n📌 PASO 5: Convirtiendo monedas...")
    df = paso5_enriquecer_moneda(df)
    
    print("\n📌 PASO 6: Seleccionando columnas finales...")
    df = paso6_renombrar_y_seleccionar_columnas(df)
    
    print("\n📌 PASO 7: Guardando Silver en Parquet...")
    paso7_guardar_silver(df, carpeta_silver)
    
    # Estadísticas finales
    print("\n" + "=" * 60)
    print("✅ PIPELINE SILVER COMPLETADO")
    print(f"   Registros totales:    {len(df):,}")
    print(f"   Con monto (COP/USD):  {df['amount_cop'].notna().sum():,}")
    print(f"   Eventos fallidos:     {df['is_failed'].sum():,}")
    print(f"   Columnas finales:     {len(df.columns)}")
    print(f"   Usuarios únicos:      {df['user_id'].nunique():,}")
    print("=" * 60)
    
    return df


if __name__ == "__main__":
    ejecutar_pipeline_silver()