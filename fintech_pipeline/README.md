# 🏦 Fintech Data Pipeline — Plataforma de Datos en Tiempo Real

> Arquitectura **Bronze → Silver → Gold** para procesamiento de eventos financieros,
> con bus de eventos basado en `asyncio` y receptor HTTP con **FastAPI**.

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://python.org)
[![pandas](https://img.shields.io/badge/pandas-2.0%2B-green)](https://pandas.pydata.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-teal)](https://fastapi.tiangolo.com)
[![Estado](https://img.shields.io/badge/Estado-Fase%202%20completa%20%7C%20Fase%203%20pr%C3%B3xima-brightgreen)]()

---

## 📋 Tabla de Contenidos

1. [Descripción General](#1-descripción-general)
2. [Arquitectura del Sistema](#2-arquitectura-del-sistema)
3. [Estructura de Carpetas](#3-estructura-de-carpetas)
4. [Dataset Principal](#4-dataset-principal)
5. [Instalación y Configuración](#5-instalación-y-configuración)
6. [Componentes del Pipeline](#6-componentes-del-pipeline)
   - [Capa Bronze](#61-capa-bronze)
   - [Bus de Eventos — asyncio](#62-bus-de-eventos--asyncio)
   - [Receptor HTTP — FastAPI](#63-receptor-http--fastapi)
   - [Capa Silver](#64-capa-silver)
   - [Capa Gold](#65-capa-gold)
7. [APIs Externas](#7-apis-externas)
8. [Cómo Ejecutar el Proyecto](#8-cómo-ejecutar-el-proyecto)
9. [Estado del Proyecto por Fases](#9-estado-del-proyecto-por-fases)
10. [Decisiones de Diseño](#10-decisiones-de-diseño)
11. [Riesgos Identificados y Mitigaciones](#11-riesgos-identificados-y-mitigaciones)
12. [Próximos Pasos — Fase 3](#12-próximos-pasos--fase-3)
13. [Dependencias](#13-dependencias)

---

## 1. Descripción General

Este proyecto implementa una **plataforma de datos en tiempo real** para una fintech colombiana.
El sistema captura eventos de negocio (pagos, transferencias, compras, recargas) desde dos fuentes:

- **Archivo JSON base** (`fintech_events_v4.json`) — carga histórica inicial de 2.000 eventos.
- **E-commerce externo** — eventos en tiempo casi real enviados vía HTTP al receptor FastAPI,
  que los pone en el bus de eventos `asyncio` para su procesamiento.

Los datos fluyen a través de tres capas progresivas de refinamiento (arquitectura Medallion de
Databricks), desde datos crudos hasta una **Visión 360 del usuario** lista para ser consultada
por un agente inteligente en lenguaje natural (Fase 3).

### Problema que resuelve

Las organizaciones capturan eventos pero no cuentan con una arquitectura que permita procesarlos
en tiempo real, integrarlos y transformarlos en información útil. Este proyecto construye ese
puente: de evento crudo a insight accionable, con trazabilidad completa en cada paso.

---

## 2. Arquitectura del Sistema

```
╔══════════════════════════════════════════════════════════════════════╗
║                        FUENTES DE DATOS                             ║
╠══════════════════════╦═══════════════════════════════════════════════╣
║  fintech_events_v4   ║         E-commerce Externo                   ║
║  .json               ║                                              ║
║  (2.000 eventos)     ║   POST /eventos → FastAPI (puerto 8000)      ║
║  Carga inicial       ║                                              ║
╚══════════════════════╩═══════════════════════════════════════════════╝
           │                            │
           │                            ▼
           │              ┌─────────────────────────┐
           │              │    BUS DE EVENTOS        │
           │              │    asyncio.Queue         │
           │              │    (maxsize = 1.000)     │
           │              │                          │
           │              │  Productor → Cola →      │
           │              │  Consumidor Bronze        │
           │              └──────────┬──────────────┘
           │                         │
           └─────────────────────────┘
                          │  micro-batch
                          │  (cada N eventos ó N segundos)
                          ▼
        ╔═════════════════════════════════════════════╗
        ║              CAPA BRONZE                    ║
        ║                                             ║
        ║  • Aplana JSON anidado (3 niveles)          ║
        ║  • Agrega metadatos de ingesta:             ║
        ║    ingestion_timestamp, source_file,        ║
        ║    batch_id, ingestion_date                 ║
        ║  • Detecta y registra duplicados por id     ║
        ║  • Guarda en Parquet particionado por fecha ║
        ║                                             ║
        ║  data/bronze/events/date=YYYY-MM-DD/        ║
        ║  batch_<id>.parquet                         ║
        ╚═════════════════════════════════════════════╝
                          │
                          ▼
        ╔═════════════════════════════════════════════╗
        ║              CAPA SILVER                    ║
        ║                                             ║
        ║  • Parsea timestamp → datetime UTC          ║
        ║  • Convierte amount int → float (COP)       ║
        ║  • Calcula amount_usd (ExchangeRate API)    ║
        ║  • Resuelve geolocalización:                ║
        ║    IPs privadas → usa payload.city          ║
        ║    IPs públicas → ipapi.co                  ║
        ║  • Agrega flags: is_failed,                 ║
        ║    is_transactional, ip_is_private          ║
        ║  • Elimina columnas redundantes             ║
        ║                                             ║
        ║  data/silver/silver_events.parquet          ║
        ╚═════════════════════════════════════════════╝
                          │
                          ▼
        ╔═════════════════════════════════════════════╗
        ║              CAPA GOLD                      ║
        ║                                             ║
        ║  gold_user_360 (489 usuarios)               ║
        ║  • Visión 360 por usuario                   ║
        ║  • 21 columnas: gasto, ticket promedio,     ║
        ║    merchant favorito, canal preferido,      ║
        ║    tasa de fallo, días sin transaccionar... ║
        ║                                             ║
        ║  gold_daily_metrics (tendencias por día)    ║
        ║  gold_event_summary (KPIs por tipo evento)  ║
        ║                                             ║
        ║  data/gold/*.parquet                        ║
        ╚═════════════════════════════════════════════╝
                          │
                          ▼  (Fase 3 — próxima)
        ╔═════════════════════════════════════════════╗
        ║         AGENTE INTELIGENTE (Fase 3)         ║
        ║                                             ║
        ║  • Text-to-SQL con Claude AI                ║
        ║  • DuckDB como motor de consulta            ║
        ║  • Dashboard Streamlit                      ║
        ╚═════════════════════════════════════════════╝
```

---

## 3. Estructura de Carpetas

```
fintech_pipeline/
│
├── data/                              ← Todas las capas de datos (no subir a Git)
│   ├── raw/
│   │   └── fintech_events_v4.json    ← Dataset base original — NO modificar
│   ├── bronze/
│   │   ├── events/                   ← Eventos fintech (aplanados con aplanar_evento)
│   │   │   └── date=YYYY-MM-DD/
│   │   │       └── batch_<id>.parquet
│   │   ├── metric/                   ← Mensajes tipo "metric" (envelope estándar)
│   │   ├── record/                   ← Mensajes tipo "record" (envelope estándar)
│   │   └── log/                      ← Mensajes tipo "log" (envelope estándar)
│   ├── silver/
│   │   └── silver_events.parquet
│   └── gold/
│       ├── gold_user_360.parquet     ← Tabla principal — Visión 360
│       ├── gold_daily_metrics.parquet
│       └── gold_event_summary.parquet
│
├── src/                              ← Código fuente modular
│   ├── bronze/
│   │   ├── ingest.py                 ← Carga y aplanamiento del JSON
│   │   ├── metadata.py              ← Metadatos de ingesta (batch_id, timestamp)
│   │   ├── save.py                  ← Escritura en formato Parquet
│   │   ├── pipeline_bronze.py       ← Orquestador de la capa Bronze
│   │   └── simulator.py             ← Generador de eventos sintéticos (Faker)
│   ├── bus/
│   │   ├── message_schema.py        ← Envelope estándar multi-tipo + generadores
│   │   ├── event_bus_asyncio.py     ← Bus asyncio: EventBus + BronzeConsumer
│   │   ├── dataset_producer.py      ← Reproductor del dataset real (sin e-commerce)
│   │   ├── pipeline_trigger.py      ← Dispara Silver→Gold tras cada batch Bronze
│   │   ├── api_receiver.py          ← Receptor HTTP FastAPI puerto 8000
│   │   ├── ecommerce_api.py         ← API de ingesta genérica puerto 8001
│   │   └── start_full_pipeline.py   ← Lanzador integrado (modo standalone)
│   ├── silver/
│   │   └── pipeline_silver.py       ← Transformación Bronze → Silver (7 pasos)
│   ├── gold/
│   │   └── pipeline_gold.py         ← Agregación Silver → Gold (3 tablas)
│   └── run_pipeline.py              ← Script maestro Bronze → Silver → Gold
│
├── notebooks/
│   ├── 01_explorar_json.ipynb       ← Exploración inicial del dataset
│   └── 02_prueba_apis.py            ← Validación de APIs externas
│
├── logs/
│   └── duplicates_<fecha>.csv       ← Registro de duplicados detectados en Bronze
│
├── .env                              ← API keys (NUNCA subir a Git)
├── .env.example                      ← Plantilla de variables de entorno
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 4. Dataset Principal

**Archivo:** `data/raw/fintech_events_v4.json`
**Fuente:** Datos simulados de una fintech colombiana
**Propósito:** Carga histórica inicial del pipeline

### Estadísticas del Dataset

| Métrica | Valor |
|---|---|
| Total de eventos | 2.000 |
| Usuarios únicos | 489 |
| Eventos exitosos | 1.700 (85%) |
| Eventos fallidos | 300 (15%) |
| Ciudades | Bogotá, Medellín, Cali, Barranquilla, Cartagena |
| Segmentos | premium, student, family, young_professional |
| IPs privadas | 2.000 / 2.000 (100%) → fallback a `payload.city` |

### Tipos de Eventos

| Evento | Cantidad | Descripción |
|---|---|---|
| `USER_REGISTERED` | 293 | Registro de nuevo usuario |
| `USER_PROFILE_UPDATED` | 284 | Actualización de perfil |
| `MONEY_ADDED` | 266 | Recarga de saldo |
| `PAYMENT_MADE` | 294 | Pago realizado exitosamente |
| `PURCHASE_MADE` | 283 | Compra realizada |
| `TRANSFER_SENT` | 280 | Transferencia enviada |
| `PAYMENT_FAILED` | 300 | Pago fallido |

### Estructura del JSON (3 niveles de profundidad)

```
evento
├── source            → "fintech.app" / "ecommerce.app"
├── detailType        → "event"
└── detail/
    ├── id            → UUID único del evento
    ├── event         → Tipo (PAYMENT_MADE, TRANSFER_SENT, etc.)
    ├── eventStatus   → "SUCCESS" | "FAILED"
    ├── payload/
    │   ├── userId, name, age, email, city, segment
    │   ├── timestamp, accountId
    │   ├── amount (COP), currency, merchant, category
    │   ├── paymentMethod, installments
    │   ├── balanceBefore, balanceAfter
    │   └── location/ → {city, country}
    └── metadata/
        ├── device, os, ip, channel
```

---

## 5. Instalación y Configuración

### Prerrequisitos

- Python 3.10 o superior
- Visual Studio Code (recomendado) con extensión Python + Jupyter
- Git

### Paso 1 — Clonar y crear entorno virtual

```bash
git clone https://github.com/tu-usuario/fintech_pipeline.git
cd fintech_pipeline

# Crear entorno virtual
python -m venv venv

# Activar (Windows)
venv\Scripts\activate
# Activar (Mac/Linux)
source venv/bin/activate
```

### Paso 2 — Instalar dependencias

```bash
pip install -r requirements.txt
```

> **Windows — emojis en consola:** los scripts usan emojis en los prints.
> Ejecutar siempre con el flag `-X utf8` para evitar errores de encoding:
>
> ```bash
> python -X utf8 src/run_pipeline.py
> python -X utf8 src/bus/start_full_pipeline.py
> ```
>
> Alternativa permanente: agregar `PYTHONUTF8=1` a las variables de entorno del sistema.

### Paso 3 — Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con tus claves de API
```

Contenido de `.env`:
```
# ExchangeRate API (registro gratuito en https://www.exchangerate-api.com)
EXCHANGE_RATE_API_KEY=tu_clave_aqui

# ipapi (opcional — plan gratuito sin key en https://ipapi.co)
IPAPI_KEY=
```

### Paso 4 — Ubicar el dataset

```bash
# Copiar el JSON base a su lugar
cp fintech_events_v4.json data/raw/fintech_events_v4.json
```

### Paso 5 — Crear la estructura de carpetas

```bash
mkdir -p data/raw data/bronze/events data/silver data/gold
mkdir -p src/bronze src/bus src/silver src/gold notebooks logs
```

---

## 6. Componentes del Pipeline

### 6.1 Capa Bronze

**Propósito:** Capturar los eventos exactamente como llegan, sin modificar ningún valor.
Es la fuente de verdad histórica e inmutable del sistema.

**Regla de oro de Bronze:** *Guardar todo, modificar nada.*

**Archivos:**

| Archivo | Descripción |
|---|---|
| `src/bronze/ingest.py` | Lee el JSON y aplana la estructura de 3 niveles en un DataFrame tabular plano. Cada campo anidado (`detail.payload.userId`) se convierte en una columna (`user_id`). |
| `src/bronze/metadata.py` | Agrega tres columnas de trazabilidad a cada lote: `ingestion_timestamp` (cuándo llegó), `source_file` (de dónde vino), `batch_id` (UUID corto del lote). |
| `src/bronze/save.py` | Escribe el DataFrame en formato Parquet con compresión Snappy, particionado por `date=YYYY-MM-DD`. Parquet ocupa ~88% menos que CSV y es 10x más rápido de leer. |
| `src/bronze/pipeline_bronze.py` | Orquestador que ejecuta los 5 pasos en orden: leer → aplanar → metadatos → duplicados → guardar. |
| `src/bronze/simulator.py` | Genera eventos de prueba con estructura idéntica al JSON base. Se usa cuando el e-commerce aún no está disponible. |

**Columnas que agrega Bronze (no existen en el JSON original):**

| Columna | Tipo | Descripción |
|---|---|---|
| `ingestion_timestamp` | string ISO | Fecha y hora exacta en que se procesó el lote |
| `source_file` | string | Nombre del archivo o fuente de origen |
| `batch_id` | string | Identificador único del lote (primeros 8 chars de UUID) |
| `ingestion_date` | string | Fecha de ingesta en formato `YYYY-MM-DD` (para particionado) |
| `is_duplicate` | bool | `True` si el `event_id` aparece más de una vez en el lote |

**Cómo ejecutar Bronze:**
```bash
python src/bronze/pipeline_bronze.py
```

**Salida esperada:**
```
data/bronze/events/date=2026-04-21/batch_a3f8c291.parquet
  → 2.000 filas × 34 columnas | ~180 KB
```

---

### 6.2 Bus de Eventos — asyncio

**Propósito:** Transportar mensajes de cualquier fuente al consumidor Bronze de forma
asíncrona y desacoplada. El bus acepta eventos fintech, métricas, registros y logs.

**Decisión de diseño:** Se evaluaron tres alternativas y se eligió `asyncio`:

| Herramienta | Evaluada | Razón de la decisión |
|---|---|---|
| `asyncio.Queue` | ✅ **ELEGIDA** | Sin instalación extra, corre en local, patrón micro-batch nativo |
| `PyPubSub` | Evaluada | Más simple pero síncrona, menos adecuada para I/O concurrente |
| `Kafka / AWS EventBridge` | Evaluada | Correctas para producción masiva, exceso para este reto |

**Archivos del bus:**

| Archivo | Rol |
|---|---|
| `message_schema.py` | Envelope estándar multi-tipo. Define `crear_mensaje()`, `clasificar_mensajes()`, generadores sintéticos por tipo. |
| `event_bus_asyncio.py` | `EventBus` (cola central) + `BronzeConsumer` (escribe Parquet). El consumer clasifica mensajes por tipo antes de guardar. |
| `dataset_producer.py` | Lee `fintech_events_v4.json` y publica los 2.000 eventos al bus. Sustituye al e-commerce mientras no está disponible. |
| `pipeline_trigger.py` | Dispara Silver → Gold en background thread después de cada batch Bronze. Incluye throttling configurable. |

**Envelope estándar de mensaje:**
```json
{
    "msg_type": "event | metric | record | log | alert",
    "source": "ecommerce | crm | mobile_app | ...",
    "message_id": "<uuid-autogenerado>",
    "timestamp": "<ISO 8601 UTC autogenerado>",
    "data": { "...payload específico del tipo..." },
    "metadata": { "device": "mobile", "channel": "app" }
}
```

**Componentes internos de `event_bus_asyncio.py`:**

**`EventBus` (la cola central)**
```
asyncio.Queue con maxsize=1.000
  → Acepta FintechEvent (dataclass) o dict crudo con cualquier envelope
  → Si la cola está llena, el productor espera automáticamente (backpressure)
```

**`BronzeConsumer` (escribe a la capa Bronze)**
```
Patrón micro-batch:
  1. Espera el primer mensaje de la cola (bloqueante)
  2. Recoge todos los disponibles sin esperar más
  3. Clasifica por msg_type → guarda en sub-carpeta Bronze correspondiente:
     - fintech_legacy → data/bronze/events/
     - metric         → data/bronze/metric/
     - record         → data/bronze/record/
     - log            → data/bronze/log/
  4. Llama al PipelineTrigger (opcional) → Silver → Gold
```

**Modo standalone — pipeline integrado con el dataset real:**
```bash
python src/bus/start_full_pipeline.py
```

**Opciones de `start_full_pipeline.py`:**
```bash
# Rápido (sin delay, procesa los 2.000 eventos lo más veloz posible)
python src/bus/start_full_pipeline.py --delay 0

# Solo Bronze sin disparar Silver/Gold
python src/bus/start_full_pipeline.py --no-trigger

# Loop infinito (stress test)
python src/bus/start_full_pipeline.py --loop --delay 0.01
```

---

### 6.3 API de Ingesta — FastAPI

**Propósito:** Exponer endpoints HTTP genéricos para recibir mensajes de cualquier tipo
(eventos, métricas, registros, logs, alertas) y encolarlos en el bus `asyncio`.
Al arrancar, inicia automáticamente el `BronzeConsumer` en background.

**Dos servidores FastAPI:**

| Puerto | Archivo | Rol |
|---|---|---|
| 8000 | `api_receiver.py` | **Receptor principal.** Encola mensajes en el bus. Corre el BronzeConsumer. |
| 8001 | `ecommerce_api.py` | **API de ingesta genérica.** Genera datos sintéticos de cualquier tipo y los envía al receptor. Sustituye al e-commerce. |

**Endpoints del receptor (puerto 8000):**

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/ingest` | Ingesta un mensaje de cualquier tipo — endpoint principal |
| `POST` | `/eventos` | Alias de backward-compat para el formato legacy fintech |
| `GET` | `/health` | Estado rápido del bus y consumidor |
| `GET` | `/pipeline/status` | Estadísticas completas: bus, consumer, trigger |
| `POST` | `/pipeline/run` | Fuerza ejecución inmediata de Silver → Gold |
| `DELETE` | `/pipeline/flush` | Procesa el batch pendiente en la cola |

**Endpoints de la API de ingesta (puerto 8001):**

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/ingest` | Envía un mensaje con envelope estándar al receptor |
| `POST` | `/ingest/batch` | Hasta 500 mensajes en una sola llamada |
| `POST` | `/simulate` | Genera N mensajes sintéticos en background (`?msg_type=event&n=200&tps=5`) |
| `POST` | `/events/payment` | Atajo: PAYMENT_MADE |
| `POST` | `/events/purchase` | Atajo: PURCHASE_MADE |
| `POST` | `/events/transfer` | Atajo: TRANSFER_SENT |
| `POST` | `/events/failure` | Atajo: PAYMENT_FAILED |
| `POST` | `/metrics/snapshot` | Atajo: métrica de negocio |
| `POST` | `/records/user` | Atajo: registro de usuario |
| `GET` | `/schema` | Documentación del envelope estándar |
| `GET` | `/health` | Estado de la conexión con el receptor |

**Cómo iniciar los servidores:**
```bash
# Terminal 1 — receptor (procesa el bus y escribe a Bronze)
uvicorn src.bus.api_receiver:app --port 8000 --reload

# Terminal 2 — API de ingesta (simula fuentes externas)
uvicorn src.bus.ecommerce_api:app --port 8001 --reload
```

**Ejemplo — ingesta de un mensaje genérico:**
```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "msg_type": "event",
    "source": "ecommerce",
    "data": {
      "subtype": "PAYMENT_MADE",
      "user_id": "user_042",
      "amount": 150000,
      "currency": "COP",
      "merchant": "Rappi"
    }
  }'
```

**Respuesta esperada:**
```json
{ "status": "accepted", "msg_type": "event", "queue_pending": 1, "total_received": 1 }
```

**Simulación sin e-commerce — desde la API de ingesta (puerto 8001):**
```bash
# Generar 200 eventos de pago a 5 por segundo
curl -X POST "http://localhost:8001/simulate?msg_type=event&n=200&tps=5"

# Generar 50 métricas de negocio
curl -X POST "http://localhost:8001/simulate?msg_type=metric&n=50&tps=2"
```

Swagger UI interactivo: `http://localhost:8001/docs`

---

### 6.4 Capa Silver

**Propósito:** Limpiar, tipificar y enriquecer los datos Bronze. Silver es la fuente
de verdad para análisis — tiene garantías de calidad que Bronze no tiene.

**Regla de plata de Silver:** *Limpiar y enriquecer, sin agregar todavía.*

**Archivo:** `src/silver/pipeline_silver.py`

**7 pasos del pipeline Silver:**

| Paso | Nombre | Descripción |
|---|---|---|
| 1 | Leer Bronze | Lee todos los Parquet de Bronze con `glob` y los concatena |
| 2 | Limpiar tipos | `timestamp` → datetime UTC · `amount` → float · `installments` → Int64 · `email` → lowercase |
| 3 | Agregar flags | `is_failed` (bool) · `is_transactional` (bool) · `ip_is_private` (bool) · `geo_source` (string) |
| 4 | Geolocalización | IPs privadas → usa `payload.city` como fallback · IPs públicas → llama a ipapi.co |
| 5 | Conversión moneda | Calcula `amount_usd` = `amount_cop × tasa_COP_USD` (API con caché de 1h) |
| 6 | Columnas finales | Renombra `amount` → `amount_cop` · Elimina 9 columnas redundantes de Bronze |
| 7 | Guardar | Escribe `data/silver/silver_events.parquet` (Snappy, ~2 MB) |

**Hallazgo importante sobre geolocalización:**
> El 100% de las IPs en el dataset base son privadas (rango 192.168.x.x).
> ipapi.co no puede resolver IPs privadas. El pipeline aplica automáticamente
> el campo `city` del payload como fuente primaria de geolocalización.
> Cuando el e-commerce envíe eventos con IPs públicas, esas sí se resolverán
> con ipapi.co de forma transparente.

**Columnas clave que agrega Silver:**

| Columna | Tipo | Descripción |
|---|---|---|
| `amount_cop` | float | Monto en pesos colombianos (renombrado desde `amount`) |
| `amount_usd` | float | Monto convertido a USD vía ExchangeRate API |
| `is_failed` | bool | `True` para eventos `PAYMENT_FAILED` |
| `is_transactional` | bool | `True` para eventos que mueven dinero |
| `ip_is_private` | bool | `True` para IPs del rango 192.168.x.x |
| `geo_source` | string | `'payload_location'` o `'ipapi'` según origen |
| `date` | date | Fecha del evento extraída del timestamp (para particionado) |

**Registros con y sin monto (por diseño del negocio):**
- 1.423 registros **con** `amount_cop` y `amount_usd` (eventos transaccionales)
- 577 registros **sin** monto (USER_REGISTERED y USER_PROFILE_UPDATED — correcto por diseño)

**Cómo ejecutar Silver:**
```bash
python src/silver/pipeline_silver.py
```

---

### 6.5 Capa Gold

**Propósito:** Producir datasets listos para el negocio. Gold responde preguntas
analíticas sin necesidad de transformaciones adicionales.

**Regla de oro de Gold:** *Una fila = una respuesta de negocio.*

**Archivo:** `src/gold/pipeline_gold.py`

**Tres tablas generadas:**

#### `gold_user_360.parquet` — Tabla principal

Una fila por usuario (489 filas). Consolida toda la información de un usuario
a través de todos sus eventos históricos.

| Columna | Tipo | Descripción |
|---|---|---|
| `user_id` | string | Identificador único — clave primaria |
| `user_name` | string | Nombre (primer valor registrado) |
| `user_email` | string | Email normalizado a minúsculas |
| `user_age` | int | Edad |
| `user_segment` | string | Segmento: premium / student / family / young_professional |
| `city` | string | Ciudad registrada |
| `total_events` | int | Total de todos los eventos (incluye registros, actualizaciones) |
| `total_transactions` | int | Solo PAYMENT_MADE + PURCHASE_MADE + TRANSFER_SENT exitosos |
| `total_amount_cop` | float | Suma de montos transaccionados en COP |
| `total_amount_usd` | float | Suma de montos transaccionados en USD |
| `avg_ticket` | float | Monto promedio por transacción en COP |
| `failed_transactions` | int | Número de transacciones fallidas |
| `failure_rate` | float | Porcentaje de fallos (0.0 → 1.0) |
| `balance_current` | float | Último `balanceAfter` conocido en COP |
| `top_merchant` | string | Comercio con más transacciones del usuario |
| `top_category` | string | Categoría más frecuente (food, shopping, transport…) |
| `preferred_channel` | string | Canal más usado: app / web / api |
| `preferred_device` | string | Dispositivo más usado: mobile / web |
| `last_transaction_date` | datetime | Fecha de la última transacción exitosa |
| `last_event_date` | datetime | Fecha del último evento de cualquier tipo |
| `days_since_last_tx` | int | Días transcurridos desde la última transacción |

#### `gold_daily_metrics.parquet` — Métricas por día

Tendencias temporales para el dashboard de negocio.

| Columna | Descripción |
|---|---|
| `date` | Fecha del evento |
| `total_events` | Total de eventos ese día |
| `total_transactions` | Transacciones exitosas |
| `total_amount_cop` | Volumen total en COP |
| `failed_count` | Número de fallos |
| `unique_users` | Usuarios únicos activos |

#### `gold_event_summary.parquet` — KPIs por tipo de evento

Vista ejecutiva para el dashboard principal.

| Columna | Descripción |
|---|---|
| `event` | Tipo de evento |
| `count` | Total de ocurrencias |
| `success_count` | Exitosos |
| `failed_count` | Fallidos |
| `pct_of_total` | Porcentaje del total de eventos |

**Cómo ejecutar Gold:**
```bash
python src/gold/pipeline_gold.py
```

---

## 7. APIs Externas

### ExchangeRate API

| Campo | Detalle |
|---|---|
| **URL** | `https://open.er-api.com/v6/latest/COP` |
| **Autenticación** | Sin key en plan gratuito |
| **Límite** | 1.500 requests/mes |
| **Uso** | Obtener tasa COP→USD para calcular `amount_usd` en Silver |
| **Caché** | 1 hora en memoria (evita llamadas por cada registro) |
| **Fallback** | Si la API falla: tasa de respaldo `1 USD = 4.150 COP` (Abril 2026) |

**Patrón de uso en Silver:**
```python
# Una sola llamada por ejecución del pipeline (el caché la reutiliza)
tasa = fx.tasa_cop_usd()                        # → 0.00024096...
amount_usd = round(amount_cop * tasa, 4)        # → 86.77 USD
```

### ipapi.co

| Campo | Detalle |
|---|---|
| **URL** | `https://ipapi.co/{ip}/json/` |
| **Autenticación** | Sin key (plan gratuito: 1.000 req/día) |
| **Uso** | Enriquecer con país, ciudad y timezone a partir de la IP |
| **Limitación actual** | Las IPs del dataset son 100% privadas → no resolvibles |
| **Estrategia aplicada** | `payload.city` como fuente primaria de geolocalización |
| **Activación futura** | Se activará automáticamente para IPs públicas del e-commerce |

### CoinGecko (opcional)

| Campo | Detalle |
|---|---|
| **URL** | `https://api.coingecko.com/api/v3/simple/price` |
| **Uso** | Datos de mercado cripto para usuarios del segmento inversor |
| **Estado** | Implementado en el notebook de prueba, pendiente de integrar en Silver |

**Probar todas las APIs:**
```bash
python notebooks/02_prueba_apis.py
```

---

## 8. Cómo Ejecutar el Proyecto

> **Nota:** en Windows ejecutar siempre con `python -X utf8` para que los emojis de los
> prints no fallen. Ejemplo: `python -X utf8 src/run_pipeline.py`

### Opción A — Pipeline integrado con el dataset real (RECOMENDADO)

Reproduce los 2.000 eventos del JSON a través del bus y dispara Silver + Gold automáticamente.
No requiere e-commerce ni servidores adicionales — todo corre en un solo proceso asyncio.

```bash
python src/bus/start_full_pipeline.py
```

Opciones útiles:
```bash
# Sin pausa entre eventos (más rápido)
python src/bus/start_full_pipeline.py --delay 0

# Solo Bronze, sin Silver/Gold automático
python src/bus/start_full_pipeline.py --no-trigger

# Loop infinito para stress test (Ctrl+C para detener)
python src/bus/start_full_pipeline.py --loop
```

### Opción B — Pipeline completo desde JSON estático (modo batch)

Lee directamente el JSON sin pasar por el bus. Útil para regenerar Gold rápidamente.

```bash
python src/run_pipeline.py
```

### Opción C — Solo Silver y Gold (Bronze ya existe)

```bash
python src/run_pipeline.py --desde-silver
```

### Opción D — Por capas (para depuración)

```bash
# Paso 1: Bronze
python src/bronze/pipeline_bronze.py

# Paso 2: Silver (requiere Bronze)
python src/silver/pipeline_silver.py

# Paso 3: Gold (requiere Silver)
python src/gold/pipeline_gold.py
```

### Opción E — Modo servidor HTTP (dos terminales)

Levanta el receptor (8000) y la API de ingesta (8001) de forma independiente.
Permite enviar mensajes de cualquier tipo mediante HTTP o el Swagger UI.

```bash
# Terminal 1: receptor — procesa el bus y escribe a Bronze
uvicorn src.bus.api_receiver:app --port 8000 --reload

# Terminal 2: API de ingesta — genera y envía mensajes sintéticos
uvicorn src.bus.ecommerce_api:app --port 8001 --reload

# Desde una tercera terminal (o Swagger en http://localhost:8001/docs):
curl -X POST "http://localhost:8001/simulate?msg_type=event&n=200&tps=5"

# Forzar Silver + Gold después de recibir suficientes eventos:
curl -X POST http://localhost:8000/pipeline/run
```

### Verificar que todo está correcto

```bash
python verificar_pipeline_completo.py
```

Resultado esperado:
```
✅ Bronze: 2.000+ registros con metadatos de ingesta
✅ Silver: 28 columnas — is_failed, amount_usd, geo_source...
✅ Gold user_360: 489 usuarios con 21 columnas
✅ Gold daily_metrics y event_summary presentes
✅ TODAS LAS CAPAS VERIFICADAS — Listo para Fase 3
```

---

## 9. Estado del Proyecto por Fases

### ✅ Fase 1 — Análisis y Diseño (COMPLETADA)

| Tarea | Entregable | Estado |
|---|---|---|
| 1.1 Explorar JSON | Tipos de evento, campos, nulos, duplicados | ✅ |
| 1.2 Esquema Bronze→Silver | Tabla de mapeo de 35 campos (Manual 3) | ✅ |
| 1.3 Métricas Gold | Diccionario formal — 21 columnas en `gold_user_360` | ✅ |
| 1.4 Prueba de APIs | `notebooks/02_prueba_apis.py` | ✅ |
| 1.5 Preguntas del agente | 10 preguntas en lenguaje natural para Fase 3 | ✅ |

### ✅ Fase 2 — Pipeline Bronze → Silver → Gold (COMPLETADA)

**Núcleo del pipeline:**

| Componente | Archivo | Estado |
|---|---|---|
| Capa Bronze | `src/bronze/pipeline_bronze.py` | ✅ Funcional |
| Capa Silver | `src/silver/pipeline_silver.py` | ✅ Funcional |
| Capa Gold | `src/gold/pipeline_gold.py` | ✅ Funcional |
| Script maestro | `src/run_pipeline.py` | ✅ Funcional |
| Verificación | `verificar_pipeline_completo.py` | ✅ Funcional |

**Bus de eventos e ingesta:**

| Componente | Archivo | Estado |
|---|---|---|
| Envelope estándar multi-tipo | `src/bus/message_schema.py` | ✅ Funcional |
| Bus asyncio + BronzeConsumer | `src/bus/event_bus_asyncio.py` | ✅ Funcional |
| Reproductor del dataset real | `src/bus/dataset_producer.py` | ✅ Funcional |
| Trigger automático Silver/Gold | `src/bus/pipeline_trigger.py` | ✅ Funcional |
| Receptor HTTP FastAPI :8000 | `src/bus/api_receiver.py` | ✅ Funcional |
| API de ingesta genérica :8001 | `src/bus/ecommerce_api.py` | ✅ Funcional |
| Lanzador integrado standalone | `src/bus/start_full_pipeline.py` | ✅ Funcional |
| Integración con e-commerce real | `POST /ingest` vía FastAPI | ⏳ Pendiente e-commerce |

### ⏳ Fase 3 — Agente Inteligente (PRÓXIMA)

| Componente | Descripción | Estado |
|---|---|---|
| Text-to-SQL | Claude AI convierte preguntas en SQL sobre Gold | 🔜 |
| Motor de consulta | DuckDB ejecuta SQL sobre `gold_user_360.parquet` | 🔜 |
| Dashboard | Streamlit con 3 páginas: Overview, Visión 360, Agente | 🔜 |

**Prerrequisito para Fase 3 — verificar antes de avanzar:**
```bash
python src/bus/start_full_pipeline.py   # Pipeline integrado completo
python verificar_pipeline_completo.py   # Confirmar sin errores
```

---

## 10. Decisiones de Diseño

### ¿Por qué asyncio en lugar de Kafka o PyPubSub?

El equipo evaluó tres alternativas para el bus de eventos:

- **Kafka**: Estándar de la industria para millones de eventos/día. Requiere instalación
  de servidor separado y conocimiento de administración de brokers. Correcto para producción
  a escala, pero exceso de complejidad para este reto.

- **PyPubSub**: Simple y legible. Sin embargo, es síncrono por naturaleza, lo que significa
  que el productor espera a que todos los suscriptores terminen antes de continuar. No es
  adecuado para I/O concurrente con disco (escritura Parquet).

- **asyncio.Queue** ✅: Nativo de Python 3.10+, sin instalación extra. Permite que productor
  y consumidor corran verdaderamente en paralelo dentro del mismo proceso. Soporta backpressure
  automático y micro-batch, el patrón exacto que recomienda Databricks para Bronze streaming.

### ¿Por qué Parquet en lugar de CSV?

| Métrica | CSV | Parquet (Snappy) |
|---|---|---|
| Tamaño (2.000 registros) | ~800 KB | ~180 KB (78% menos) |
| Velocidad de lectura | Lenta (parseo texto) | Rápida (columnar binario) |
| Tipos de datos | Todo string | Tipos nativos (int, float, datetime) |
| Particionado | Manual | Nativo por carpetas `date=YYYY-MM-DD` |
| Estándar en la industria | No | Sí (Databricks, Spark, BigQuery) |

### ¿Por qué Bronze no elimina duplicados?

Bronze es inmutable por diseño (principio de auditoría). Si se elimina un duplicado y luego
se descubre que fue un error del sistema fuente (no un duplicado real), no hay forma de
recuperarlo. Bronze los marca con `is_duplicate = True` y los registra en `logs/`.
Silver es la capa responsable de la deduplicación definitiva.

### ¿Por qué separar `amount_cop` de `amount_usd`?

Mantener ambas columnas permite análisis en la moneda local (COP para reguladores colombianos)
y en moneda de referencia (USD para comparaciones internacionales) sin recalcular. La tasa
de conversión se registra implícitamente en el valor de `amount_usd`, lo que permite auditar
qué tasa se usó en cada lote.

---

## 11. Riesgos Identificados y Mitigaciones

| Riesgo | Probabilidad | Impacto | Mitigación Implementada |
|---|---|---|---|
| IPs privadas → ipapi no funciona | **Alta** (ya ocurrió: 100% privadas) | Media | Fallback automático a `payload.city` |
| ExchangeRate API no disponible | Media | Baja | Tasa de respaldo `1 USD = 4.150 COP` codificada |
| E-commerce envía esquema diferente | Media | Alta | Bronze acepta cualquier JSON; Silver valida con `errors='coerce'` |
| Cola asyncio llena (backpressure) | Baja | Media | `maxsize=1.000`; el productor espera automáticamente |
| Parquet corrupto en Bronze | Muy baja | Alta | Cada lote es un archivo independiente; fallo aísla solo ese lote |
| Duplicados en eventos | Baja (0 en dataset base) | Media | Detectados y registrados en Bronze; eliminados en Silver |

---

## 12. Próximos Pasos — Fase 3

La Fase 3 construye el **Agente Inteligente** sobre la capa Gold.

### Las 10 preguntas que el agente debe responder

| # | Pregunta | Complejidad |
|---|---|---|
| 1 | ¿Cuántos usuarios únicos hay en la plataforma? | Baja |
| 2 | ¿Cuál es el usuario con mayor gasto total en COP? | Baja |
| 3 | ¿Cuánto ha gastado el usuario `user_99` en total? | Baja |
| 4 | ¿Cuáles son los 5 merchants más populares? | Media |
| 5 | ¿Qué segmento tiene el mayor ticket promedio? | Media |
| 6 | ¿Cuántos usuarios tienen más de 3 transacciones fallidas? | Media |
| 7 | ¿Cuál es el canal preferido de los usuarios premium? | Media |
| 8 | ¿Qué ciudad tiene el mayor volumen de transacciones en COP? | Media-Alta |
| 9 | ¿Cuáles son los 3 usuarios más inactivos? | Alta |
| 10 | Dame un resumen financiero completo del usuario `user_210` | Alta |

### Stack de la Fase 3

```python
# Agente: pregunta → SQL → resultado → respuesta natural
import anthropic
import duckdb

conn = duckdb.connect()
conn.register("gold_user_360", pd.read_parquet("data/gold/gold_user_360.parquet"))

def agente(pregunta: str) -> str:
    sql = generar_sql_con_claude(pregunta)   # Claude genera el SQL
    resultado = conn.execute(sql).df()        # DuckDB lo ejecuta
    respuesta = generar_respuesta(resultado)  # Claude redacta la respuesta
    return respuesta
```

---

## 13. Dependencias

```txt
# ── Pipeline de datos ──────────────────────────────────────────────────────────
pandas>=2.0.0
pyarrow>=14.0.0
numpy>=1.26.0

# ── Bus de eventos y API HTTP ──────────────────────────────────────────────────
fastapi>=0.110.0
uvicorn[standard]>=0.27.0
pydantic>=2.0.0

# ── Fuentes de datos y utilidades ─────────────────────────────────────────────
requests>=2.31.0
Faker>=19.0.0
python-dotenv>=1.0.0

# ── Fase 3 — Agente Inteligente (descomentar cuando se inicie) ────────────────
# anthropic>=0.25.0
# duckdb>=0.10.0
# streamlit>=1.32.0
```

**Instalar:**
```bash
pip install -r requirements.txt
```

**Regenerar `requirements.txt` después de instalar nuevos paquetes:**
```bash
# Solo dependencias directas (recomendado — legible y mantenible)
pip install pipreqs
pipreqs . --force

# Alternativa — congela TODO lo instalado en el venv (más exhaustivo)
pip freeze > requirements.txt
```

---

## Contacto y Contribución

Este proyecto fue desarrollado como parte del **Reto 1 — Plataforma de Datos Fintech en
Tiempo Real**, enmarcado en los desafíos de arquitectura de datos modernos para fintechs,
bancos digitales y plataformas de lealtad colombianas.

**Referencia arquitectónica:** [Databricks Medallion Architecture](https://www.databricks.com/blog/what-is-medallion-architecture)

---

*Última actualización: Abril 2026 — Fases 1 y 2 completadas — Fase 3 próxima*



instalacion de ollama

pip install strands-agents duckdb matplotlib seaborn pyarrow python-dotenv pandas fastapi uvicorn faker requests streamlit