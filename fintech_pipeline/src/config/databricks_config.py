import os
from dotenv import load_dotenv

load_dotenv()  # funciona local

# DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
# DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST = os.getenv("https://dbc-cd89db62-9f56.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("")

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    raise ValueError("❌ Faltan variables de entorno de Databricks")