import os
import requests

DATABRICKS_HOST = ""
DATABRICKS_TOKEN = ""

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}"
}


def subir_archivo_dbfs(local_path, dbfs_path):
    """
    Sube un archivo a DBFS usando API REST
    """
    url = f"{DATABRICKS_HOST}/api/2.0/dbfs/put"

    with open(local_path, "rb") as f:
        files = {
            "file": f
        }
        data = {
            "path": dbfs_path,
            "overwrite": "true"
        }

        response = requests.post(url, headers=HEADERS, data=data, files=files)

    if response.status_code == 200:
        print(f"✅ Subido: {dbfs_path}")
    else:
        print(f"❌ Error {response.status_code}: {response.text}")


def subir_parquets(local_folder, dbfs_base_path):
    """
    Recorre carpeta y sube todos los parquet
    """
    for root, _, files in os.walk(local_folder):
        for file in files:
            if file.endswith(".parquet"):
                local_file = os.path.join(root, file)

                relative_path = os.path.relpath(local_file, local_folder)
                dbfs_file = f"{dbfs_base_path}/{relative_path}".replace("\\", "/")

                print(f"⬆️ {local_file} → {dbfs_file}")
                subir_archivo_dbfs(local_file, dbfs_file)
