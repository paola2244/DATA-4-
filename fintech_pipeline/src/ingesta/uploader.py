import os
import subprocess


def subir_parquets(local_path: str, dbfs_path: str):
    """
    Sube archivos parquet a Databricks DBFS manteniendo estructura.

    Args:
        local_path: ruta local (ej: data/silver)
        dbfs_path: ruta destino (ej: dbfs:/FileStore/fintech_pipeline/silver)
    """

    if not os.path.exists(local_path):
        print(f"❌ Ruta no existe: {local_path}")
        return

    print(f"\n☁️ Subiendo desde {local_path} → {dbfs_path}\n")

    for root, dirs, files in os.walk(local_path):
        for file in files:
            if file.endswith(".parquet"):
                local_file = os.path.join(root, file)

                # mantener estructura relativa
                relative_path = os.path.relpath(local_file, local_path)
                dbfs_file = f"{dbfs_path}/{relative_path}"

                print(f"⬆️ {local_file} → {dbfs_file}")

                try:
                    subprocess.run([
                        "databricks", "fs", "cp",
                        local_file,
                        dbfs_file,
                        "--overwrite"
                    ], check=True)

                except subprocess.CalledProcessError as e:
                    print(f"❌ Error subiendo {local_file}: {e}")

    print("\n✅ Subida completada\n")