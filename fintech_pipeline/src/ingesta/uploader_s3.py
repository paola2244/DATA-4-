import os
import boto3
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

BUCKET = os.getenv("AWS_BUCKET")


def subir_parquets(local_folder, capa):
    """
    capa = 'silver' o 'gold'
    """

    if not os.path.exists(local_folder):
        print(f"❌ No existe: {local_folder}")
        return

    print(f"\n☁️ Subiendo {capa} → s3://{BUCKET}/{capa}/\n")

    for root, _, files in os.walk(local_folder):
        for file in files:
            if file.endswith(".parquet"):
                local_file = os.path.join(root, file)

                s3_key = f"{capa}/{file}"

                print(f"⬆️ {local_file} → s3://{BUCKET}/{s3_key}")

                try:
                    s3.upload_file(local_file, BUCKET, s3_key)
                    print("✅ OK")
                except Exception as e:
                    print(f"❌ Error: {e}")