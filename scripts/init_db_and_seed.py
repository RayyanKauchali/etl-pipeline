# scripts/init_db_and_seed.py
import os
import time
import io
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import text
import boto3
from botocore.client import Config

load_dotenv()

# ---------- Config (from .env or defaults) ----------
POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql+psycopg2://etl_user:etl_pass@localhost:5432/analytics_db",
)
# SQL file to create target table
CREATE_TABLE_SQL = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sql", "create_target_table.sql")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")  # host:port or host
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-data")
SAMPLE_LOCAL_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sample_data", "orders_sample.csv")
SAMPLE_OBJECT_KEY = "sample/orders_sample.csv"

# ---------- Helpers ----------
def wait_for_postgres(uri: str, timeout_s: int = 60):
    print("⏳ Waiting for Postgres to be available...")
    engine = None
    start = time.time()
    while True:
        try:
            engine = sqlalchemy.create_engine(uri)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Postgres is available.")
            return
        except Exception as e:
            if time.time() - start > timeout_s:
                raise RuntimeError(f"Postgres not reachable after {timeout_s}s: {e}")
            time.sleep(1)

def create_table_from_sql(uri: str, sql_path: str):
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    engine = sqlalchemy.create_engine(uri)
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("✅ Executed table DDL.")

def get_minio_client(endpoint: str, access_key: str, secret_key: str, secure: bool = False):
    endpoint_url = f"http{'s' if secure else ''}://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def upload_file_to_minio(local_path: str, bucket: str, object_key: str, endpoint: str, access_key: str, secret_key: str):
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Sample file not found: {local_path}")
    s3 = get_minio_client(endpoint, access_key, secret_key)
    # create bucket if missing (MinIO will error if bucket exists; ignore)
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception:
        pass
    with open(local_path, "rb") as f:
        s3.put_object(Bucket=bucket, Key=object_key, Body=f)
    print(f"✅ Uploaded {local_path} -> s3://{bucket}/{object_key}")

# ---------- Main ----------
def main():
    print("Starting initialization...")
    # 1) Wait for Postgres
    wait_for_postgres(POSTGRES_URI, timeout_s=60)

    # 2) Create target table from SQL
    try:
        create_table_from_sql(POSTGRES_URI, CREATE_TABLE_SQL)
    except Exception as e:
        print("Error creating table:", e)
        raise

    # 3) Upload sample CSV to MinIO
    try:
        upload_file_to_minio(
            SAMPLE_LOCAL_PATH,
            MINIO_BUCKET,
            SAMPLE_OBJECT_KEY,
            MINIO_ENDPOINT,
            MINIO_ACCESS_KEY,
            MINIO_SECRET_KEY,
        )
    except Exception as e:
        print("Error uploading sample to MinIO:", e)
        raise

    print("Initialization finished successfully!")

if __name__ == "__main__":
    main()
