import os
import io
import csv
import json
import logging
import tempfile
from typing import List, Dict
from dotenv import load_dotenv

import pandas as pd
import boto3
from botocore.client import Config
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import Integer, Float, String, Date, Boolean, Text
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from jsonschema import Draft7Validator

# load env
load_dotenv()

# ---------- Config ----------
# CHANGED: Replaced 'localhost' with 'minio' (the container name from docker-compose)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() in ("true", "1")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-data")

# CHANGED: Replaced 'localhost' with 'pg_etl' (the container name from docker-compose)
POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql+psycopg2://etl_user:etl_pass@pg_etl:5432/analytics_db"
)

# psycopg2 connection parameters (parsed)
TARGET_TABLE = os.getenv("TARGET_TABLE", "orders_clean")
UNIQUE_KEY = os.getenv("UNIQUE_KEY", "order_id")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "schemas/orders_schema.json")

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl")

# ---------- MinIO helpers ----------
def get_minio_client():
    # Ensure http/https prefix is correct
    endpoint_url = f"http{'s' if MINIO_SECURE else ''}://{MINIO_ENDPOINT}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def download_object_as_df(key: str) -> pd.DataFrame:
    s3 = get_minio_client()
    resp = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
    content = resp["Body"].read()
    if key.lower().endswith(".csv"):
        return pd.read_csv(io.BytesIO(content))
    else:
        return pd.read_json(io.BytesIO(content), orient="records")

def upload_sample_to_minio(local_path: str, object_key: str = None):
    if object_key is None:
        object_key = os.path.basename(local_path)
    s3 = get_minio_client()
    try:
        s3.create_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        pass
    with open(local_path, "rb") as f:
        s3.put_object(Bucket=MINIO_BUCKET, Key=object_key, Body=f)
    logger.info("Uploaded %s -> s3://%s/%s", local_path, MINIO_BUCKET, object_key)

# ---------- JSON Schema validation ----------
def load_json_schema(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Schema file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def validate_row_with_schema(row: Dict, schema: Dict) -> List[str]:
    validator = Draft7Validator(schema)
    errors = []
    for err in validator.iter_errors(row):
        errors.append(f"{err.message} (path: {'/'.join(map(str, err.path))})")
    return errors

def validate_dataframe_schema(df: pd.DataFrame, schema_path: str = SCHEMA_PATH, sample_limit: int = 1000) -> List[dict]:
    schema = load_json_schema(schema_path)
    msgs = []
    for i, row in enumerate(df.to_dict(orient="records")):
        if i >= sample_limit:
            break
        errors = validate_row_with_schema(row, schema)
        if errors:
            msgs.append({"row_index": i, "errors": errors, "row": row})
    return msgs

# ---------- Transform ----------
def transform_orders_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: c.strip().lower())
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        def to_iso_date(val):
            if pd.isna(val):
                return None
            try:
                return val.date().isoformat()
            except Exception:
                return str(val)
        df["order_date"] = df["order_date"].apply(to_iso_date)
    if {"quantity", "price"}.issubset(df.columns):
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
        df["total_price"] = df["quantity"] * df["price"]
    if UNIQUE_KEY in df.columns:
        df = df.dropna(subset=[UNIQUE_KEY])
    df = df.reset_index(drop=True)
    return df

# ---------- DQ ----------
REQUIRED_COLUMNS = ["order_id", "user_id", "product_id", "quantity", "price", "order_date"]
def basic_data_quality_checks(df: pd.DataFrame, required_columns: List[str] = REQUIRED_COLUMNS) -> List[str]:
    msgs = []
    miss = [c for c in required_columns if c not in df.columns]
    if miss:
        msgs.append(f"Missing required columns: {miss}")
    if len(df) == 0:
        msgs.append("No rows in dataset after download/transform.")
    if UNIQUE_KEY in df.columns and df[UNIQUE_KEY].duplicated().any():
        msgs.append("Duplicate values found in UNIQUE_KEY column.")
    return msgs

# ---------- Helpers for SQL type mapping ----------
def pandas_dtype_to_sqlalchemy(col_name: str, series: pd.Series):
    if pd.api.types.is_integer_dtype(series):
        return Column(col_name, Integer)
    if pd.api.types.is_float_dtype(series):
        return Column(col_name, Float)
    if pd.api.types.is_bool_dtype(series):
        return Column(col_name, Boolean)
    if pd.api.types.is_datetime64_any_dtype(series):
        return Column(col_name, Date)
    # default to text/string
    return Column(col_name, Text)

# ---------- DB load using COPY and upsert ----------
def get_sqlalchemy_engine():
    return create_engine(POSTGRES_URI)

def create_table_if_not_exists(engine, table_name: str, df: pd.DataFrame):
    metadata = MetaData()
    cols = []
    for col in df.columns:
        cols.append(pandas_dtype_to_sqlalchemy(col, df[col]))
    table = Table(table_name, metadata, *cols, extend_existing=True)
    metadata.bind = engine
    metadata.create_all(engine, tables=[table])
    return table

def create_staging_table(engine, staging_name: str, df: pd.DataFrame):
    # Drop if exists then create
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{staging_name}";'))
    metadata = MetaData()
    cols = []
    for col in df.columns:
        cols.append(pandas_dtype_to_sqlalchemy(col, df[col]))
    staging = Table(staging_name, metadata, *cols)
    metadata.bind = engine
    staging.create(engine)
    return staging

def copy_df_to_table_via_copy(engine, df: pd.DataFrame, table_name: str):
    """
    Use psycopg2 COPY FROM STDIN for fast insert. Convert df to CSV in-memory.
    """
    # build CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=True, quoting=csv.QUOTE_MINIMAL)
    csv_buffer.seek(0)

    # create a raw DBAPI connection from SQLAlchemy engine
    raw_conn = engine.raw_connection()
    cursor = None
    try:
        cursor = raw_conn.cursor()
        sql = f'COPY "{table_name}" FROM STDIN WITH (FORMAT csv, HEADER true)'
        cursor.copy_expert(sql, csv_buffer)
        raw_conn.commit()
    except Exception as e:
        try:
            raw_conn.rollback()
        except Exception:
            pass
        logger.exception("COPY failed: %s", e)
        raise
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        try:
            raw_conn.close()
        except Exception:
            pass

def attempt_cast_date_columns(engine, staging_table: str, df: pd.DataFrame):
    """
    For any column name containing 'date' (case-insensitive), attempt to alter the staging column
    to type DATE using USING column::date.
    """
    date_cols = [c for c in df.columns if "date" in c.lower()]
    if not date_cols:
        return
    with engine.connect() as conn:
        for c in date_cols:
            try:
                logger.info("Attempting to cast staging column %s to DATE", c)
                conn.execute(text(f'ALTER TABLE "{staging_table}" ALTER COLUMN "{c}" TYPE date USING ("{c}"::date);'))
            except Exception as e:
                # fetch few values to help debug
                res = conn.execute(text(f'SELECT "{c}" FROM "{staging_table}" LIMIT 10')).fetchall()
                sample_vals = [r[0] for r in res]
                logger.error("Failed to cast staging column %s to date. Sample values: %s", c, sample_vals)
                raise RuntimeError(
                    f'Could not cast staging column "{c}" to DATE. '
                    f'Check values and format (expected YYYY-MM-DD). Error: {e}. Sample values: {sample_vals}'
                ) from e

def load_df_to_postgres_upsert(df: pd.DataFrame, table_name: str = TARGET_TABLE, unique_key: str = UNIQUE_KEY):
    """
    Robust upsert:
    1. Ensure target table exists (create if needed).
    2. Create staging table with same columns.
    3. Bulk insert into staging via COPY.
    4. Attempt to cast date-like staging columns to DATE (so types match target).
    5. INSERT INTO target SELECT FROM staging ON CONFLICT DO UPDATE...
    6. Drop staging.
    """
    engine = get_sqlalchemy_engine()
    inspector = sqlalchemy.inspect(engine)
    try:
        # 1) ensure target table exists (create with inferred types if missing)
        if not inspector.has_table(table_name):
            logger.info("Target table %s missing — creating.", table_name)
            # create using df.head(0) to create column structure (types inferred)
            create_table_if_not_exists(engine, table_name, df.head(0))

        # 2) create staging
        staging_table = f"{table_name}_staging"
        logger.info("Creating staging table %s", staging_table)
        create_staging_table(engine, staging_table, df)

        # 3) bulk load via COPY
        logger.info("Bulk copying %d rows into staging %s", len(df), staging_table)
        copy_df_to_table_via_copy(engine, df, staging_table)

        # 4) try to cast date-like columns in staging to DATE so types align for INSERT
        attempt_cast_date_columns(engine, staging_table, df)

        # 5) upsert
        columns = list(df.columns)
        cols_sql = ", ".join(f'"{c}"' for c in columns)
        updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in columns if c != unique_key)

        upsert_sql = f'''
        INSERT INTO "{table_name}" ({cols_sql})
        SELECT {cols_sql} FROM "{staging_table}"
        ON CONFLICT ("{unique_key}") DO UPDATE
        SET {updates};
        '''
        with engine.begin() as conn:
            conn.execute(text(upsert_sql))
            conn.execute(text(f'DROP TABLE IF EXISTS "{staging_table}";'))

        logger.info("Upsert complete: %d rows upserted to %s", len(df), table_name)

    except Exception as e:
        logger.exception("Error during upsert: %s", e)
        raise

# ---------- Orchestrator ----------
def run_etl_from_minio_to_postgres(object_key: str, perform_upsert: bool = True, schema_check: bool = True):
    logger.info("Starting ETL for s3://%s/%s", MINIO_BUCKET, object_key)
    # extract
    df_raw = download_object_as_df(object_key)
    logger.info("Downloaded %d raw rows", len(df_raw))

    # transform
    df_clean = transform_orders_df(df_raw)
    logger.info("Transformed to %d rows", len(df_clean))

    # DQ
    dq = basic_data_quality_checks(df_clean)
    if dq:
        logger.error("Data quality issues: %s", dq)
        raise ValueError("Data quality checks failed.")

    # schema validation
    if schema_check:
        schema_errors = validate_dataframe_schema(df_clean)
        if schema_errors:
            logger.error("Schema validation failed for %d rows (showing up to 5):", len(schema_errors))
            for err in schema_errors[:5]:
                logger.error("Row %s errors: %s", err["row_index"], err["errors"])
            raise ValueError("Schema validation failed.")
        logger.info("Schema validation passed (sampled).")

    # load
    if perform_upsert:
        load_df_to_postgres_upsert(df_clean)
    else:
        # append mode: use COPY into existing table
        engine = get_sqlalchemy_engine()
        copy_df_to_table_via_copy(engine, df_clean, TARGET_TABLE)
        logger.info("Appended %d rows to %s", len(df_clean), TARGET_TABLE)

    logger.info("ETL finished successfully.")