import os
import io
import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
from src.db import get_source_engine

load_dotenv()

BUCKET_NAME = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

TABLE_FILE_MAPPING = {
    "customers/customers.csv": "customers",
    "products/products.csv": "products",
    "orders/orders.csv": "orders",
    "order_items/order_items.csv": "order_items",
    "payments/payments.csv": "payments",
    "shipments/shipments.csv": "shipments",
}


def get_s3_etag(bucket, key):
    response = s3.head_object(Bucket=bucket, Key=key)
    return response["ETag"].replace('"', ''), response["LastModified"]


def already_processed(engine, bucket, key, etag):
    query = text("""
        SELECT 1
        FROM etl.s3_file_tracker
        WHERE bucket_name = :bucket
          AND object_key = :key
          AND etag = :etag
          AND load_status = 'SUCCESS'
        LIMIT 1
    """)
    with engine.connect() as conn:
        result = conn.execute(
            query,
            {"bucket": bucket, "key": key, "etag": etag}
        ).fetchone()
        return result is not None


def mark_processed(engine, bucket, key, etag, last_modified, status):
    query = text("""
        INSERT INTO etl.s3_file_tracker (bucket_name, object_key, etag, last_modified, load_status)
        VALUES (:bucket, :key, :etag, :last_modified, :status)
        ON CONFLICT (bucket_name, object_key, etag)
        DO UPDATE SET
            load_status = EXCLUDED.load_status,
            processed_at = CURRENT_TIMESTAMP
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "bucket": bucket,
            "key": key,
            "etag": etag,
            "last_modified": last_modified,
            "status": status
        })


def load_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read()
    return pd.read_csv(io.BytesIO(content))


def ingest_s3_to_source():
    engine = get_source_engine()

    for s3_key, table_name in TABLE_FILE_MAPPING.items():
        etag = None
        last_modified = None

        try:
            etag, last_modified = get_s3_etag(BUCKET_NAME, s3_key)

            if already_processed(engine, BUCKET_NAME, s3_key, etag):
                print(f"Skipped already processed file: {s3_key}")
                continue

            df = load_csv_from_s3(BUCKET_NAME, s3_key)

            print(f"Loading {s3_key} into source table {table_name}")

            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))

            df.to_sql(table_name, engine, if_exists="append", index=False)

            mark_processed(engine, BUCKET_NAME, s3_key, etag, last_modified, "SUCCESS")
            print(f"Loaded successfully: {s3_key}")

        except Exception as e:
            mark_processed(engine, BUCKET_NAME, s3_key, etag, last_modified, "FAILED")
            print(f"Error processing {s3_key}: {e}")
            raise