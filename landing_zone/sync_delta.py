#!/usr/bin/env python3
"""
Sync metadata Parquet → Delta Lake.

Reads ``metadata/observations.parquet`` from the MinIO landing-zone bucket
and overwrites the Delta Lake table at
``s3a://<MINIO_BUCKET>/metadata/observations_delta/``
so the table matches the Parquet file exactly.

Run this whenever you want to update the Delta Lake table after ingestion.

Requires:
  - env vars: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
  - pip install pyarrow deltalake python-dotenv minio

Run:
    python sync_delta.py
"""

import io
import os

from dotenv import load_dotenv
load_dotenv()

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake
from minio import Minio

# =============================================================================
#  Config
# =============================================================================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "landing-zone")

PARQUET_KEY = "metadata/observations.parquet"
DELTA_TABLE_PATH = "metadata/observations_delta"


# =============================================================================
#  Helpers
# =============================================================================
def _s3_storage_options():
    scheme = "https" if MINIO_SECURE else "http"
    return {
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_ENDPOINT_URL": f"{scheme}://{MINIO_ENDPOINT}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true" if not MINIO_SECURE else "false",
    }


def read_parquet_from_minio(client):
    """Download and read the Parquet metadata file from MinIO."""
    try:
        resp = client.get_object(MINIO_BUCKET, PARQUET_KEY)
        data = resp.read()
        resp.close()
        resp.release_conn()
        return pq.read_table(io.BytesIO(data))
    except Exception as e:
        print(f"  Failed to read {PARQUET_KEY}: {e}")
        return None


def sync_to_delta(table):
    """Overwrite the Delta Lake table with the given PyArrow Table."""
    storage_options = _s3_storage_options()
    delta_uri = f"s3a://{MINIO_BUCKET}/{DELTA_TABLE_PATH}"
    write_deltalake(
        delta_uri,
        table,
        mode="overwrite",
        storage_options=storage_options,
    )
    return delta_uri


# =============================================================================
#  Main
# =============================================================================
def run():
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError(
            "MinIO credentials missing. "
            "Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY in your .env file."
        )

    print(f"\n  Connecting to MinIO at {MINIO_ENDPOINT}...")
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    print(f"  Reading {PARQUET_KEY} from bucket '{MINIO_BUCKET}'...")
    table = read_parquet_from_minio(client)

    if table is None or table.num_rows == 0:
        print("  No Parquet data found — nothing to sync.")
        return

    print(f"  Parquet contains {table.num_rows} rows, {table.num_columns} columns.")
    print(f"  Syncing to Delta Lake...")

    try:
        delta_uri = sync_to_delta(table)
        print(f"\n  Delta Lake synced successfully.")
        print(f"   Table: {delta_uri}")
        print(f"   Rows:  {table.num_rows}\n")
    except Exception as e:
        print(f"\n  Delta Lake sync failed: {e}\n")
        raise


if __name__ == "__main__":
    run()
