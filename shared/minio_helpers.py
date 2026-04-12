"""
Shared MinIO, CSV and Parquet helpers used across all pipeline stages.
"""

import os
import io
import csv

from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq

# =============================================================================
#  MinIO config (from env)
# =============================================================================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "landing-zone")

METADATA_KEY = "metadata/observations.csv"
PARQUET_KEY = "metadata/observations.parquet"

LANDING_METADATA_FIELDS = [
    "uuid", "source_id", "time_recorded/added", "duration",
    "audio_size", "audio_path", "audio_format", "source", "peak_frequency_hz",
]


def create_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client, bucket, placeholders):
    """Create bucket if missing and touch placeholder keys."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"  Created bucket: {bucket}")
    for key in placeholders:
        try:
            client.stat_object(bucket, key)
        except Exception:
            client.put_object(bucket, key, io.BytesIO(b""), 0)
    print("  Bucket and folder structure ready.")


# =============================================================================
#  Parquet helpers
# =============================================================================
def _rows_to_table(rows):
    if not rows:
        return None
    all_keys = list(dict.fromkeys(k for row in rows for k in row))
    arrays = {key: [str(row.get(key, "")) for row in rows] for key in all_keys}
    return pa.table(arrays)


def read_parquet_from_minio(client, bucket, key=PARQUET_KEY):
    try:
        resp = client.get_object(bucket, key)
        data = resp.read()
        resp.close(); resp.release_conn()
        return pq.read_table(io.BytesIO(data))
    except Exception:
        return None


def _write_parquet_to_minio(client, bucket, table, key=PARQUET_KEY):
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    client.put_object(bucket, key, buf, len(buf.getvalue()), "application/octet-stream")


def _unify_schemas(existing_table, new_table):
    if existing_table is None:
        return new_table
    if new_table is None:
        return existing_table
    all_names = list(dict.fromkeys(
        list(existing_table.schema.names) + list(new_table.schema.names)
    ))
    def pad_table(tbl, target_names):
        for name in target_names:
            if name not in tbl.schema.names:
                tbl = tbl.append_column(name, pa.array([""] * tbl.num_rows, type=pa.string()))
        return tbl.select(target_names)
    return pa.concat_tables([pad_table(existing_table, all_names), pad_table(new_table, all_names)])


def update_parquet(client, bucket, new_rows, key=PARQUET_KEY):
    new_table = _rows_to_table(new_rows)
    if new_table is None:
        return read_parquet_from_minio(client, bucket, key)
    existing = read_parquet_from_minio(client, bucket, key)
    combined = _unify_schemas(existing, new_table)
    _write_parquet_to_minio(client, bucket, combined, key)
    print(f"  Updated: {key}  ({combined.num_rows} total rows)")
    return combined


# =============================================================================
#  CSV helpers
# =============================================================================
def merge_metadata_fieldnames(existing_names, row_keys, priority_fields=None):
    """Build a unified fieldnames list preserving order.

    *priority_fields* are inserted before any new keys from *row_keys*.
    Falls back to LANDING_METADATA_FIELDS when *priority_fields* is None.
    """
    if priority_fields is None:
        priority_fields = LANDING_METADATA_FIELDS
    out = list(existing_names or [])
    seen = set(out)
    for k in priority_fields:
        if k not in seen:
            seen.add(k)
            out.append(k)
    for k in row_keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def read_csv_from_minio(client, bucket, key=METADATA_KEY):
    """Return raw CSV text from MinIO, or None if not found."""
    try:
        resp = client.get_object(bucket, key)
        data = resp.read().decode("utf-8")
        resp.close(); resp.release_conn()
        return data
    except Exception:
        return None


def append_rows_to_csv(client, bucket, new_rows, priority_fields=None,
                       key=METADATA_KEY):
    """Read existing CSV from MinIO, append *new_rows*, write back."""
    existing = read_csv_from_minio(client, bucket, key)

    if existing:
        reader = csv.DictReader(io.StringIO(existing))
        fieldnames = merge_metadata_fieldnames(
            reader.fieldnames, new_rows[0].keys(), priority_fields,
        )
        rows_list = list(reader)
    else:
        fieldnames = merge_metadata_fieldnames(
            [], new_rows[0].keys(), priority_fields,
        )
        rows_list = []

    for prev in rows_list:
        for fn in fieldnames:
            if fn not in prev:
                prev[fn] = ""

    rows_list.extend(new_rows)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_list)
    csv_bytes = buf.getvalue().encode("utf-8")
    client.put_object(bucket, key, io.BytesIO(csv_bytes), len(csv_bytes), "text/csv")


def load_existing_source_ids(client, bucket=None, key=METADATA_KEY):
    """Return set of source_id strings already stored in metadata CSV."""
    bucket = bucket or MINIO_BUCKET
    try:
        resp = client.get_object(bucket, key)
        data = resp.read().decode("utf-8")
        resp.close(); resp.release_conn()
    except Exception:
        return set()
    reader = csv.DictReader(io.StringIO(data))
    return {row.get("source_id", "").strip() for row in reader
            if row.get("source_id", "").strip()}
