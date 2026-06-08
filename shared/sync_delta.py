"""
Sync zone metadata Parquet → Delta Lake on MinIO.

Each zone bucket keeps:
  - metadata/observations.parquet  (tabular source of truth)
  - metadata/observations_delta/  (Delta table, overwritten to match Parquet)
"""

from __future__ import annotations

import os

import pyarrow as pa
from deltalake import write_deltalake

from shared.minio_helpers import (
    MINIO_ACCESS_KEY,
    MINIO_ANALYST_ACCESS_KEY,
    MINIO_ANALYST_SECRET_KEY,
    MINIO_ENDPOINT,
    MINIO_PIPELINE_ACCESS_KEY,
    MINIO_PIPELINE_SECRET_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    PARQUET_KEY,
    create_minio_client,
    read_parquet_from_minio,
)

OBSERVATIONS_DELTA_PATH = "metadata/observations_delta"

# ── Per-zone Delta Lake schemas ──────────────────────────────────────────────
# Columns not listed here stay as their inferred type (usually string).

_LANDING_FLOAT_COLS = {"duration", "peak_frequency_hz"}
_LANDING_INT_COLS = {"audio_size"}

_TRUSTED_FLOAT_COLS = {
    "duration",
    "symmetry_score",
    "pattern_stability_score",
    "peak_frequency_hz",
    "peak_time_s",
    "peak_amplitude",
    "peak_rms",
    "processing_time(seconds)",
}
_TRUSTED_INT_COLS = {"audio_size", "image_size", "video_size"}

_EXPLOITATION_FLOAT_COLS = _TRUSTED_FLOAT_COLS | {
    "spectral_centroid_hz",
    "spectral_bandwidth_hz",
    "spectral_rolloff_hz",
    "spectral_flatness",
    "signal_energy",
    "spectral_entropy",
    "zero_crossing_rate",
    "loudness",
    "harmonic_energy_ratio",
    "feature_processing_time(seconds)",
}
_EXPLOITATION_INT_COLS = _TRUSTED_INT_COLS

ZONE_SCHEMAS: dict[str, dict] = {
    "landing": {"float64": _LANDING_FLOAT_COLS, "int64": _LANDING_INT_COLS},
    "trusted": {"float64": _TRUSTED_FLOAT_COLS, "int64": _TRUSTED_INT_COLS},
    "exploitation": {"float64": _EXPLOITATION_FLOAT_COLS, "int64": _EXPLOITATION_INT_COLS},
}


def _cast_table_for_zone(table: pa.Table, zone_label: str) -> pa.Table:
    """Cast string columns to proper numeric types based on the zone schema."""
    schema_def = ZONE_SCHEMAS.get(zone_label)
    if not schema_def:
        return table
    for col_name in table.schema.names:
        col = table.column(col_name)
        if col_name in schema_def.get("float64", set()):
            target = pa.float64()
        elif col_name in schema_def.get("int64", set()):
            target = pa.int64()
        else:
            continue
        if col.type == target:
            continue
        try:
            idx = table.schema.get_field_index(col_name)
            table = table.set_column(idx, col_name, col.cast(target))
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            pass
    return table


def s3_storage_options() -> dict[str, str]:
    """Pipeline (RW) storage options — used by Delta Lake writes."""
    scheme = "https" if MINIO_SECURE else "http"
    return {
        "AWS_ACCESS_KEY_ID": MINIO_PIPELINE_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_PIPELINE_SECRET_KEY,
        "AWS_ENDPOINT_URL": f"{scheme}://{MINIO_ENDPOINT}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true" if not MINIO_SECURE else "false",
    }


def s3_storage_options_readonly() -> dict[str, str]:
    """Analyst (read-only) storage options — used by data consumption."""
    scheme = "https" if MINIO_SECURE else "http"
    return {
        "AWS_ACCESS_KEY_ID": MINIO_ANALYST_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_ANALYST_SECRET_KEY,
        "AWS_ENDPOINT_URL": f"{scheme}://{MINIO_ENDPOINT}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true" if not MINIO_SECURE else "false",
    }


def write_observations_delta(
    table,
    bucket: str,
    *,
    delta_path: str = OBSERVATIONS_DELTA_PATH,
) -> str:
    """Overwrite the zone Delta table from a PyArrow table."""
    delta_uri = f"s3a://{bucket}/{delta_path}"
    write_deltalake(
        delta_uri,
        table,
        mode="overwrite",
        storage_options=s3_storage_options(),
    )
    return delta_uri


def sync_observations_to_delta(
    client,
    bucket: str,
    *,
    parquet_key: str = PARQUET_KEY,
    delta_path: str = OBSERVATIONS_DELTA_PATH,
    zone_label: str | None = None,
) -> str | None:
    """
    Read observations Parquet from MinIO and overwrite the zone Delta table.

    Returns the Delta URI, or None when Parquet is missing/empty (sync skipped).
    """
    label = zone_label or bucket
    table = read_parquet_from_minio(client, bucket, key=parquet_key)
    if table is None or table.num_rows == 0:
        print(f"  [{label}] No Parquet at {parquet_key!r} — Delta sync skipped.")
        return None

    table = _cast_table_for_zone(table, label)
    print(
        f"  [{label}] Syncing Delta: {parquet_key} → {delta_path} "
        f"({table.num_rows} rows, {table.num_columns} columns)..."
    )
    delta_uri = write_observations_delta(table, bucket, delta_path=delta_path)
    print(f"  [{label}] Delta table ready: {delta_uri}")
    return delta_uri


def _require_credentials() -> None:
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError(
            "MinIO credentials missing. "
            "Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY in your .env file."
        )


def run_for_bucket(
    bucket: str,
    *,
    zone_label: str | None = None,
    client=None,
) -> str | None:
    """CLI helper: sync one zone bucket."""
    _require_credentials()
    label = zone_label or bucket
    if client is None:
        print(f"\n  Connecting to MinIO at {MINIO_ENDPOINT}...")
        client = create_minio_client()
    return sync_observations_to_delta(client, bucket, zone_label=label)


def sync_all_zones(*, client=None) -> dict[str, str | None]:
    """Sync landing, trusted, and exploitation zone Delta tables."""
    from shared.minio_helpers import LANDING_ZONE_BUCKET

    _require_credentials()
    if client is None:
        print(f"\n  Connecting to MinIO at {MINIO_ENDPOINT}...")
        client = create_minio_client()

    trusted_bucket = os.environ.get("TRUSTED_ZONE_BUCKET", "trusted-zone").strip() or "trusted-zone"
    exploitation_bucket = (
        os.environ.get("EXPLOITATION_ZONE_BUCKET", "exploitation-zone").strip()
        or "exploitation-zone"
    )

    results = {}
    for label, bucket in (
        ("landing", LANDING_ZONE_BUCKET),
        ("trusted", trusted_bucket),
        ("exploitation", exploitation_bucket),
    ):
        print(f"\n  --- {label} zone ({bucket}) ---")
        results[label] = sync_observations_to_delta(client, bucket, zone_label=label)
    return results


def main() -> None:
    """CLI: sync all zones, or one zone: python shared/sync_delta.py [landing|trusted|exploitation]."""
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    try:
        from dotenv import load_dotenv

        load_dotenv(root / ".env")
    except ImportError:
        pass

    from shared.minio_helpers import LANDING_ZONE_BUCKET

    zone = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    if zone in ("landing", "trusted", "exploitation"):
        buckets = {
            "landing": LANDING_ZONE_BUCKET,
            "trusted": os.environ.get("TRUSTED_ZONE_BUCKET", "trusted-zone"),
            "exploitation": os.environ.get("EXPLOITATION_ZONE_BUCKET", "exploitation-zone"),
        }
        run_for_bucket(buckets[zone], zone_label=zone)
    else:
        sync_all_zones()


if __name__ == "__main__":
    main()
