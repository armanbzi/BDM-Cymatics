#!/usr/bin/env python3
"""
Data Governance — File integrity & completeness checks (Great Expectations).

Validates data quality at each zone boundary:

  Checkpoint 1  Landing → Trusted
    • Structured:  UUID not null, no duplicates, frequency > 0, valid source.
    • Unstructured: Audio files exist in MinIO, WAV header valid, size > 0.

  Checkpoint 2  Trusted → Exploitation
    • Structured:  All trusted fields present, scores in [0,1], valid paths.
    • Unstructured: Cymatics images exist in MinIO, PNG readable, 2048×2048.
                    Cymatics videos exist in MinIO, size > 0.

  Checkpoint 3  Exploitation (embeddings)
    • Embedding vectors correct dimensionality, L2-normalised, not all zeros.

Orchestrator: option 11 → Data governance → Data quality checks.

Run directly:
    python governance/data_quality.py
"""

from __future__ import annotations

import io
import os
import struct
import sys
import uuid as _uuid
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
_EZ_DIR = _PROJECT_ROOT / "exploitation_zone"
if str(_EZ_DIR) not in sys.path:
    sys.path.insert(1, str(_EZ_DIR))

try:
    from dotenv import load_dotenv

    load_dotenv(_PROJECT_ROOT / ".env")
except ImportError:
    pass

import great_expectations as gx
import pandas as pd

from shared.minio_helpers import create_minio_client

# ── Constants ──────────────────────────────────────────────────────────────

LANDING_BUCKET = os.environ.get("LANDING_ZONE_BUCKET", "landing-zone")
TRUSTED_BUCKET = os.environ.get("TRUSTED_ZONE_BUCKET", "trusted-zone")
EXPLOITATION_BUCKET = os.environ.get("EXPLOITATION_ZONE_BUCKET", "exploitation-zone")
METADATA_KEY = "metadata/observations.csv"
QUALITY_REPORT_KEY = "governance/quality_report.json"

EXPECTED_IMG_RES = 2048
AUDIO_EMBEDDING_DIM = 2048
TEXT_EMBEDDING_DIM = 384
CYMATICS_EMBEDDING_DIM = 512


# ── Helpers ───────────────────────────────────────────────────────────────


def _load_csv(minio_client, bucket: str) -> pd.DataFrame:
    """Download a metadata CSV from MinIO and return a DataFrame."""
    resp = minio_client.get_object(bucket, METADATA_KEY)
    data = resp.read()
    resp.close()
    resp.release_conn()
    return pd.read_csv(io.BytesIO(data))


def _object_exists(minio_client, bucket: str, key: str) -> bool:
    """Check whether an object exists in a MinIO bucket."""
    try:
        minio_client.stat_object(bucket, key)
        return True
    except Exception:
        return False


def _object_size(minio_client, bucket: str, key: str) -> int:
    """Return object size in bytes, or -1 if not found."""
    try:
        info = minio_client.stat_object(bucket, key)
        return info.size
    except Exception:
        return -1


def _is_valid_wav_header(minio_client, bucket: str, key: str) -> bool:
    """Download the first 44 bytes and check for a valid WAV/RIFF header."""
    try:
        resp = minio_client.get_object(bucket, key, length=44)
        header = resp.read()
        resp.close()
        resp.release_conn()
        if len(header) < 12:
            return False
        return header[:4] == b"RIFF" and header[8:12] == b"WAVE"
    except Exception:
        return False


def _is_valid_png_header(minio_client, bucket: str, key: str) -> bool:
    """Download the first 8 bytes and check for a valid PNG signature."""
    PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
    try:
        resp = minio_client.get_object(bucket, key, length=8)
        header = resp.read()
        resp.close()
        resp.release_conn()
        return header == PNG_SIGNATURE
    except Exception:
        return False


def _png_dimensions(minio_client, bucket: str, key: str) -> tuple[int, int]:
    """Read PNG width and height from the IHDR chunk (bytes 16-23)."""
    try:
        resp = minio_client.get_object(bucket, key, length=24)
        data = resp.read()
        resp.close()
        resp.release_conn()
        if len(data) < 24:
            return (0, 0)
        width = struct.unpack(">I", data[16:20])[0]
        height = struct.unpack(">I", data[20:24])[0]
        return (width, height)
    except Exception:
        return (0, 0)


# ── Result display ───────────────────────────────────────────────────────


def _print_section(title: str) -> None:
    width = 62
    print(f"\n{'─' * width}")
    print(f"  {title}")
    print(f"{'─' * width}")


def _print_result(name: str, passed: int, failed: int, total: int) -> None:
    status = "PASS" if failed == 0 else "FAIL"
    icon = "✓" if failed == 0 else "✗"
    print(f"  {icon} {name:<44} {passed}/{total}  [{status}]")


def _print_summary(results: list[dict]) -> None:
    width = 62
    total_pass = sum(r["passed"] for r in results)
    total_fail = sum(r["failed"] for r in results)
    total_all = total_pass + total_fail
    print(f"\n{'═' * width}")
    if total_fail == 0:
        print(f"  ALL CHECKS PASSED — {total_pass}/{total_all} expectations met")
    else:
        print(f"  {total_fail} CHECK(S) FAILED — {total_pass}/{total_all} passed")
    print(f"{'═' * width}\n")


def save_quality_report(minio_client, results: list[dict]) -> str:
    """Save quality check results as JSON to the exploitation-zone bucket."""
    import json
    from datetime import datetime, timezone
    from io import BytesIO

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_passed": sum(r["passed"] for r in results),
        "total_failed": sum(r["failed"] for r in results),
        "checks": results,
    }

    payload = json.dumps(report, indent=2, default=str).encode("utf-8")
    minio_client.put_object(
        EXPLOITATION_BUCKET,
        QUALITY_REPORT_KEY,
        BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    path = f"{EXPLOITATION_BUCKET}/{QUALITY_REPORT_KEY}"
    print(f"\n  Quality report saved: {path} ({len(payload) / 1024:.1f} KB)")
    return path


# ── Checkpoint 1: Landing → Trusted ──────────────────────────────────────


def validate_landing_zone(minio_client) -> list[dict]:
    """Validate landing-zone metadata and file integrity."""
    _print_section("Checkpoint 1 — Landing Zone")

    df = _load_csv(minio_client, LANDING_BUCKET)
    print(f"  Loaded {len(df)} rows from {LANDING_BUCKET}/{METADATA_KEY}")

    results: list[dict] = []

    # ── Structured checks via Great Expectations ─────────────────
    context = gx.get_context()
    data_source = context.data_sources.add_pandas("landing_ds")
    data_asset = data_source.add_dataframe_asset("landing_observations")
    batch_def = data_asset.add_batch_definition_whole_dataframe("landing_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(
        gx.ExpectationSuite(name=f"landing_{_uuid.uuid4().hex[:8]}")
    )

    # UUID must not be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="uuid")
    )
    # UUID must be unique
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="uuid")
    )
    # peak_frequency_hz must be > 0
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="peak_frequency_hz", min_value=0.0, strict_min=True,
        )
    )
    # source must not be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="source")
    )
    # audio_path must not be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="audio_path")
    )
    # Required columns present
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=["uuid", "audio_path", "peak_frequency_hz", "source"],
            exact_match=False,
        )
    )

    validation = batch.validate(suite)

    for r in validation.results:
        name = r.expectation_config.type
        col = r.expectation_config.kwargs.get("column", "—")
        success = r.success
        label = f"{name} ({col})"
        p = 1 if success else 0
        f = 0 if success else 1
        results.append({"name": label, "passed": p, "failed": f})
        _print_result(label, p, f, 1)

    # Cleanup ephemeral suite
    # ── Unstructured checks: audio file integrity ────────────────
    _print_section("Checkpoint 1 — Audio File Integrity")

    audio_paths = df["audio_path"].dropna().tolist()
    exist_pass, exist_fail = 0, 0
    wav_pass, wav_fail = 0, 0
    size_pass, size_fail = 0, 0

    sample_size = min(len(audio_paths), 50)
    sampled = audio_paths[:sample_size]

    for path in sampled:
        if _object_exists(minio_client, LANDING_BUCKET, path):
            exist_pass += 1
        else:
            exist_fail += 1

        if _is_valid_wav_header(minio_client, LANDING_BUCKET, path):
            wav_pass += 1
        else:
            wav_fail += 1

        sz = _object_size(minio_client, LANDING_BUCKET, path)
        if sz > 0:
            size_pass += 1
        else:
            size_fail += 1

    results.append({"name": "Audio files exist", "passed": exist_pass, "failed": exist_fail})
    _print_result("Audio files exist in MinIO", exist_pass, exist_fail, sample_size)

    results.append({"name": "Valid WAV headers", "passed": wav_pass, "failed": wav_fail})
    _print_result("Valid WAV headers (RIFF/WAVE)", wav_pass, wav_fail, sample_size)

    results.append({"name": "Audio file size > 0", "passed": size_pass, "failed": size_fail})
    _print_result("Audio file size > 0 bytes", size_pass, size_fail, sample_size)

    return results


# ── Checkpoint 2: Trusted → Exploitation ─────────────────────────────────


def validate_trusted_zone(minio_client) -> list[dict]:
    """Validate trusted-zone metadata, images, and videos."""
    _print_section("Checkpoint 2 — Trusted Zone")

    df = _load_csv(minio_client, TRUSTED_BUCKET)
    print(f"  Loaded {len(df)} rows from {TRUSTED_BUCKET}/{METADATA_KEY}")

    results: list[dict] = []

    # ── Structured checks via Great Expectations ─────────────────
    context = gx.get_context()
    data_source = context.data_sources.add_pandas("trusted_ds")
    data_asset = data_source.add_dataframe_asset("trusted_observations")
    batch_def = data_asset.add_batch_definition_whole_dataframe("trusted_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(
        gx.ExpectationSuite(name=f"trusted_{_uuid.uuid4().hex[:8]}")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="uuid")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="uuid")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="peak_frequency_hz", min_value=0.0, strict_min=True,
        )
    )
    # Symmetry score in [0, 1]
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="symmetry_score", min_value=0.0, max_value=1.0,
        )
    )
    # Pattern stability in [0, 1]
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="pattern_stability_score", min_value=0.0, max_value=1.0,
        )
    )
    # image_path must not be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="image_path")
    )
    # video_path must not be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="video_path")
    )
    # Required columns present
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "uuid", "audio_path", "image_path", "video_path",
                "peak_frequency_hz", "symmetry_score",
                "pattern_stability_score", "source",
            ],
            exact_match=False,
        )
    )

    validation = batch.validate(suite)

    for r in validation.results:
        name = r.expectation_config.type
        col = r.expectation_config.kwargs.get("column", "—")
        success = r.success
        label = f"{name} ({col})"
        p = 1 if success else 0
        f = 0 if success else 1
        results.append({"name": label, "passed": p, "failed": f})
        _print_result(label, p, f, 1)

    # ── Unstructured checks: image integrity ─────────────────────
    _print_section("Checkpoint 2 — Cymatics Image Integrity")

    image_paths = df["image_path"].dropna().tolist()
    sample_size = min(len(image_paths), 50)
    sampled = image_paths[:sample_size]

    img_exist_p, img_exist_f = 0, 0
    png_valid_p, png_valid_f = 0, 0
    res_pass, res_fail = 0, 0

    for path in sampled:
        if _object_exists(minio_client, TRUSTED_BUCKET, path):
            img_exist_p += 1
        else:
            img_exist_f += 1

        if _is_valid_png_header(minio_client, TRUSTED_BUCKET, path):
            png_valid_p += 1
        else:
            png_valid_f += 1

        w, h = _png_dimensions(minio_client, TRUSTED_BUCKET, path)
        if w == EXPECTED_IMG_RES and h == EXPECTED_IMG_RES:
            res_pass += 1
        else:
            res_fail += 1

    results.append({"name": "Images exist", "passed": img_exist_p, "failed": img_exist_f})
    _print_result("Cymatics images exist in MinIO", img_exist_p, img_exist_f, sample_size)

    results.append({"name": "Valid PNG headers", "passed": png_valid_p, "failed": png_valid_f})
    _print_result("Valid PNG signature", png_valid_p, png_valid_f, sample_size)

    results.append({"name": "Image resolution 2048×2048", "passed": res_pass, "failed": res_fail})
    _print_result(f"Image resolution {EXPECTED_IMG_RES}×{EXPECTED_IMG_RES}", res_pass, res_fail, sample_size)

    # ── Unstructured checks: video integrity ─────────────────────
    _print_section("Checkpoint 2 — Cymatics Video Integrity")

    video_paths = df["video_path"].dropna().tolist()
    sample_size_v = min(len(video_paths), 50)
    sampled_v = video_paths[:sample_size_v]

    vid_exist_p, vid_exist_f = 0, 0
    vid_size_p, vid_size_f = 0, 0

    for path in sampled_v:
        if _object_exists(minio_client, TRUSTED_BUCKET, path):
            vid_exist_p += 1
        else:
            vid_exist_f += 1

        sz = _object_size(minio_client, TRUSTED_BUCKET, path)
        if sz > 0:
            vid_size_p += 1
        else:
            vid_size_f += 1

    results.append({"name": "Videos exist", "passed": vid_exist_p, "failed": vid_exist_f})
    _print_result("Cymatics videos exist in MinIO", vid_exist_p, vid_exist_f, sample_size_v)

    results.append({"name": "Video file size > 0", "passed": vid_size_p, "failed": vid_size_f})
    _print_result("Video file size > 0 bytes", vid_size_p, vid_size_f, sample_size_v)

    return results


# ── Checkpoint 3: Exploitation (embeddings) ──────────────────────────────


def validate_exploitation_zone(minio_client) -> list[dict]:
    """Validate exploitation-zone metadata and embedding sanity."""
    _print_section("Checkpoint 3 — Exploitation Zone")

    df = _load_csv(minio_client, EXPLOITATION_BUCKET)
    print(f"  Loaded {len(df)} rows from {EXPLOITATION_BUCKET}/{METADATA_KEY}")

    results: list[dict] = []

    # ── Structured checks via Great Expectations ─────────────────
    context = gx.get_context()
    data_source = context.data_sources.add_pandas("exploitation_ds")
    data_asset = data_source.add_dataframe_asset("exploitation_observations")
    batch_def = data_asset.add_batch_definition_whole_dataframe("exploitation_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(
        gx.ExpectationSuite(name=f"exploitation_{_uuid.uuid4().hex[:8]}")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="uuid")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="uuid")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="peak_frequency_hz", min_value=0.0, strict_min=True,
        )
    )
    # Spectral features must not be null
    for col in [
        "spectral_centroid_hz", "spectral_bandwidth_hz",
        "spectral_entropy", "loudness",
    ]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
        )
    # harmonic_energy_ratio in [0, 1]
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="harmonic_energy_ratio", min_value=0.0, max_value=1.0,
        )
    )
    # Required feature columns present
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "uuid", "spectral_centroid_hz", "spectral_bandwidth_hz",
                "spectral_rolloff_hz", "spectral_flatness", "signal_energy",
                "spectral_entropy", "zero_crossing_rate", "loudness",
                "MFCCs", "harmonic_energy_ratio",
            ],
            exact_match=False,
        )
    )

    validation = batch.validate(suite)

    for r in validation.results:
        name = r.expectation_config.type
        col = r.expectation_config.kwargs.get("column", "—")
        success = r.success
        label = f"{name} ({col})"
        p = 1 if success else 0
        f = 0 if success else 1
        results.append({"name": label, "passed": p, "failed": f})
        _print_result(label, p, f, 1)

    # ── Embedding sanity checks (Milvus) ─────────────────────────
    _print_section("Checkpoint 3 — Embedding Sanity (Milvus)")

    try:
        from milvus_embeddings import connect_milvus

        milvus_client = connect_milvus()

        for coll_name, expected_dim, field_name in [
            ("sound_audio_embeddings", AUDIO_EMBEDDING_DIM, "audio_embedding"),
            ("sound_text_embeddings", TEXT_EMBEDDING_DIM, "text_embedding"),
            ("sound_cymatics_embeddings", CYMATICS_EMBEDDING_DIM, "cymatics_embedding"),
        ]:
            if not milvus_client.has_collection(coll_name):
                results.append({"name": f"Collection {coll_name}", "passed": 0, "failed": 1})
                _print_result(f"Collection '{coll_name}' exists", 0, 1, 1)
                continue

            stats = milvus_client.get_collection_stats(coll_name)
            row_count = int(stats.get("row_count", 0))

            results.append({"name": f"Collection {coll_name}", "passed": 1, "failed": 0})
            _print_result(
                f"Collection '{coll_name}' exists ({row_count} rows)", 1, 0, 1,
            )

            # Sample a few vectors to check dimensionality
            sample = milvus_client.query(
                collection_name=coll_name,
                filter="",
                output_fields=["uuid", field_name],
                limit=5,
            )

            dim_pass, dim_fail = 0, 0
            zero_pass, zero_fail = 0, 0
            for entity in sample:
                vec = entity.get(field_name, [])
                if len(vec) == expected_dim:
                    dim_pass += 1
                else:
                    dim_fail += 1
                if any(v != 0.0 for v in vec):
                    zero_pass += 1
                else:
                    zero_fail += 1

            n = len(sample)
            results.append({"name": f"{coll_name} dim={expected_dim}", "passed": dim_pass, "failed": dim_fail})
            _print_result(f"  Dimensionality = {expected_dim}", dim_pass, dim_fail, n)

            results.append({"name": f"{coll_name} non-zero", "passed": zero_pass, "failed": zero_fail})
            _print_result(f"  Vectors non-zero", zero_pass, zero_fail, n)

    except Exception as e:
        print(f"  Milvus not reachable — skipping embedding checks: {e}")
        results.append({"name": "Milvus connection", "passed": 0, "failed": 1})

    return results


# ── Interactive CLI ──────────────────────────────────────────────────────


def run_interactive(*, from_orchestrator: bool = False) -> None:
    """Run all data quality checkpoints and display summary."""
    width = 62
    print(f"\n{'─' * width}")
    print("  Data Governance — Quality Checks (Great Expectations)")
    print(f"{'─' * width}")
    print("  Validates data integrity at every zone boundary:")
    print("    CP1: Landing  → structured metadata + audio files")
    print("    CP2: Trusted  → metadata + cymatics images + videos")
    print("    CP3: Exploitation → features + Milvus embeddings")
    print(f"{'─' * width}")
    print()

    try:
        minio_client = create_minio_client()
    except Exception as e:
        print(f"\n  Failed to connect to MinIO: {e}")
        print("  Start MinIO with: docker compose up -d minio")
        if not from_orchestrator:
            raise SystemExit(1) from e
        return

    all_results: list[dict] = []

    # ── Run checkpoints ──────────────────────────────────────────
    try:
        all_results.extend(validate_landing_zone(minio_client))
    except Exception as e:
        print(f"\n  Landing zone validation failed: {e}")

    try:
        all_results.extend(validate_trusted_zone(minio_client))
    except Exception as e:
        print(f"\n  Trusted zone validation failed: {e}")

    try:
        all_results.extend(validate_exploitation_zone(minio_client))
    except Exception as e:
        print(f"\n  Exploitation zone validation failed: {e}")

    # ── Summary & persist ──────────────────────────────────────
    _print_summary(all_results)
    save_quality_report(minio_client, all_results)

    return all_results


def main() -> None:
    run_interactive(from_orchestrator=False)


if __name__ == "__main__":
    main()
