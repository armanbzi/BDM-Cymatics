#!/usr/bin/env python3
"""
Landing Zone — cold-path (ESC-50).

1. Downloads the ESC-50 dataset from GitHub if not already present locally.
2. Picks records across random categories to ensure variety.
3. Stores audio WAV in MinIO, appends lightweight metadata rows.
4. Deletes the temporary dataset folder after ingestion completes.

Only categories matching our interests are ingested:
  rain, sea_waves, thunderstorm, crickets, chirping_birds, water_drops,
  wind, church_bells, cat, dog, crow, rooster, hen, insects, frog,
  crackling_fire, pouring_water.

Requires:
  - env vars: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
  - Optional: ESC50_BASE_PATH (auto-downloads if not set or missing)

Run:
    python cold_esc50.py            # interactive — prompts for batch size
    python cold_esc50.py 20         # CLI — ingest 20 sounds

"""

import os
import sys
import csv
import random
import shutil
import tempfile
import uuid
import zipfile
from collections import defaultdict
from datetime import datetime, timezone

import requests

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dotenv import load_dotenv
load_dotenv()

import numpy as np
from scipy.io import wavfile

from shared.minio_helpers import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT,
    MINIO_BUCKET, create_minio_client, ensure_bucket,
    METADATA_KEY, LANDING_METADATA_FIELDS,
    update_parquet, append_rows_to_csv, load_existing_source_ids,
)
from shared.freq_detection import detect_peak_freq

# =============================================================================
#  Config
# =============================================================================
ESC50_BASE_PATH = os.environ.get("ESC50_BASE_PATH", "")

ESC50_GITHUB_ZIP = (
    "https://github.com/karoldvl/ESC-50/archive/refs/heads/master.zip"
)
ESC50_DEFAULT_DIR = os.path.join(tempfile.gettempdir(), "ESC-50-master")

BATCH_SIZE = None

SOURCE = "ESC-50"
SAMPLE_RATE = 44100

ESC50_CATEGORIES = [
    "rain", "sea_waves", "thunderstorm", "crickets", "chirping_birds",
    "water_drops", "wind", "church_bells", "cat", "dog",
    "crow", "rooster", "hen", "insects", "frog",
    "crackling_fire", "pouring_water",
]

COLD_EXTRA_FIELDS = ["tags", "description", "category"]
PRIORITY_FIELDS = LANDING_METADATA_FIELDS + COLD_EXTRA_FIELDS


# =============================================================================
#  Dataset download
# =============================================================================
def download_esc50_if_needed():
    """Return path to ESC-50 root. Downloads from GitHub if missing."""
    if ESC50_BASE_PATH and os.path.isdir(ESC50_BASE_PATH):
        meta = os.path.join(ESC50_BASE_PATH, "meta", "esc50.csv")
        if os.path.isfile(meta):
            print(f"  Using existing ESC-50 at {ESC50_BASE_PATH}")
            return ESC50_BASE_PATH

    if os.path.isdir(ESC50_DEFAULT_DIR):
        meta = os.path.join(ESC50_DEFAULT_DIR, "meta", "esc50.csv")
        if os.path.isfile(meta):
            print(f"  Using cached ESC-50 at {ESC50_DEFAULT_DIR}")
            return ESC50_DEFAULT_DIR

    print("  ESC-50 dataset not found — downloading from GitHub...")
    zip_path = os.path.join(tempfile.gettempdir(), "esc50.zip")
    try:
        resp = requests.get(ESC50_GITHUB_ZIP, stream=True, timeout=300)
        resp.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
        print(f"  Downloaded {os.path.getsize(zip_path) / 1e6:.1f} MB")
    except Exception as e:
        raise RuntimeError(
            f"Failed to download ESC-50 from GitHub: {e}\n"
            "Set ESC50_BASE_PATH in .env to a local copy instead."
        ) from e

    print("  Extracting...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(tempfile.gettempdir())
    os.remove(zip_path)

    if not os.path.isdir(ESC50_DEFAULT_DIR):
        raise RuntimeError(
            f"Extraction did not produce expected directory {ESC50_DEFAULT_DIR}"
        )
    print(f"  ESC-50 ready at {ESC50_DEFAULT_DIR}")
    return ESC50_DEFAULT_DIR


# =============================================================================
#  Process one ESC-50 record — audio upload + metadata row
# =============================================================================
def process_record(client, audio_np, esc_row):
    """Upload audio WAV to MinIO, return lightweight metadata row dict (or None)."""
    audio = audio_np.copy()
    duration = float(len(audio) / SAMPLE_RATE)

    pk = np.max(np.abs(audio))
    if pk > 1e-6:
        audio /= pk

    peak_freq = detect_peak_freq(audio)
    peak_freq_int = int(round(peak_freq))

    obs_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    category = esc_row["category"]
    safe_cat = category.replace(" ", "_")
    stem = f"{safe_cat}_{peak_freq_int}_{obs_id}"
    audio_key = f"audio/{SOURCE}/{peak_freq_int}/{stem}.wav"

    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            wav_tmp = f.name
        wavfile.write(wav_tmp, SAMPLE_RATE, (audio * 32767).astype(np.int16))
        audio_size = os.path.getsize(wav_tmp)
        with open(wav_tmp, "rb") as f:
            client.put_object(MINIO_BUCKET, audio_key, f, audio_size, "audio/wav")
        os.remove(wav_tmp)
    except Exception as e:
        print(f"    Upload failed: {e}")
        return None

    return {
        "uuid": obs_id,
        "source_id": esc_row["filename"],
        "time_recorded/added": ts,
        "duration": duration,
        "audio_size": audio_size,
        "audio_path": audio_key,
        "audio_format": "wav",
        "source": SOURCE,
        "peak_frequency_hz": peak_freq,
        "tags": category,
        "description": f"ESC-50 fold={esc_row.get('fold', '-')} target={esc_row.get('target', '-')}",
        "category": category,
    }


# =============================================================================
#  Category-diverse batch selection
# =============================================================================
def select_diverse_batch(candidates, batch_size):
    """Pick up to *batch_size* records, cycling through categories."""
    by_cat = defaultdict(list)
    for r in candidates:
        by_cat[r["category"]].append(r)
    for recs in by_cat.values():
        random.shuffle(recs)

    cats = list(by_cat.keys())
    random.shuffle(cats)
    batch = []
    idx = 0
    while len(batch) < batch_size:
        exhausted = 0
        for _ in range(len(cats)):
            if len(batch) >= batch_size:
                break
            cat = cats[idx % len(cats)]
            idx += 1
            if by_cat[cat]:
                batch.append(by_cat[cat].pop())
            else:
                exhausted += 1
        if exhausted == len(cats):
            break
    return batch


# =============================================================================
#  Main
# =============================================================================
def run(batch_size=10):
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError(
            "MinIO credentials missing. "
            "Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY in your .env file."
        )

    print("\n[1/3] Resolving ESC-50 dataset location...")
    esc50_root = download_esc50_if_needed()
    is_temp = esc50_root == ESC50_DEFAULT_DIR
    meta_csv = os.path.join(esc50_root, "meta", "esc50.csv")
    audio_dir = os.path.join(esc50_root, "audio")
    if not os.path.isfile(meta_csv):
        raise RuntimeError(f"ESC-50 metadata CSV not found at {meta_csv}")
    if not os.path.isdir(audio_dir):
        raise RuntimeError(f"ESC-50 audio directory not found at {audio_dir}")

    print("\n[2/3] Connecting to MinIO...")
    try:
        client = create_minio_client()
        ensure_bucket(client, MINIO_BUCKET, ["metadata/.keep", f"audio/{SOURCE}/.keep"])
    except Exception as e:
        raise RuntimeError(
            f"Cannot connect to MinIO at {MINIO_ENDPOINT}. "
            f"Check that the server is running and credentials are correct.\n"
            f"  Detail: {e}"
        ) from e

    with open(meta_csv, newline="") as f:
        reader = csv.DictReader(f)
        all_records = list(reader)

    allowed = set(ESC50_CATEGORIES)
    eligible = [r for r in all_records if r["category"] in allowed]
    print(f"  ESC-50: {len(all_records)} total records, {len(eligible)} in our categories.")

    print(f"\n[3/3] Selecting diverse batch of up to {batch_size} records...")
    existing_ids = load_existing_source_ids(client, MINIO_BUCKET)
    if existing_ids:
        print(f"  Found {len(existing_ids)} existing records — duplicates will be skipped.")

    candidates = [r for r in eligible if r["filename"] not in existing_ids]
    print(f"  New candidates after dedup: {len(candidates)}")

    if not candidates:
        print("  Nothing to ingest (all eligible records already exist in metadata).")
        return

    batch = select_diverse_batch(candidates, batch_size)
    cats_in_batch = defaultdict(int)
    for r in batch:
        cats_in_batch[r["category"]] += 1
    print(f"  Selected {len(batch)} records across {len(cats_in_batch)} categories:")
    for cat, cnt in sorted(cats_in_batch.items()):
        print(f"    {cat}: {cnt}")

    print(f"\n  Processing batch of {len(batch)} records...\n")
    rows = []
    for i, esc_row in enumerate(batch):
        fname = esc_row["filename"]
        cat = esc_row["category"]
        fpath = os.path.join(audio_dir, fname)
        print(f"  [{i+1}/{len(batch)}] {fname} (category={cat})")

        if not os.path.isfile(fpath):
            print(f"    Skipped — file not found: {fpath}")
            continue
        try:
            sr, data = wavfile.read(fpath)
            if data.dtype == np.int16:
                audio_np = data.astype(np.float64) / 32768.0
            else:
                audio_np = data.astype(np.float64)
            if audio_np.ndim > 1:
                audio_np = audio_np.mean(axis=1)
        except Exception as e:
            print(f"    Skipped — read failed: {e}")
            continue
        if len(audio_np) < SAMPLE_RATE:
            print("    Skipped — audio too short.")
            continue

        row = process_record(client, audio_np, esc_row)
        if row:
            rows.append(row)
            print(f"    Stored — {row['peak_frequency_hz']:.0f} Hz  uuid={row['uuid'][:8]}...")

    if not rows:
        print("\n  No records processed.")
        return

    append_rows_to_csv(client, MINIO_BUCKET, rows, PRIORITY_FIELDS)
    update_parquet(client, MINIO_BUCKET, rows)

    print("\n  Cold-path ESC-50 ingestion complete.")
    print(f"   Batch: {len(rows)} records across {len(set(r['category'] for r in rows))} categories.")
    print(f"   Metadata: {METADATA_KEY}")

    if is_temp and os.path.isdir(esc50_root):
        print(f"\n  Cleaning up temporary dataset at {esc50_root}...")
        shutil.rmtree(esc50_root, ignore_errors=True)
        print("  Temporary files removed.\n")
    else:
        print()


def _resolve_batch_size():
    if BATCH_SIZE is not None:
        try:
            n = int(BATCH_SIZE)
            if n > 0:
                return n
        except (TypeError, ValueError):
            pass
    for arg in sys.argv[1:]:
        try:
            n = int(arg)
            if n > 0:
                return n
        except ValueError:
            continue
    return int(input("Enter batch size (number of ESC-50 records to ingest): "))


if __name__ == "__main__":
    run(batch_size=_resolve_batch_size())
