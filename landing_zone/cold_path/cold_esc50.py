#!/usr/bin/env python3
"""
Landing Zone — cold-path (ESC-50).

1. Downloads the ESC-50 dataset from GitHub if not already present locally.
2. Picks records across random categories to ensure variety.
3. Stores audio WAV in MinIO, appends lightweight metadata rows.
4. Deletes the temporary dataset folder after ingestion completes.

Only categories matching our existing dataset are ingested:
  rain, sea_waves, thunderstorm, crickets, chirping_birds, water_drops,
  wind, church_bells, cat, dog, crow, rooster, hen, insects, frog,
  crackling_fire, pouring_water.

Requires:
  - env vars: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
  - Optional: ESC50_BASE_PATH (auto-downloads if not set or missing)

Run:
    python cold_esc50.py            # interactive — prompts for batch size
    python cold_esc50.py 20         # CLI — ingest 20 sounds

In Jupyter, set BATCH_SIZE below to a positive integer.
"""

import os
import io
import csv
import random
import shutil
import tempfile
import uuid
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
load_dotenv()

import numpy as np
from scipy.signal import find_peaks
from scipy.io import wavfile

from minio import Minio

import pyarrow as pa
import pyarrow.parquet as pq

# =============================================================================
#  Config from env
# =============================================================================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "landing-zone")

ESC50_BASE_PATH = os.environ.get("ESC50_BASE_PATH", "")

ESC50_GITHUB_ZIP = (
    "https://github.com/karoldvl/ESC-50/archive/refs/heads/master.zip"
)
ESC50_DEFAULT_DIR = os.path.join(tempfile.gettempdir(), "ESC-50-master")

# In Jupyter set this to a positive int (e.g. 10); CLI can pass as argv.
BATCH_SIZE = None

SOURCE = "ESC-50"
METADATA_KEY = "metadata/observations.csv"
SAMPLE_RATE = 44100

# ESC-50 categories that overlap with our dataset's sound classes
ESC50_CATEGORIES = [
    "rain", "sea_waves", "thunderstorm", "crickets", "chirping_birds",
    "water_drops", "wind", "church_bells", "cat", "dog",
    "crow", "rooster", "hen", "insects", "frog",
    "crackling_fire", "pouring_water",
]

METADATA_FIELDS = [
    "uuid", "source_id", "time_recorded/added", "duration",
    "audio_size", "audio_path", "audio_format", "source", "peak_frequency_hz",
]

COLD_EXTRA_FIELDS = ["tags", "description", "category"]

# =============================================================================
#  Parquet / Delta Lake helpers
# =============================================================================
PARQUET_KEY = "metadata/observations.parquet"


def _rows_to_table(rows):
    if not rows:
        return None
    all_keys = list(dict.fromkeys(k for row in rows for k in row))
    arrays = {key: [str(row.get(key, "")) for row in rows] for key in all_keys}
    return pa.table(arrays)


def _read_parquet_from_minio(client, bucket, key=PARQUET_KEY):
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


def update_parquet(client, bucket, new_rows):
    new_table = _rows_to_table(new_rows)
    if new_table is None:
        return _read_parquet_from_minio(client, bucket)
    existing = _read_parquet_from_minio(client, bucket)
    combined = _unify_schemas(existing, new_table)
    _write_parquet_to_minio(client, bucket, combined)
    print(f"  Updated: {PARQUET_KEY}  ({combined.num_rows} total rows)")
    return combined


# =============================================================================
#  Peak frequency detection (harmonic selection — same as warm / hot path)
# =============================================================================
def harmonic_dominant_freq(window_candidates, max_harmonics=5, use_energy_weight=True):
    candidate_counts = Counter()
    for c in window_candidates:
        f = c[0]
        if f <= 0:
            continue
        weight = c[3] if use_energy_weight else 1
        for div in range(1, max_harmonics + 1):
            if f % div == 0:
                candidate_counts[f // div] += weight
    if not candidate_counts:
        return 0.0
    return float(max(candidate_counts.keys(), key=lambda k: candidate_counts[k]))


def dominant_freq_hz_for_chunk(chunk, sample_rate=SAMPLE_RATE):
    if len(chunk) < 64:
        return 0.0
    fft = np.abs(np.fft.rfft(chunk))
    fft_norm = fft / (np.max(fft) + 1e-12)
    pidx, _ = find_peaks(fft_norm, height=0.06, distance=6, prominence=0.04)
    if len(pidx) == 0:
        pidx = np.array([np.argmax(fft_norm)])
    top_idx = pidx[np.argmax(fft_norm[pidx])]
    return float(top_idx * sample_rate / len(chunk))


def detect_peak_freq(audio, sample_rate=SAMPLE_RATE):
    """Sliding 0.25s windows + harmonic voting (max_harmonics=4), matching warm path."""
    audio = np.asarray(audio, dtype=np.float64).flatten()
    if len(audio) < 64:
        return 0.0
    window_len = int(sample_rate * 0.25)
    hop = max(1, window_len // 2)
    window_candidates = []
    if len(audio) > window_len:
        for off in range(0, len(audio) - window_len, hop):
            chunk = audio[off : off + window_len]
            hz = dominant_freq_hz_for_chunk(chunk, sample_rate)
            freq_bin = int(round(hz))
            energy = float(np.sum(np.abs(np.fft.rfft(chunk)) ** 2))
            window_candidates.append((freq_bin, chunk.copy(), off, energy, hz))
    else:
        hz = dominant_freq_hz_for_chunk(audio, sample_rate)
        freq_bin = int(round(hz))
        energy = float(np.sum(np.abs(np.fft.rfft(audio)) ** 2))
        window_candidates.append((freq_bin, audio.copy(), 0, energy, hz))
    if not window_candidates:
        return 0.0
    return harmonic_dominant_freq(window_candidates, max_harmonics=4, use_energy_weight=True)


# =============================================================================
#  CSV helpers
# =============================================================================
def merge_metadata_fieldnames(existing_names, row_keys):
    out = list(existing_names or [])
    seen = set(out)
    for k in METADATA_FIELDS + COLD_EXTRA_FIELDS:
        if k not in seen:
            seen.add(k)
            out.append(k)
    for k in row_keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


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
#  MinIO helpers
# =============================================================================
def ensure_minio_structure(client):
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"  Created bucket: {MINIO_BUCKET}")
    placeholders = [
        "metadata/.keep",
        f"audio/{SOURCE}/.keep",
    ]
    for key in placeholders:
        try:
            client.stat_object(MINIO_BUCKET, key)
        except Exception:
            client.put_object(MINIO_BUCKET, key, io.BytesIO(b""), 0)
    print("  Bucket and folder structure ready.")


# =============================================================================
#  Deduplication
# =============================================================================
def load_existing_source_ids(client):
    """Return set of source_id values already stored in the metadata CSV."""
    try:
        resp = client.get_object(MINIO_BUCKET, METADATA_KEY)
        data = resp.read().decode("utf-8")
        resp.close(); resp.release_conn()
    except Exception:
        return set()
    reader = csv.DictReader(io.StringIO(data))
    ids = set()
    for row in reader:
        val = row.get("source_id", "").strip()
        if val:
            ids.add(val)
    return ids


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
    """Pick up to *batch_size* records, cycling through categories so each
    consecutive record comes from a different category."""
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

    # --- Step 1: Download dataset if needed ---
    print("\n[1/3] Resolving ESC-50 dataset location...")
    esc50_root = download_esc50_if_needed()
    is_temp = esc50_root == ESC50_DEFAULT_DIR
    meta_csv = os.path.join(esc50_root, "meta", "esc50.csv")
    audio_dir = os.path.join(esc50_root, "audio")
    if not os.path.isfile(meta_csv):
        raise RuntimeError(f"ESC-50 metadata CSV not found at {meta_csv}")
    if not os.path.isdir(audio_dir):
        raise RuntimeError(f"ESC-50 audio directory not found at {audio_dir}")

    # --- Step 2: Connect to MinIO ---
    print("\n[2/3] Connecting to MinIO...")
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        ensure_minio_structure(client)
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

    # --- Select batch with category diversity ---
    print(f"\n[3/3] Selecting diverse batch of up to {batch_size} records...")
    existing_ids = load_existing_source_ids(client)
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

    # --- Process batch (audio upload + metadata) ---
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

    # Append to shared CSV
    try:
        resp = client.get_object(MINIO_BUCKET, METADATA_KEY)
        existing = resp.read().decode("utf-8")
        resp.close(); resp.release_conn()
    except Exception:
        existing = None

    buf = io.StringIO()
    if existing:
        reader = csv.DictReader(io.StringIO(existing))
        fieldnames = merge_metadata_fieldnames(reader.fieldnames, rows[0].keys())
        rows_list = list(reader)
    else:
        fieldnames = merge_metadata_fieldnames([], rows[0].keys())
        rows_list = []

    for prev in rows_list:
        for fn in fieldnames:
            if fn not in prev:
                prev[fn] = ""

    rows_list.extend(rows)
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_list)
    csv_bytes = buf.getvalue().encode("utf-8")
    client.put_object(MINIO_BUCKET, METADATA_KEY, io.BytesIO(csv_bytes), len(csv_bytes), "text/csv")

    update_parquet(client, MINIO_BUCKET, rows)

    print(f"\n  Cold-path ESC-50 ingestion complete.")
    print(f"   Batch: {len(rows)} records across {len(set(r['category'] for r in rows))} categories.")
    print(f"   Metadata: {METADATA_KEY}")

    # Cleanup: remove temp dataset folder if it was auto-downloaded
    if is_temp and os.path.isdir(esc50_root):
        print(f"\n  Cleaning up temporary dataset at {esc50_root}...")
        shutil.rmtree(esc50_root, ignore_errors=True)
        print("  Temporary files removed.\n")
    else:
        print()


def _resolve_batch_size():
    import sys
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
