#!/usr/bin/env python3
"""
Landing Zone — cold-path (Freesound).

Downloads a user-specified batch of audio files (≤10 s, original quality)
from Freesound API v2, uploads each WAV to MinIO, and appends
metadata rows to the CSV / Parquet.

Requires:
  - env vars: FREESOUND_API_KEY, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
  - ffmpeg on PATH (for decoding non-WAV formats to 44100 Hz mono WAV)

Run:
    python cold_freesound.py          # interactive — prompts for batch size
    python cold_freesound.py 20       # CLI — ingest 20 sounds
"""

import os
import io
import csv
import json
import subprocess
import tempfile
import random
import time
import uuid
from collections import Counter
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

FREESOUND_API_KEY = os.environ.get("FREESOUND_API_KEY", "")

# In Jupyter set this to a positive int (e.g. 10); CLI can pass as argv.
BATCH_SIZE = None

SOURCE = "Freesound"
METADATA_KEY = "metadata/observations.csv"
SAMPLE_RATE = 44100

QUERIES = [
    "rain", "church bells", "crickets",
    "thunder", "water drops", "sea waves",
    "wind", "singing bowl", "tuning fork",
    "bird song", "cat purring", "dog bark",
    "waterfall", "ocean", "forest",
]

METADATA_FIELDS = [
    "uuid", "source_id", "time_recorded/added", "duration",
    "audio_size", "audio_path", "audio_format", "source", "peak_frequency_hz",
]

COLD_EXTRA_FIELDS = ["tags", "description", "category"]


# =============================================================================
#  Parquet helpers
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
#  Peak-frequency detection (harmonic selection — same as warm / hot path)
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
#  Freesound helpers
# =============================================================================
FREESOUND_SEARCH_FIELDS = (
    "id,name,tags,description,created,type,filesize,duration,"
    "geotag,channels,samplerate,bitrate,bitdepth,license,username,"
    "previews,download"
)


def search_freesound(query, page_size=15, page=1, created_after=None):
    """Return list of sound dicts for ≤10 s sounds matching *query*.
    If *created_after* is given (ISO-8601 date string, e.g. '2025-01-01'),
    only sounds created after that date are returned.
    """
    url = "https://freesound.org/apiv2/search/text/"
    duration_filter = "duration:[1 TO 10]"
    if created_after:
        duration_filter += f" created:[{created_after} TO *]"
    params = {
        "query": query,
        "token": FREESOUND_API_KEY,
        "fields": FREESOUND_SEARCH_FIELDS,
        "page_size": page_size,
        "page": page,
        "filter": duration_filter,
    }
    resp = requests.get(url, params=params, timeout=30)
    data = resp.json()
    return data.get("results", [])


def download_sound_bytes(sound):
    """Download original audio; fall back to high-quality preview."""
    dl_url = sound.get("download")
    if dl_url:
        r = requests.get(dl_url, params={"token": FREESOUND_API_KEY}, timeout=60,
                         allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 100:
            ext = sound.get("type", "wav")
            return r.content, ext
    previews = sound.get("previews", {})
    for key in ("preview-hq-mp3", "preview-lq-mp3"):
        url = previews.get(key)
        if url:
            r = requests.get(url, timeout=60)
            if r.status_code == 200 and len(r.content) > 100:
                return r.content, "mp3"
    return None, None


def decode_to_numpy(audio_bytes, ext="wav"):
    """Decode audio bytes to mono float64 at SAMPLE_RATE via ffmpeg."""
    with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as f:
        f.write(audio_bytes)
        src = f.name
    dst = src + ".wav"
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", src, "-ar", str(SAMPLE_RATE), "-ac", "1",
             "-sample_fmt", "s16", dst],
            capture_output=True, check=True,
        )
        sr, data = wavfile.read(dst)
        return data.astype(np.float64) / 32768.0
    finally:
        for p in (src, dst):
            try:
                os.remove(p)
            except OSError:
                pass


# =============================================================================
#  MinIO helpers
# =============================================================================
LAST_INGESTION_KEY = "metadata/freesound_last_ingestion.txt"


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


def read_last_ingestion_time(client):
    """Read the last-ingestion ISO timestamp from MinIO (or None)."""
    try:
        resp = client.get_object(MINIO_BUCKET, LAST_INGESTION_KEY)
        ts = resp.read().decode("utf-8").strip()
        resp.close(); resp.release_conn()
        return ts if ts else None
    except Exception:
        return None


def write_last_ingestion_time(client, ts_iso):
    """Persist the current ingestion timestamp to MinIO."""
    data = ts_iso.encode("utf-8")
    client.put_object(MINIO_BUCKET, LAST_INGESTION_KEY,
                      io.BytesIO(data), len(data), "text/plain")


# =============================================================================
#  Process one sound — upload audio + return metadata row
# =============================================================================
def process_sound(client, sound, audio_np, query):
    """Upload audio WAV to MinIO and return a lightweight metadata dict."""
    audio = audio_np.copy()
    actual_duration_s = float(len(audio) / SAMPLE_RATE)

    pk = np.max(np.abs(audio))
    if pk > 1e-6:
        audio /= pk

    peak_freq = detect_peak_freq(audio)
    peak_freq_int = int(round(peak_freq))

    obs_id = str(uuid.uuid4())
    safe_cat = query.replace(" ", "_")
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

    tags_raw = sound.get("tags", [])
    tags_str = "|".join(tags_raw) if isinstance(tags_raw, list) else str(tags_raw)
    description = (sound.get("description") or "-").replace("\n", " ").replace("\r", "")[:500]
    created = sound.get("created", "-")

    row = {
        "uuid": obs_id,
        "source_id": str(sound.get("id", "")),
        "time_recorded/added": created if created and created != "-" else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "duration": actual_duration_s,
        "audio_size": audio_size,
        "audio_path": audio_key,
        "audio_format": sound.get("type", "-"),
        "source": SOURCE,
        "peak_frequency_hz": peak_freq,
        "tags": tags_str,
        "description": description,
        "category": query,
    }
    return row


# =============================================================================
#  Deduplication: load existing source_id values from CSV in MinIO
# =============================================================================
def load_existing_source_ids(client):
    """Return set of source_id strings already stored in the metadata CSV."""
    try:
        resp = client.get_object(MINIO_BUCKET, METADATA_KEY)
        data = resp.read().decode("utf-8")
        resp.close(); resp.release_conn()
    except Exception:
        return set()
    reader = csv.DictReader(io.StringIO(data))
    ids = set()
    for row in reader:
        sid = row.get("source_id", "").strip()
        if sid:
            ids.add(sid)
    return ids


# =============================================================================
#  Main
# =============================================================================
def run(batch_size=10, created_after=None, update_checkpoint=False):
    """Run cold-path Freesound ingestion.

    Args:
        batch_size: max number of sounds to ingest.
        created_after: ISO date string (e.g. '2025-01-01') — only fetch sounds
            created after this date.  If *None* and *update_checkpoint* is True,
            the last-ingestion timestamp stored in MinIO is used automatically.
        update_checkpoint: if True, persist current UTC time as the new
            last-ingestion timestamp in MinIO after a successful run.
    """
    if not FREESOUND_API_KEY:
        raise RuntimeError(
            "Freesound API key not found. "
            "Set FREESOUND_API_KEY in your .env file "
            "(get one at https://freesound.org/apiv2/apply/)."
        )
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError(
            "MinIO credentials missing. "
            "Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY in your .env file."
        )

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

    if created_after is None and update_checkpoint:
        created_after = read_last_ingestion_time(client)
    if created_after:
        print(f"  Incremental mode: only sounds created after {created_after}")

    run_start_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    existing_ids = load_existing_source_ids(client)
    if existing_ids:
        print(f"  Found {len(existing_ids)} existing records in metadata — duplicates will be skipped.")

    try:
        probe = search_freesound(QUERIES[0], page_size=1, page=1, created_after=created_after)
        if isinstance(probe, dict) and "detail" in probe:
            raise RuntimeError(
                f"Freesound API rejected the request: {probe['detail']}. "
                "Check that FREESOUND_API_KEY is valid."
            )
    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Cannot reach Freesound API (network error): {e}"
        ) from e

    collected = []
    seen_ids = set()
    exhausted_queries = set()
    query_pool = {q: [] for q in QUERIES}
    query_page = {q: 1 for q in QUERIES}
    max_attempts = batch_size * 10
    attempts = 0
    while len(collected) < batch_size and attempts < max_attempts:
        available = [q for q in QUERIES if q not in exhausted_queries]
        if not available:
            break
        query = random.choice(available)
        if not query_pool[query]:
            try:
                results = search_freesound(query, page_size=15,
                                           page=query_page[query],
                                           created_after=created_after)
            except requests.exceptions.RequestException as e:
                print(f"  Warning: search failed for '{query}': {e}")
                exhausted_queries.add(query)
                attempts += 1
                continue
            if not results:
                exhausted_queries.add(query)
                attempts += 1
                continue
            random.shuffle(results)
            query_pool[query] = results
            query_page[query] += 1
            time.sleep(0.3)
        s = query_pool[query].pop()
        sid = str(s["id"])
        attempts += 1
        if sid in seen_ids or sid in existing_ids:
            if sid in existing_ids:
                print(f"  Skipped (already in CSV): source_id={sid}")
            continue
        seen_ids.add(sid)
        collected.append((s, query))

    print(f"\n  Collected {len(collected)} new sounds from Freesound.\n")

    if not collected:
        print("  Nothing to ingest (all candidates already exist in metadata).")
        if update_checkpoint:
            write_last_ingestion_time(client, run_start_ts)
        return

    rows = []
    for i, (sound, query) in enumerate(collected):
        name = sound.get("name", "?")
        print(f"  [{i+1}/{len(collected)}] {name} ({sound.get('duration',0):.1f}s, query={query})")

        try:
            audio_bytes, ext = download_sound_bytes(sound)
        except requests.exceptions.RequestException as e:
            print(f"    Skipped — download error: {e}")
            continue
        if audio_bytes is None:
            print("    Skipped — download failed (no audio returned).")
            continue
        try:
            audio_np = decode_to_numpy(audio_bytes, ext)
        except Exception as e:
            print(f"    Skipped — decode failed: {e}")
            continue
        if len(audio_np) < SAMPLE_RATE:
            print("    Skipped — audio too short after decode.")
            continue

        row = process_sound(client, sound, audio_np, query)
        if row:
            rows.append(row)
            print(f"    Stored — {row['peak_frequency_hz']:.0f} Hz  uuid={row['uuid'][:8]}...")
        time.sleep(0.3)

    if not rows:
        print("\n  No sounds processed.")
        if update_checkpoint:
            write_last_ingestion_time(client, run_start_ts)
        return

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

    if update_checkpoint:
        write_last_ingestion_time(client, run_start_ts)
        print(f"  Checkpoint updated: {LAST_INGESTION_KEY} → {run_start_ts}")

    print(f"\n  Cold-path ingestion complete.")
    print(f"   Batch: {len(rows)} sounds processed and stored.")
    print(f"   Metadata: {METADATA_KEY}\n")


def _resolve_batch_size():
    """CLI argv, module-level BATCH_SIZE, or interactive prompt."""
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
    return int(input("Enter batch size (number of sounds to ingest): "))


def _parse_cli_args():
    """Parse CLI arguments: batch_size, --created-after, --checkpoint."""
    import sys
    batch_size = _resolve_batch_size()
    created_after = None
    update_checkpoint = False
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--created-after" and i + 1 < len(args):
            created_after = args[i + 1]
        if arg == "--checkpoint":
            update_checkpoint = True
    return batch_size, created_after, update_checkpoint


if __name__ == "__main__":
    _bs, _ca, _ck = _parse_cli_args()
    run(batch_size=_bs, created_after=_ca, update_checkpoint=_ck)
