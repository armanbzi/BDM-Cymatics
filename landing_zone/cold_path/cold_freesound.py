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
import sys
import io
import subprocess
import tempfile
import random
import time
import uuid
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
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
    MINIO_BUCKET, create_minio_client, ensure_bucket,
    METADATA_KEY, LANDING_METADATA_FIELDS,
    update_parquet, append_rows_to_csv, load_existing_source_ids,
)
from shared.freq_detection import detect_peak_freq

# =============================================================================
#  Config
# =============================================================================
FREESOUND_API_KEY = os.environ.get("FREESOUND_API_KEY", "")

BATCH_SIZE = None

SOURCE = "Freesound"
SAMPLE_RATE = 44100

QUERIES = [
    "rain", "church bells", "crickets",
    "thunder", "water drops", "sea waves",
    "wind", "singing bowl", "tuning fork",
    "bird song", "cat purring", "dog bark",
    "waterfall", "ocean", "forest",
]

COLD_EXTRA_FIELDS = ["tags", "description", "category"]
PRIORITY_FIELDS = LANDING_METADATA_FIELDS + COLD_EXTRA_FIELDS

# HTTP tuning (Freesound can be slow; defaults are generous)
_FS_CONNECT = float(os.environ.get("FREESOUND_CONNECT_TIMEOUT", "20"))
_FS_READ_SEARCH = float(os.environ.get("FREESOUND_READ_TIMEOUT", "120"))
_FS_READ_DOWNLOAD = float(os.environ.get("FREESOUND_DOWNLOAD_TIMEOUT", "300"))
_FS_MAX_RETRIES = int(os.environ.get("FREESOUND_MAX_RETRIES", "5"))
# Transient HTTP statuses (empty body on 504 is common behind proxies)
_RETRY_HTTP = frozenset({429, 502, 503, 504})


# =============================================================================
#  Freesound HTTP (retries: timeouts, connection errors, gateway/rate-limit)
# =============================================================================
def _freesound_get(url, *, params=None, timeout, label="Freesound",
                   allow_redirects=True):
    """GET with retries on timeouts, connection errors, and transient HTTP codes."""
    last_resp = None
    last_exc = None
    for attempt in range(1, _FS_MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url, params=params, timeout=timeout,
                allow_redirects=allow_redirects,
            )
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ConnectionError,
        ) as e:
            last_exc = e
            if attempt >= _FS_MAX_RETRIES:
                break
            wait = min(2 ** attempt, 60)
            print(f"  {label}: network error ({e.__class__.__name__}), "
                  f"attempt {attempt}/{_FS_MAX_RETRIES} — retrying in {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code in _RETRY_HTTP:
            last_resp = resp
            try:
                resp.content  # drain body so the connection can be reused
            except Exception:
                pass
            if attempt >= _FS_MAX_RETRIES:
                return resp
            wait = min(2 ** attempt, 60)
            if resp.status_code == 429:
                wait = max(wait, 30)
            print(f"  {label}: HTTP {resp.status_code} — retrying in {wait}s "
                  f"(attempt {attempt}/{_FS_MAX_RETRIES})...")
            time.sleep(wait)
            continue

        return resp

    if last_resp is not None:
        return last_resp
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{label}: request failed after {_FS_MAX_RETRIES} attempts")


def _parse_freesound_response(resp):
    """Parse JSON; raise RuntimeError on HTTP error or empty/non-JSON body."""
    try:
        data = resp.json()
    except (ValueError, requests.exceptions.JSONDecodeError) as e:
        preview = (resp.text or "").strip()[:300] or "(empty body)"
        sc = resp.status_code
        if sc in _RETRY_HTTP or sc >= 500:
            raise RuntimeError(
                f"Freesound gateway/server error (HTTP {sc}); body not JSON: {preview}. "
                "The service may be overloaded — try again later, or raise "
                "FREESOUND_MAX_RETRIES / FREESOUND_READ_TIMEOUT."
            ) from e
        raise RuntimeError(
            f"Freesound API returned non-JSON (HTTP {sc}): {preview}"
        ) from e
    if not resp.ok:
        detail = data.get("detail", data)
        raise RuntimeError(
            f"Freesound API error (HTTP {resp.status_code}): {detail}"
        )
    return data


# =============================================================================
#  Freesound helpers
# =============================================================================
FREESOUND_SEARCH_FIELDS = (
    "id,name,tags,description,created,type,filesize,duration,"
    "geotag,channels,samplerate,bitrate,bitdepth,license,username,"
    "previews,download"
)


def search_freesound(query, page_size=15, page=1, created_after=None):
    """Return list of sound dicts for ≤10 s sounds matching *query*."""
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
    t = (_FS_CONNECT, _FS_READ_SEARCH)
    resp = _freesound_get(url, params=params, timeout=t, label="Search")
    data = _parse_freesound_response(resp)
    return data.get("results", [])


def download_sound_bytes(sound):
    """Download original audio; fall back to high-quality preview."""
    dl_timeout = (_FS_CONNECT, _FS_READ_DOWNLOAD)
    dl_url = sound.get("download")
    if dl_url:
        r = _freesound_get(
            dl_url,
            params={"token": FREESOUND_API_KEY},
            timeout=dl_timeout,
            label="Download",
            allow_redirects=True,
        )
        if r.status_code == 200 and len(r.content) > 100:
            ext = sound.get("type", "wav")
            return r.content, ext
    previews = sound.get("previews", {})
    for key in ("preview-hq-mp3", "preview-lq-mp3"):
        url = previews.get(key)
        if url:
            r = _freesound_get(url, params=None, timeout=dl_timeout, label="Preview")
            if r.status_code == 200 and len(r.content) > 100:
                return r.content, "mp3"
    return None, None


_ALLOWED_EXTENSIONS = frozenset({"wav", "mp3", "ogg", "flac", "aiff", "aac", "m4a"})


def decode_to_numpy(audio_bytes, ext="wav"):
    """Decode audio bytes to mono float64 at SAMPLE_RATE via ffmpeg."""
    ext = ext.lower().strip(".")
    if ext not in _ALLOWED_EXTENSIONS:
        raise ValueError(f"Unsupported audio extension: {ext}")
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
#  MinIO helpers (Freesound-specific)
# =============================================================================
LAST_INGESTION_KEY = "metadata/freesound_last_ingestion.txt"


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
#  Main
# =============================================================================
def run(batch_size=10, created_after=None, update_checkpoint=False):
    """Run cold-path Freesound ingestion."""
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
        client = create_minio_client()
        ensure_bucket(client, MINIO_BUCKET, ["metadata/.keep", f"audio/{SOURCE}/.keep"])
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

    existing_ids = load_existing_source_ids(client, MINIO_BUCKET)
    if existing_ids:
        print(f"  Found {len(existing_ids)} existing records in metadata — duplicates will be skipped.")

    try:
        search_freesound(QUERIES[0], page_size=1, page=1, created_after=created_after)
    except RuntimeError:
        raise
    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Cannot reach Freesound API after {_FS_MAX_RETRIES} attempts: {e}\n"
            "  Check your network or try again later. Optional env: "
            "FREESOUND_CONNECT_TIMEOUT, FREESOUND_READ_TIMEOUT, FREESOUND_MAX_RETRIES."
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

    append_rows_to_csv(client, MINIO_BUCKET, rows, PRIORITY_FIELDS)
    update_parquet(client, MINIO_BUCKET, rows)

    if update_checkpoint:
        write_last_ingestion_time(client, run_start_ts)
        print(f"  Checkpoint updated: {LAST_INGESTION_KEY} → {run_start_ts}")

    print("\n  Cold-path ingestion complete.")
    print(f"   Batch: {len(rows)} sounds processed and stored.")
    print(f"   Metadata: {METADATA_KEY}\n")


def _resolve_batch_size():
    """CLI argv, module-level BATCH_SIZE, or interactive prompt."""
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
