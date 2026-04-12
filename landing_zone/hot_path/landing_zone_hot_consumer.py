#!/usr/bin/env python3
"""
Landing Zone — hot-path consumer.

Consumes Kafka messages (audio_path, bucket, timestamp, sample_rate, device),
downloads raw audio from MinIO, detects peak frequency (harmonic-aware),
moves audio from  audio/hot-path/raw/<uuid>.wav
            to    audio/hot-path/<peak_freq>/<uuid>-<peak_freq>.wav,
deletes the raw copy, and appends a metadata row to CSV + Parquet.

Cymatics generation, spectral features, happen
later in the trusted-zone.
"""

import os
import io
import csv
import json
import time
import uuid
from collections import Counter
from io import BytesIO
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

import numpy as np
from scipy.signal import find_peaks
from scipy.io import wavfile

from minio import Minio
from minio.commonconfig import CopySource

import pyarrow as pa
import pyarrow.parquet as pq

try:
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition, OffsetAndMetadata
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False
    TopicPartition = None
    OffsetAndMetadata = None

# =============================================================================
#  Config
# =============================================================================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "landing-zone")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
KAFKA_TOPIC_HOT = os.environ.get("KAFKA_TOPIC_HOT", "landing-zone-events-hot-path")

SOURCE = "hot-path"
METADATA_KEY = "metadata/observations.csv"
SAMPLE_RATE = 44100

METADATA_FIELDS = [
    "uuid", "source_id", "time_recorded/added", "duration",
    "audio_size", "audio_path", "audio_format", "source", "peak_frequency_hz",
]

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
#  CSV helpers
# =============================================================================
def merge_metadata_fieldnames(existing_names, row_keys):
    out = list(existing_names or [])
    seen = set(out)
    for k in METADATA_FIELDS:
        if k not in seen:
            seen.add(k)
            out.append(k)
    for k in row_keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


# =============================================================================
#  Peak frequency detection (harmonic selection — same as warm path)
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
#  MinIO helpers
# =============================================================================
def ensure_minio_structure(client):
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            print(f"  Created bucket: {MINIO_BUCKET}")
        placeholders = [
            "metadata/.keep",
            "audio/hot-path/.keep",
        ]
        for key in placeholders:
            try:
                client.stat_object(MINIO_BUCKET, key)
            except Exception:
                client.put_object(MINIO_BUCKET, key, io.BytesIO(b""), 0)
        print("  Bucket and folder structure ready.")
    except Exception as e:
        raise RuntimeError(f"MinIO structure setup failed: {e}") from e


# =============================================================================
#  Message processing
# =============================================================================
def process_message(client, audio, raw_audio_path, raw_bucket, sample_rate):
    """Download audio, detect peak freq, move to structured path, write metadata."""
    audio = np.asarray(audio, dtype=np.float64).flatten()
    if len(audio) < sample_rate:
        return False

    peak_freq = detect_peak_freq(audio, sample_rate)
    if peak_freq <= 0:
        return False
    peak_freq_int = int(round(peak_freq))

    obs_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    audio_path = f"audio/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.wav"
    audio_duration_s = float(len(audio) / sample_rate)

    try:
        client.copy_object(
            MINIO_BUCKET,
            audio_path,
            CopySource(raw_bucket, raw_audio_path),
        )
        client.remove_object(raw_bucket, raw_audio_path)
        audio_size = int(client.stat_object(MINIO_BUCKET, audio_path).size)
    except Exception:
        wav_buf = BytesIO()
        wavfile.write(wav_buf, sample_rate, (audio * 32767).astype(np.int16))
        audio_size = wav_buf.getbuffer().nbytes
        wav_buf.seek(0)
        client.put_object(MINIO_BUCKET, audio_path, wav_buf, audio_size, "audio/wav")
        try:
            client.remove_object(raw_bucket, raw_audio_path)
        except Exception:
            pass

    row = {
        "uuid": obs_id,
        "source_id": obs_id,
        "time_recorded/added": ts,
        "duration": audio_duration_s,
        "audio_size": audio_size,
        "audio_path": audio_path,
        "audio_format": "wav",
        "source": SOURCE,
        "peak_frequency_hz": peak_freq,
    }

    try:
        resp = client.get_object(MINIO_BUCKET, METADATA_KEY)
        existing = resp.read().decode("utf-8")
        resp.close(); resp.release_conn()
    except Exception:
        existing = None

    buf = io.StringIO()
    if existing:
        reader = csv.DictReader(io.StringIO(existing))
        fieldnames = merge_metadata_fieldnames(reader.fieldnames, row.keys())
        rows_list = list(reader)
    else:
        fieldnames = merge_metadata_fieldnames([], row.keys())
        rows_list = []

    for prev in rows_list:
        for fn in fieldnames:
            if fn not in prev:
                prev[fn] = ""

    rows_list.append(row)
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_list)
    csv_bytes = buf.getvalue().encode("utf-8")
    client.put_object(MINIO_BUCKET, METADATA_KEY, io.BytesIO(csv_bytes), len(csv_bytes), "text/csv")

    update_parquet(client, MINIO_BUCKET, [row])

    print(f"  [hot] {peak_freq_int} Hz  obs_id={obs_id[:8]}...  → {audio_path}")
    return True


# =============================================================================
#  Main consumer loop
# =============================================================================
def run():
    if not HAS_KAFKA:
        raise RuntimeError("Install kafka-python: pip install kafka-python")
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    ensure_minio_structure(client)

    servers = KAFKA_BOOTSTRAP_SERVERS.split(",")

    MAX_RETRIES = 5
    RETRY_DELAY = 3
    consumer = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_HOT,
                bootstrap_servers=servers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id="landing-zone-hot-consumer",
            )
            break
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Could not connect to Kafka after {MAX_RETRIES} attempts: {e}\n"
                    "  Make sure Kafka is running: docker compose up -d kafka"
                ) from e
            print(f"  [hot consumer] Kafka not ready (attempt {attempt}/{MAX_RETRIES}), retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
    processed_raw_paths = set()
    print(f"\n  Hot-path consumer: topic={KAFKA_TOPIC_HOT} (manual commit)\n")

    def commit_message(msg):
        try:
            tp = TopicPartition(msg.topic, msg.partition)
            next_offset = int(msg.offset) + 1
            consumer.commit(offsets={
                tp: OffsetAndMetadata(next_offset, "", -1)
            })
        except Exception as e:
            print(f"  [hot consumer] commit failed: {e}")

    for message in consumer:
        try:
            value = message.value
            if not value or "audio_path" not in value or "bucket" not in value:
                commit_message(message)
                continue

            bucket = value["bucket"]
            raw_path = value["audio_path"]
            if raw_path in processed_raw_paths:
                commit_message(message)
                continue

            try:
                resp = client.get_object(bucket, raw_path)
                wav_bytes = resp.read()
                resp.close(); resp.release_conn()
            except Exception:
                processed_raw_paths.add(raw_path)
                commit_message(message)
                continue

            sr_msg = int(value.get("sample_rate", SAMPLE_RATE))
            sr, data = wavfile.read(BytesIO(wav_bytes))
            if data.dtype == np.int16:
                audio = data.astype(np.float32) / 32768.0
            else:
                audio = data.astype(np.float32).flatten()
            if audio.ndim > 1:
                audio = audio.mean(axis=1)

            ok = process_message(client, audio, raw_audio_path=raw_path, raw_bucket=bucket, sample_rate=sr)
            if ok:
                processed_raw_paths.add(raw_path)
                commit_message(message)
        except Exception as e:
            print(f"  [hot consumer] message error: {e}")


if __name__ == "__main__":
    run()
