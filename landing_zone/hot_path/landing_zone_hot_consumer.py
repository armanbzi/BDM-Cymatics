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
import sys
import io
import json
import time
import uuid
from io import BytesIO
from datetime import datetime, timezone

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dotenv import load_dotenv
load_dotenv()

import numpy as np
from scipy.io import wavfile

from minio import Minio
from minio.commonconfig import CopySource

from shared.minio_helpers import (
    MINIO_BUCKET, create_minio_client, ensure_bucket,
    METADATA_KEY, update_parquet, append_rows_to_csv,
)
from shared.freq_detection import detect_peak_freq

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
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
KAFKA_TOPIC_HOT = os.environ.get("KAFKA_TOPIC_HOT", "landing-zone-events-hot-path")

SOURCE = "hot-path"
SAMPLE_RATE = 44100


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

    append_rows_to_csv(client, MINIO_BUCKET, [row])
    update_parquet(client, MINIO_BUCKET, [row])

    print(f"  [hot] {peak_freq_int} Hz  obs_id={obs_id[:8]}...  → {audio_path}")
    return True


# =============================================================================
#  Main consumer loop
# =============================================================================
def run():
    if not HAS_KAFKA:
        raise RuntimeError("Install kafka-python: pip install kafka-python")

    client = create_minio_client()
    ensure_bucket(client, MINIO_BUCKET, ["metadata/.keep", "audio/hot-path/.keep"])

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
