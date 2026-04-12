#!/usr/bin/env python3
"""
Landing Zone — Hot path, producer

- Live: shows the cymatics pattern in real time; and in every 5s we:
  1. Upload audio to MinIO (audio/hot-path/raw/<uuid>.wav)
  2. Send metadata-only message to Kafka (audio_path, bucket, timestamp, sample_rate)
- Consumer (landing_zone_hot_consumer.py) downloads audio from MinIO and processes → video, image, CSV.

"""

import json
import os
import sys
import threading
import queue
import uuid
from datetime import datetime, timezone
from io import BytesIO

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dotenv import load_dotenv
load_dotenv()

import numpy as np
import sounddevice as sd
import cv2
import time
from scipy.io import wavfile

from shared.minio_helpers import MINIO_BUCKET, MINIO_SECURE, create_minio_client
from shared.cymatics_engine import (
    N_ZONES, build_zones, build_zone_sources, analyse_frame,
    compute_interference, displacement_to_brightness, render_composite,
)

try:
    from minio import Minio
    HAS_MINIO = True
except ImportError:
    HAS_MINIO = False

try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False
    KafkaAdminClient = None
    NewTopic = None
    TopicAlreadyExistsError = Exception

# =============================================================================
#  Config
# =============================================================================
RAW_AUDIO_PREFIX = "audio/hot-path/raw"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_HOT", "landing-zone-events-hot-path")

DURATION = 5
SAMPLE_RATE = 44100
FPS = 30
SIM_RES = 350
LIVE_WINDOW_SEC = 0.25
LIVE_DISPLAY_SIZE = 600


def ensure_minio_raw(client):
    """Ensure bucket and raw audio prefix exist."""
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    try:
        client.stat_object(MINIO_BUCKET, f"{RAW_AUDIO_PREFIX}/.keep")
    except Exception:
        client.put_object(MINIO_BUCKET, f"{RAW_AUDIO_PREFIX}/.keep", BytesIO(b""), 0)


def send_batch_to_kafka(producer, client, batch, device_name="-"):
    """Upload 5s audio to MinIO, then send metadata-only message to Kafka (daemon-safe)."""
    audio = np.asarray(batch, dtype=np.float64).flatten()
    if len(audio) > DURATION * SAMPLE_RATE:
        audio = audio[-int(DURATION * SAMPLE_RATE) :]
    if len(audio) < DURATION * SAMPLE_RATE:
        return
    pk = np.max(np.abs(audio))
    if pk > 1e-6:
        audio /= pk
    wav_buf = BytesIO()
    wavfile.write(wav_buf, SAMPLE_RATE, (audio * 32767).astype(np.int16))
    wav_bytes = wav_buf.getvalue()
    capture_id = str(uuid.uuid4())
    audio_path = f"{RAW_AUDIO_PREFIX}/{capture_id}.wav"
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        client.put_object(
            MINIO_BUCKET,
            audio_path,
            BytesIO(wav_bytes),
            len(wav_bytes),
            "audio/wav",
        )
    except Exception as e:
        print(f"  [hot] MinIO upload failed: {e}")
        return
    payload = {
        "audio_path": audio_path,
        "bucket": MINIO_BUCKET,
        "timestamp": ts,
        "sample_rate": SAMPLE_RATE,
        "duration": DURATION,
        "device": device_name,
    }
    try:
        producer.send(KAFKA_TOPIC, value=payload)
        producer.flush()
    except Exception as e:
        print(f"  [hot] Kafka send failed: {e}")


def run():
    if not HAS_MINIO:
        raise RuntimeError("Install minio: pip install minio")
    if not HAS_KAFKA or not KAFKA_BOOTSTRAP_SERVERS:
        raise RuntimeError("Kafka required: pip install kafka-python and set KAFKA_BOOTSTRAP_SERVERS")

    client = create_minio_client()
    ensure_minio_raw(client)

    servers = KAFKA_BOOTSTRAP_SERVERS.split(",")

    MAX_RETRIES = 5
    RETRY_DELAY = 3

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=servers)
            existing_topics = admin.list_topics()
            if KAFKA_TOPIC not in existing_topics:
                try:
                    admin.create_topics(
                        [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)],
                        validate_only=False,
                    )
                except TopicAlreadyExistsError:
                    pass
            admin.close()
            break
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"  [hot] Topic ensure skipped after {MAX_RETRIES} attempts: {e}")
            else:
                print(f"  [hot] Kafka not ready (attempt {attempt}/{MAX_RETRIES}), retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            break
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Could not connect to Kafka after {MAX_RETRIES} attempts: {e}\n"
                    "  Make sure Kafka is running: docker compose up -d kafka"
                ) from e
            print(f"  [hot producer] Kafka not ready (attempt {attempt}/{MAX_RETRIES}), retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    vz = build_zones(SIM_RES)
    v_sources = build_zone_sources(SIM_RES, vz)
    live_samples = int(SAMPLE_RATE * LIVE_WINDOW_SEC)
    block_size = 1024
    audio_queue = queue.Queue()
    buffer = []

    def callback(indata, _frames, _time, _status):
        if _status:
            print(_status)
        audio_queue.put(indata.copy().flatten())

    try:
        dev_info = sd.query_devices(sd.default.device[0])
        device_name = dev_info.get("name", "-")
    except Exception:
        device_name = "-"

    stream = sd.InputStream(
        samplerate=SAMPLE_RATE,
        channels=1,
        dtype="float32",
        blocksize=block_size,
        callback=callback,
    )
    stream.start()
    print("\n  Hot path: live cymatics + 5s batches → MinIO (raw) + Kafka (metadata)")
    print("   Close the window or press Q to stop.\n")

    win_name = "Cymatics (hot path) — Q to quit"
    cv2.namedWindow(win_name, cv2.WINDOW_NORMAL)
    t0 = time.perf_counter()
    batch_samples = int(DURATION * SAMPLE_RATE)
    max_buf = int(DURATION * SAMPLE_RATE * 2)

    try:
        while True:
            while True:
                try:
                    chunk = audio_queue.get_nowait()
                    buffer.extend(chunk.tolist())
                except queue.Empty:
                    break
            if len(buffer) > max_buf:
                buffer = buffer[-max_buf:]

            if len(buffer) >= batch_samples:
                batch = np.array(buffer[:batch_samples], dtype=np.float32)
                buffer = buffer[batch_samples:]
                threading.Thread(target=send_batch_to_kafka, args=(producer, client, batch, device_name), daemon=True).start()

            live_chunk = buffer[-live_samples:] if len(buffer) >= live_samples else buffer
            if len(live_chunk) < 64:
                frame = np.zeros((LIVE_DISPLAY_SIZE, LIVE_DISPLAY_SIZE, 3), dtype=np.uint8)
                cv2.putText(frame, "Listening...", (20, LIVE_DISPLAY_SIZE // 2), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (200, 200, 200), 2)
            else:
                live_arr = np.array(live_chunk, dtype=np.float64)
                t = (time.perf_counter() - t0) % 100.0
                zone_br = []
                for zi in range(N_ZONES):
                    rms, sfreqs, _ = analyse_frame(live_arr, zi, SAMPLE_RATE)
                    disp_x, disp_y = compute_interference(v_sources[zi], sfreqs, t, SIM_RES)
                    br = displacement_to_brightness(disp_x, disp_y, vz["masks"][zi], SIM_RES, rms, t)
                    zone_br.append(br)
                raw = render_composite(zone_br, vz, t, SIM_RES)
                frame = cv2.resize(raw, (LIVE_DISPLAY_SIZE, LIVE_DISPLAY_SIZE), interpolation=cv2.INTER_CUBIC)
                vd = np.sqrt(
                    (np.mgrid[0:LIVE_DISPLAY_SIZE, 0:LIVE_DISPLAY_SIZE][1] - LIVE_DISPLAY_SIZE / 2.0) ** 2
                    + (np.mgrid[0:LIVE_DISPLAY_SIZE, 0:LIVE_DISPLAY_SIZE][0] - LIVE_DISPLAY_SIZE / 2.0) ** 2
                )
                vig = np.clip(1.0 - (vd / (LIVE_DISPLAY_SIZE * 0.50)) ** 4, 0.0, 1.0)
                for ch in range(3):
                    frame[:, :, ch] = (frame[:, :, ch].astype(np.float64) * vig).astype(np.uint8)
                rms, _, hz = analyse_frame(live_arr, 0, SAMPLE_RATE)
                cv2.putText(frame, f"{hz:.0f} Hz", (10, LIVE_DISPLAY_SIZE - 40), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 0), 3, cv2.LINE_AA)
                cv2.putText(frame, f"{hz:.0f} Hz", (8, LIVE_DISPLAY_SIZE - 38), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2, cv2.LINE_AA)
                cv2.putText(frame, f"RMS: {rms:.3f}", (10, LIVE_DISPLAY_SIZE - 12), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 3, cv2.LINE_AA)
                cv2.putText(frame, f"RMS: {rms:.3f}", (8, LIVE_DISPLAY_SIZE - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (220, 220, 200), 2, cv2.LINE_AA)

            elapsed = time.perf_counter() - t0
            timer_txt = f"{elapsed:.1f} s"
            cv2.putText(frame, timer_txt, (12, 32), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 0), 3, cv2.LINE_AA)
            cv2.putText(frame, timer_txt, (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2, cv2.LINE_AA)

            cv2.imshow(win_name, frame)
            if cv2.waitKey(1) & 0xFF == ord("q"):
                break
    finally:
        stream.stop()
        stream.close()
        cv2.destroyAllWindows()
    print("\n  Hot path stopped.\n")


if __name__ == "__main__":
    run()
