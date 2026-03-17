#!/usr/bin/env python3
"""
Landing Zone — Hot path: audio capture → store in MinIO → metadata to Kafka → consumer.

- Live: shows the cymatics pattern in real time; and in every 5s we:
  1. Upload audio to MinIO (audio/hot-path/raw/<uuid>.wav)
  2. Send metadata-only message to Kafka (audio_path, bucket, timestamp, sample_rate)
- Consumer (landing_zone_hot_consumer.py) downloads audio from MinIO and processes → video, image, CSV.

run the docker compose file to have the needed containers (MinIO, Kafka/zookeeper) running 
and the needed environment variables set.
"""

import json
import os
import threading
import queue
import uuid
from datetime import datetime, timezone
from io import BytesIO

from dotenv import load_dotenv
load_dotenv()

import numpy as np
import sounddevice as sd
import cv2
import time
from scipy.signal import find_peaks
from scipy.io import wavfile

# MinIO (store raw audio only)
try:
    from minio import Minio
    HAS_MINIO = True
except ImportError:
    HAS_MINIO = False

# Kafka
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
#  MinIO + Kafka config
# =============================================================================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "landing-zone")
RAW_AUDIO_PREFIX = "audio/hot-path/raw"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_HOT", "landing-zone-events-hot-path")

# =============================================================================
#  Cymatics settings (same as warm path)
# =============================================================================
DURATION = 5
SAMPLE_RATE = 44100
FPS = 30
NUM_FRAMES = DURATION * FPS
VIDEO_RES = 600
SIM_RES = 350
IMG_RES = 2048
IMG_SIM = 900
LIVE_WINDOW_SEC = 0.25  # window for live display (pattern follows this)
LIVE_DISPLAY_SIZE = 600

ZONE_FRAC = [0.62]
N_SOURCES = [8, 14]
ZONE_SPATIAL_SCALE = [15.0, 22.0]
N_ZONES = 2

# -----------------------------------------------------------------------------
#  Same cymatics logic as warm path (build_zones → render_composite, etc.)
# -----------------------------------------------------------------------------
def build_zones(gs):
    yy, xx = np.mgrid[0:gs, 0:gs]
    c = gs / 2.0
    rr = np.sqrt((xx - c) ** 2 + (yy - c) ** 2)
    theta = np.arctan2(yy - c, xx - c)
    R = gs / 2.0 - 3
    r1 = R * ZONE_FRAC[0]
    masks = [rr < r1, (rr >= r1) & (rr < R)]
    plate = rr < R
    inner_r = [0.0, r1]
    outer_r = [r1, R]
    boundaries = [r1]
    return dict(rr=rr, theta=theta, R=R, c=c, r1=r1, masks=masks, plate=plate,
                inner_r=inner_r, outer_r=outer_r, boundaries=boundaries)


def build_zone_sources(gs, zinfo):
    yy, xx = np.mgrid[0:gs, 0:gs]
    px, py = xx.astype(np.float64), yy.astype(np.float64)
    c = zinfo["c"]
    all_src = []
    for zi in range(N_ZONES):
        r_in, r_out = zinfo["inner_r"][zi], zinfo["outer_r"][zi]
        ns = N_SOURCES[zi]
        pts = []
        if zi == 0:
            for si in range(ns):
                a = 2.0 * np.pi * si / ns
                pts.append((c + (r_out + 1) * np.cos(a), c + (r_out + 1) * np.sin(a)))
        else:
            ns_in, ns_out = ns // 2, ns - ns // 2
            for si in range(ns_in):
                a = 2.0 * np.pi * si / ns_in
                pts.append((c + (r_in - 1) * np.cos(a), c + (r_in - 1) * np.sin(a)))
            for si in range(ns_out):
                a = 2.0 * np.pi * si / ns_out + np.pi / ns_out
                pts.append((c + (r_out + 1) * np.cos(a), c + (r_out + 1) * np.sin(a)))
        src_data = []
        for sx, sy in pts:
            dx, dy = px - sx, py - sy
            d = np.sqrt(dx**2 + dy**2) + 1e-12
            src_data.append((d, dx / d, dy / d))
        all_src.append(src_data)
    return all_src


def analyse_frame(chunk, zone_idx):
    if len(chunk) < 64:
        return 0.0, [(2.5 + ZONE_SPATIAL_SCALE[zone_idx] * 0.5, 1.0)], 0.0
    fft = np.abs(np.fft.rfft(chunk))
    rms = float(np.sqrt(np.mean(chunk**2)))
    fft_norm = fft / (np.max(fft) + 1e-12)
    pidx, _ = find_peaks(fft_norm, height=0.08, distance=5, prominence=0.04)
    if len(pidx) == 0:
        pidx = np.array([np.argmax(fft_norm)])
    pamps = fft_norm[pidx]
    order = np.argsort(pamps)[::-1][:6]
    pidx, pamps = pidx[order], pamps[order]
    fl = len(fft_norm)
    scale = ZONE_SPATIAL_SCALE[zone_idx]
    sfreqs = [(2.5 + np.sqrt(fidx / fl) * scale, float(amp)) for fidx, amp in zip(pidx, pamps)]
    dominant_hz = float(pidx[0] * SAMPLE_RATE / len(chunk))
    return rms, sfreqs, dominant_hz


def compute_interference(src_data, sfreqs, t, gs):
    disp_x = np.zeros((gs, gs), dtype=np.float64)
    disp_y = np.zeros((gs, gs), dtype=np.float64)
    for si, (d, ux, uy) in enumerate(src_data):
        for freq, amp in sfreqs:
            w = 2.0 * np.pi * freq / gs
            arg = w * d + t * freq * 0.3
            wave = (amp * np.sin(arg) if si % 2 == 0 else amp * np.cos(arg)) * 0.6
            disp_x += wave * ux
            disp_y += wave * uy
    return disp_x, disp_y


def displacement_to_brightness(disp_x, disp_y, zmask, gs, rms, t):
    disp_mag = np.sqrt(disp_x**2 + disp_y**2)
    vals = disp_mag[zmask]
    if len(vals) == 0:
        return np.zeros((gs, gs))
    dm = np.mean(vals) + 2.0 * np.std(vals) + 1e-12
    disp_norm = np.clip(disp_mag / dm, 0.0, 1.0)
    nodal = np.exp(-disp_norm * 6.0)
    du8 = (disp_norm * 255).astype(np.uint8)
    sx = cv2.Sobel(du8, cv2.CV_64F, 1, 0, ksize=3)
    sy = cv2.Sobel(du8, cv2.CV_64F, 0, 1, ksize=3)
    smag = np.sqrt(sx**2 + sy**2)
    sm = np.percentile(smag[zmask], 97) + 1e-12 if np.any(zmask) else 1.0
    edges = np.clip(smag / sm, 0.0, 1.0) ** 0.5
    disp_signed = disp_x + disp_y
    zc_r = np.roll(disp_signed, 1, axis=1)
    zc_d = np.roll(disp_signed, 1, axis=0)
    zc = np.clip(
        (disp_signed * zc_r < 0).astype(np.float64) + (disp_signed * zc_d < 0).astype(np.float64),
        0.0, 1.0,
    )
    zc[:, 0] = zc[:, -1] = zc[0, :] = zc[-1, :] = 0
    zc = cv2.GaussianBlur(zc, (3, 3), 0.6)
    brightness = nodal * 0.6 + edges * 0.25 + zc * 0.3
    cell_tex = np.abs(np.sin(disp_norm * np.pi * 4 + t * 3.0)) ** 3 * (0.03 + rms * 0.12)
    brightness += cell_tex
    return brightness


def render_composite(zone_br, zinfo, t, gs):
    rr, R = zinfo["rr"], zinfo["R"]
    br = np.zeros((gs, gs), dtype=np.float64)
    for zi in range(N_ZONES):
        br += zone_br[zi] * zinfo["masks"][zi].astype(np.float64)
    norm_r = rr / R
    br *= np.clip(1.0 - (norm_r * 0.94) ** 3, 0.0, 1.0)
    br[~zinfo["plate"]] = 0.0
    shadow_w = max(3, int(gs / 55))
    shadow = np.zeros((gs, gs), dtype=np.float64)
    for rb in zinfo["boundaries"]:
        d_from_b = np.abs(rr - rb)
        band = np.clip(1.0 - d_from_b / shadow_w, 0.0, 1.0) ** 1.5
        shadow = np.maximum(shadow, band)
    br *= 1.0 - shadow * 0.65
    rim_d = np.abs(rr - R)
    rim_g = np.clip(1.0 - rim_d / 5.0, 0.0, 1.0) ** 3 * 0.2
    rim_g[rr > R + 5] = 0.0
    br = np.clip(br, 0.0, None)
    br = br / (br + 0.55)
    plate_f = zinfo["plate"].astype(np.float64)
    angle = zinfo["theta"]
    deep_r, deep_g, deep_b = 8.0, 18.0, 38.0
    mid_r, mid_g, mid_b = 12.0, 72.0, 108.0
    b_lo = np.clip(br * 2.0, 0.0, 1.0)
    b_mid = np.clip((br - 0.15) * 2.5, 0.0, 1.0)
    b_hi = np.clip((br - 0.38) * 3.2, 0.0, 1.0)
    b_spec = np.clip((br - 0.58) * 4.5, 0.0, 1.0)
    r_ch = deep_r + b_lo * (mid_r - deep_r) + b_mid * 28.0
    g_ch = deep_g + b_lo * (mid_g - deep_g) + b_mid * 85.0
    b_ch = deep_b + b_lo * (mid_b - deep_b) + b_mid * 92.0
    env_lights = [
        (0.0, (0.90, 0.35, 0.50)), (1.05, (0.30, 0.85, 0.45)), (2.09, (0.40, 0.50, 0.95)),
        (3.14, (0.85, 0.70, 0.25)), (4.19, (0.65, 0.30, 0.85)), (5.24, (0.25, 0.80, 0.80)),
    ]
    env_r = np.zeros_like(br)
    env_g = np.zeros_like(br)
    env_b = np.zeros_like(br)
    for light_angle, (lr, lg, lb) in env_lights:
        weight = np.exp(-0.5 * (np.sin(angle - light_angle) * 0.5) ** 2 / 0.12)
        env_r += np.clip(weight, 0.0, 1.0) * lr
        env_g += np.clip(weight, 0.0, 1.0) * lg
        env_b += np.clip(weight, 0.0, 1.0) * lb
    env_total = env_r + env_g + env_b + 1e-12
    env_r /= env_total
    env_g /= env_total
    env_b /= env_total
    reflect = (b_hi * 0.7 + b_spec * 1.0) * plate_f
    r_ch += reflect * env_r * 180.0
    g_ch += reflect * env_g * 180.0
    b_ch += reflect * env_b * 180.0
    r_ch += b_spec * (140.0 + env_r * 60.0)
    g_ch += b_spec * (155.0 + env_g * 50.0)
    b_ch += b_spec * (140.0 + env_b * 40.0)
    flicker = np.sin(br * 20.0 + t * 4.5) * 0.5 + 0.5
    caustic_f = b_hi * flicker * plate_f
    r_ch += caustic_f * env_r * 50.0
    g_ch += caustic_f * env_g * 50.0
    b_ch += caustic_f * env_b * 50.0
    fresnel = np.clip((norm_r - 0.7) * 3.3, 0.0, 1.0) * br * plate_f
    r_ch += fresnel * 22.0
    g_ch += fresnel * 45.0
    b_ch += fresnel * 55.0
    sss = np.clip(br * 2.0 - br**2 * 3.0, 0.0, 0.3) * plate_f
    r_ch += sss * 5.0
    g_ch += sss * 38.0
    b_ch += sss * 30.0
    shadow_dark = 1.0 - shadow * 0.55
    r_ch *= shadow_dark
    g_ch *= shadow_dark
    b_ch *= shadow_dark
    r_ch += rim_g * 12.0
    g_ch += rim_g * 35.0
    b_ch += rim_g * 45.0
    r_ch = np.clip(r_ch, 0, 255)
    g_ch = np.clip(g_ch, 0, 255)
    b_ch = np.clip(b_ch, 0, 255)
    out = ~zinfo["plate"] & (rr > R + 3)
    r_ch[out] = g_ch[out] = b_ch[out] = 0
    return np.stack([b_ch, g_ch, r_ch], axis=-1).astype(np.uint8)


def ensure_minio_raw(client):
    """Ensure bucket and raw audio prefix exist."""
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    try:
        client.stat_object(MINIO_BUCKET, f"{RAW_AUDIO_PREFIX}/.keep")
    except Exception:
        client.put_object(MINIO_BUCKET, f"{RAW_AUDIO_PREFIX}/.keep", BytesIO(b""), 0)


def send_batch_to_kafka(producer, client, batch):
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
    }
    try:
        producer.send(KAFKA_TOPIC, value=payload)
        producer.flush()
    except Exception as e:
        print(f"  [hot] Kafka send failed: {e}")


def run():
    if not HAS_MINIO:
        raise RuntimeError("Install minio: pip install minio")
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY")
    if not HAS_KAFKA or not KAFKA_BOOTSTRAP_SERVERS:
        raise RuntimeError("Kafka required: pip install kafka-python and set KAFKA_BOOTSTRAP_SERVERS")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    ensure_minio_raw(client)

    servers = KAFKA_BOOTSTRAP_SERVERS.split(",")
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
    except Exception as e:
        print(f"  [hot] Topic ensure skipped: {e}")
    producer = KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

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

    stream = sd.InputStream(
        samplerate=SAMPLE_RATE,
        channels=1,
        dtype="float32",
        blocksize=block_size,
        callback=callback,
    )
    stream.start()
    print("\n🔴  Hot path: live cymatics + 5s batches → MinIO (raw) + Kafka (metadata)")
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

            # Every 5s: upload audio to MinIO, send metadata to Kafka in background
            if len(buffer) >= batch_samples:
                batch = np.array(buffer[:batch_samples], dtype=np.float32)
                buffer = buffer[batch_samples:]
                threading.Thread(target=send_batch_to_kafka, args=(producer, client, batch), daemon=True).start()

            live_chunk = buffer[-live_samples:] if len(buffer) >= live_samples else buffer
            if len(live_chunk) < 64:
                frame = np.zeros((LIVE_DISPLAY_SIZE, LIVE_DISPLAY_SIZE, 3), dtype=np.uint8)
                cv2.putText(frame, "Listening...", (20, LIVE_DISPLAY_SIZE // 2), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (200, 200, 200), 2)
            else:
                live_arr = np.array(live_chunk, dtype=np.float64)
                t = (time.perf_counter() - t0) % 100.0
                zone_br = []
                for zi in range(N_ZONES):
                    rms, sfreqs, _ = analyse_frame(live_arr, zi)
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
                rms, _, hz = analyse_frame(live_arr, 0)
                cv2.putText(frame, f"{hz:.0f} Hz", (10, LIVE_DISPLAY_SIZE - 40), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 0), 3, cv2.LINE_AA)
                cv2.putText(frame, f"{hz:.0f} Hz", (8, LIVE_DISPLAY_SIZE - 38), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2, cv2.LINE_AA)
                cv2.putText(frame, f"RMS: {rms:.3f}", (10, LIVE_DISPLAY_SIZE - 12), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 3, cv2.LINE_AA)
                cv2.putText(frame, f"RMS: {rms:.3f}", (8, LIVE_DISPLAY_SIZE - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (220, 220, 200), 2, cv2.LINE_AA)

            # Elapsed timer (seconds)
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
    print("\n✅  Hot path stopped.\n")


if __name__ == "__main__":
    run()
