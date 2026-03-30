#!/usr/bin/env python3
"""
Landing Zone — warm-path.

MinIO: stores audio, image, video, and metadata in the same bucket (landing-zone).
Kafka: after upload, produces an event message.

Run: records 5s audio, generates video/image, uploads to MinIO, then produces to Kafka.

run the docker compose file to have the needed containers (MinIO, Kafka/zookeeper) running 
and the needed environment variables set.
"""

import os
import io
import csv
import json
import tempfile
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import numpy as np
import sounddevice as sd
import cv2
import time
from scipy.signal import find_peaks
from scipy.io import wavfile
from scipy.fft import dct

# MinIO
try:
    from minio import Minio
    HAS_MINIO = True
except ImportError:
    HAS_MINIO = False

import pyarrow as pa
import pyarrow.parquet as pq

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
    TopicAlreadyExistsError = Exception  # no-op if kafka not installed

# =============================================================================
#  MinIO config from env
# =============================================================================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "landing-zone")

# =============================================================================
#  Kafka config from env (transfer to landing-zone in same MinIO)
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
KAFKA_TOPIC_WARM = os.environ.get("KAFKA_TOPIC_WARM", "landing-zone-events-warm-path")

SOURCE = "warm-path"
METADATA_KEY = "metadata/observations.csv"
# Bump when changing recording, simulation, render, or export pipeline.
PROCESSING_VERSION = "1.1.0"

# Preferred column order for new / merged metadata CSV (legacy columns follow).
METADATA_EXTRA_FIELDS = [
    "uuid",
    "source_id",
    "time_recorded",
    "processing_version",
    "duration",
    "spectral_centroid_hz",
    "spectral_bandwidth_hz",
    "spectral_rolloff_hz",
    "spectral_flatness",
    "MFCCs",
    "spectral_entropy",
    "zero_crossing_rate",
    "signal_energy",
    "harmonic_energy_ratio",
    "symmetry_score",
    "pattern_stability_score",
    "image_resolution",
    "video_resolution",
    "audio_size",
    "image_size",
    "video_size",
    "processing_time(seconds)",
    "kafka_topic",
    "device",
    "audio_format",
    "loudness",
]
LEGACY_METADATA_FIELDS = [
    "source",
    "peak_frequency_hz",
    "peak_time_s",
    "peak_amplitude",
    "peak_rms",
    "audio_path",
    "image_path",
    "video_path",
    "all_peak_frequencies_hz",
]

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
#  Cymatics settings (2 zones — same as visualizer)
# =============================================================================
DURATION = 5
SAMPLE_RATE = 44100
FPS = 30
NUM_FRAMES = DURATION * FPS
VIDEO_RES = 600
SIM_RES = 350
IMG_RES = 2048
IMG_SIM = 900

ZONE_FRAC = [0.62]
N_SOURCES = [8, 14]
ZONE_SPATIAL_SCALE = [15.0, 22.0]
N_ZONES = 2


def build_zones(gs):
    """Two concentric zones — seamless, no visible barriers."""
    yy, xx = np.mgrid[0:gs, 0:gs]
    c = gs / 2.0
    rr = np.sqrt((xx - c) ** 2 + (yy - c) ** 2)
    theta = np.arctan2(yy - c, xx - c)
    R = gs / 2.0 - 3
    r1 = R * ZONE_FRAC[0]
    masks = [
        rr < r1,
        (rr >= r1) & (rr < R),
    ]
    plate = rr < R
    inner_r = [0.0, r1]
    outer_r = [r1, R]
    boundaries = [r1]
    return dict(rr=rr, theta=theta, R=R, c=c, r1=r1,
                masks=masks, plate=plate,
                inner_r=inner_r, outer_r=outer_r,
                boundaries=boundaries)


def build_zone_sources(gs, zinfo):
    """Place wave sources around the perimeter of each zone.
    Inner disk — sources on its outer edge.
    Ring zone — sources on both inner and outer edges.
    """
    yy, xx = np.mgrid[0:gs, 0:gs]
    px = xx.astype(np.float64)
    py = yy.astype(np.float64)
    c = zinfo["c"]
    all_src = []
    for zi in range(N_ZONES):
        r_in = zinfo["inner_r"][zi]
        r_out = zinfo["outer_r"][zi]
        ns = N_SOURCES[zi]
        pts = []
        if zi == 0:
            for si in range(ns):
                a = 2.0 * np.pi * si / ns
                pts.append((c + (r_out + 1) * np.cos(a), c + (r_out + 1) * np.sin(a)))
        else:
            ns_in = ns // 2
            ns_out = ns - ns_in
            for si in range(ns_in):
                a = 2.0 * np.pi * si / ns_in
                pts.append((c + (r_in - 1) * np.cos(a), c + (r_in - 1) * np.sin(a)))
            for si in range(ns_out):
                a = 2.0 * np.pi * si / ns_out + np.pi / ns_out
                pts.append((c + (r_out + 1) * np.cos(a), c + (r_out + 1) * np.sin(a)))
        src_data = []
        for sx, sy in pts:
            dx = px - sx
            dy = py - sy
            d = np.sqrt(dx**2 + dy**2) + 1e-12
            src_data.append((d, dx / d, dy / d))
        all_src.append(src_data)
    return all_src


def harmonic_dominant_freq(window_candidates, max_harmonics=5, use_energy_weight=True):
    """
    Pick the true fundamental by counting divisors of window-dominant frequencies.
    E.g. 60 Hz and 120 Hz windows both boost the 60 Hz candidate.
    """
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
    fundamental = max(candidate_counts.keys(), key=lambda k: candidate_counts[k])
    return float(fundamental)


def dominant_freq_hz_for_chunk(chunk, sample_rate=SAMPLE_RATE):
    """Dominant frequency in Hz for a single chunk (FFT + find_peaks, top peak)."""
    if len(chunk) < 64:
        return 0.0
    fft = np.abs(np.fft.rfft(chunk))
    fft_norm = fft / (np.max(fft) + 1e-12)
    pidx, _ = find_peaks(fft_norm, height=0.06, distance=6, prominence=0.04)
    if len(pidx) == 0:
        pidx = np.array([np.argmax(fft_norm)])
    top_idx = pidx[np.argmax(fft_norm[pidx])]
    return float(top_idx * sample_rate / len(chunk))


def analyse_frame(chunk, zone_idx):
    fft = np.abs(np.fft.rfft(chunk))
    rms = float(np.sqrt(np.mean(chunk**2)))
    fft_norm = fft / (np.max(fft) + 1e-12)
    pidx, _ = find_peaks(fft_norm, height=0.08, distance=5, prominence=0.04)
    if len(pidx) == 0:
        pidx = np.array([np.argmax(fft_norm)])
    pamps = fft_norm[pidx]
    order = np.argsort(pamps)[::-1][:6]
    pidx = pidx[order]
    pamps = pamps[order]
    fl = len(fft_norm)
    scale = ZONE_SPATIAL_SCALE[zone_idx]
    sfreqs = [(2.5 + np.sqrt(fidx / fl) * scale, float(amp)) for fidx, amp in zip(pidx, pamps)]
    dominant_hz = float(pidx[0] * SAMPLE_RATE / len(chunk))
    return rms, sfreqs, dominant_hz


def spectral_features_hz(audio, sample_rate=SAMPLE_RATE):
    """Centroid, bandwidth, rolloff (85% energy), flatness, and time-domain energy."""
    x = np.asarray(audio, dtype=np.float64).flatten()
    n = len(x)
    if n < 2:
        z = 0.0
        return z, z, z, z, z
    mag = np.abs(np.fft.rfft(x)) + 1e-20
    freqs = np.fft.rfftfreq(n, 1.0 / sample_rate)
    p = mag ** 2
    s = float(p.sum()) + 1e-20
    centroid = float((freqs * p).sum() / s)
    bandwidth = float(np.sqrt(((freqs - centroid) ** 2 * p).sum() / s))
    cum = np.cumsum(p)
    rolloff_idx = int(np.searchsorted(cum, 0.85 * s))
    rolloff = float(freqs[min(rolloff_idx, len(freqs) - 1)])
    geo = float(np.exp(np.mean(np.log(mag))))
    arith = float(np.mean(mag))
    flatness = float(geo / (arith + 1e-20))
    signal_energy = float(np.sum(x ** 2))
    return centroid, bandwidth, rolloff, flatness, signal_energy


def _hz_to_mel(f):
    return 2595.0 * np.log10(1.0 + np.maximum(f, 0.0) / 700.0)


def _mel_to_hz(m):
    return 700.0 * (10.0 ** (m / 2595.0) - 1.0)


def _mel_filterbank(n_fft, n_mels, sample_rate):
    n_freqs = n_fft // 2 + 1
    mel_min, mel_max = _hz_to_mel(0.0), _hz_to_mel(sample_rate / 2.0)
    mels = np.linspace(mel_min, mel_max, n_mels + 2)
    hz = _mel_to_hz(mels)
    bins = np.floor((n_fft + 1) * hz / sample_rate).astype(int)
    bins = np.clip(bins, 0, n_freqs - 1)
    fbank = np.zeros((n_mels, n_freqs), dtype=np.float64)
    for m in range(1, n_mels + 1):
        left, center, right = int(bins[m - 1]), int(bins[m]), int(bins[m + 1])
        if center > left:
            for k in range(left, center):
                fbank[m - 1, k] = (k - left) / float(center - left)
        if right > center:
            for k in range(center, right):
                fbank[m - 1, k] = (right - k) / float(right - center)
    return fbank


def mfccs_mean_json(audio, sample_rate=SAMPLE_RATE, n_mfcc=13, n_fft=2048, hop_length=None, n_mels=40):
    """Mean MFCC vector over frames; JSON array string for CSV (librosa-style mel + DCT-II)."""
    x = np.asarray(audio, dtype=np.float64).flatten()
    if hop_length is None:
        hop_length = max(n_fft // 4, 1)
    if len(x) < n_fft:
        return json.dumps([0.0] * n_mfcc)
    win = np.hanning(n_fft)
    fbank = _mel_filterbank(n_fft, n_mels, sample_rate)
    n_frames = 1 + (len(x) - n_fft) // hop_length
    if n_frames < 1:
        return json.dumps([0.0] * n_mfcc)
    mfcc_acc = np.zeros(n_mfcc, dtype=np.float64)
    for i in range(n_frames):
        frame = x[i * hop_length : i * hop_length + n_fft] * win
        spec = np.abs(np.fft.rfft(frame, n=n_fft)) ** 2
        mel = np.maximum(np.dot(fbank, spec), 1e-10)
        log_mel = np.log(mel)
        coeffs = dct(log_mel, type=2, norm="ortho")[:n_mfcc]
        mfcc_acc += coeffs
    mean_mfcc = mfcc_acc / n_frames
    return json.dumps([round(float(v), 6) for v in mean_mfcc])


def spectral_entropy_nats(audio, sample_rate=SAMPLE_RATE):
    """Shannon entropy of normalized single-frame power spectrum (full clip FFT), nats."""
    x = np.asarray(audio, dtype=np.float64).flatten()
    n = len(x)
    if n < 2:
        return 0.0
    power = np.abs(np.fft.rfft(x)) ** 2
    p = power / (np.sum(power) + 1e-20)
    p = p[p > 1e-20]
    return float(-np.sum(p * np.log(p)))


def zero_crossing_rate(audio):
    """Fraction of adjacent samples where sign changes (zeros treated as +1)."""
    x = np.asarray(audio, dtype=np.float64).flatten()
    n = len(x)
    if n < 2:
        return 0.0
    signs = np.sign(x)
    signs[signs == 0] = 1.0
    return float(np.sum(np.abs(np.diff(signs)) > 0) / (n - 1))


def loudness_db(audio):
    """RMS loudness in dBFS."""
    rms = float(np.sqrt(np.mean(np.asarray(audio, dtype=np.float64) ** 2)))
    return float(20.0 * np.log10(rms + 1e-20))


def harmonic_energy_ratio(audio, sample_rate, fundamental_hz, max_harmonics=8, half_width_hz=None):
    """Share of magnitude-squared spectrum near integer harmonics of fundamental_hz."""
    x = np.asarray(audio, dtype=np.float64).flatten()
    n = len(x)
    if n < 2 or fundamental_hz <= 0:
        return 0.0
    power = np.abs(np.fft.rfft(x)) ** 2
    freqs = np.fft.rfftfreq(n, 1.0 / sample_rate)
    total = float(power.sum()) + 1e-20
    if half_width_hz is None:
        half_width_hz = max(sample_rate / n * 2.0, 5.0)
    h_sum = 0.0
    for k in range(1, max_harmonics + 1):
        fk = fundamental_hz * k
        if fk >= sample_rate / 2:
            break
        mask = np.abs(freqs - fk) <= half_width_hz
        h_sum += float(power[mask].sum())
    return float(h_sum / total)


def symmetry_score_cymatics(img_bgr):
    """0–1 score from Pearson correlation of grayscale image with its horizontal flip."""
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY).astype(np.float64)
    g = gray.ravel()
    gl = np.fliplr(gray).ravel()
    g = g - g.mean()
    gl = gl - gl.mean()
    denom = (np.linalg.norm(g) * np.linalg.norm(gl)) + 1e-12
    c = float(np.dot(g, gl) / denom)
    return float(np.clip(0.5 * (c + 1.0), 0.0, 1.0))


def pattern_stability_score(frames_bgr, step=3):
    """Mean cosine similarity between grayscale frames spaced by `step` (video stability)."""
    if len(frames_bgr) < 2:
        return 1.0
    corrs = []
    prev = cv2.cvtColor(frames_bgr[0], cv2.COLOR_BGR2GRAY).astype(np.float64).ravel()
    prev = prev / (np.linalg.norm(prev) + 1e-12)
    for i in range(step, len(frames_bgr), step):
        cur = cv2.cvtColor(frames_bgr[i], cv2.COLOR_BGR2GRAY).astype(np.float64).ravel()
        cur = cur / (np.linalg.norm(cur) + 1e-12)
        corrs.append(float(np.dot(prev, cur)))
        prev = cur
    return float(np.mean(corrs)) if corrs else 1.0


def merge_metadata_fieldnames(existing_names, row_keys):
    """Preserve existing column order; append new keys from row not yet present."""
    out = list(existing_names or [])
    seen = set(out)
    for k in METADATA_EXTRA_FIELDS + LEGACY_METADATA_FIELDS:
        if k not in seen:
            seen.add(k)
            out.append(k)
    for k in row_keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


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
    zc[:, 0] = 0; zc[:, -1] = 0; zc[0, :] = 0; zc[-1, :] = 0
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
    env_r /= env_total; env_g /= env_total; env_b /= env_total
    reflect = (b_hi * 0.7 + b_spec * 1.0) * plate_f
    r_ch += reflect * env_r * 180.0; g_ch += reflect * env_g * 180.0; b_ch += reflect * env_b * 180.0
    r_ch += b_spec * (140.0 + env_r * 60.0); g_ch += b_spec * (155.0 + env_g * 50.0); b_ch += b_spec * (140.0 + env_b * 40.0)
    flicker = np.sin(br * 20.0 + t * 4.5) * 0.5 + 0.5
    caustic_f = b_hi * flicker * plate_f
    r_ch += caustic_f * env_r * 50.0; g_ch += caustic_f * env_g * 50.0; b_ch += caustic_f * env_b * 50.0
    fresnel = np.clip((norm_r - 0.7) * 3.3, 0.0, 1.0) * br * plate_f
    r_ch += fresnel * 22.0; g_ch += fresnel * 45.0; b_ch += fresnel * 55.0
    sss = np.clip(br * 2.0 - br**2 * 3.0, 0.0, 0.3) * plate_f
    r_ch += sss * 5.0; g_ch += sss * 38.0; b_ch += sss * 30.0
    shadow_dark = 1.0 - shadow * 0.55
    r_ch *= shadow_dark; g_ch *= shadow_dark; b_ch *= shadow_dark
    r_ch += rim_g * 12.0; g_ch += rim_g * 35.0; b_ch += rim_g * 45.0
    r_ch = np.clip(r_ch, 0, 255); g_ch = np.clip(g_ch, 0, 255); b_ch = np.clip(b_ch, 0, 255)
    out = ~zinfo["plate"] & (rr > R + 3)
    r_ch[out] = g_ch[out] = b_ch[out] = 0
    return np.stack([b_ch, g_ch, r_ch], axis=-1).astype(np.uint8)


class MinIOUploadError(Exception):
    """Raised when MinIO upload or structure setup fails. Avoids partial state confusion."""
    def __init__(self, message, cause=None, bucket=None, key=None, uploaded_keys=None):
        super().__init__(message)
        self.message = message
        self.cause = cause
        self.bucket = bucket
        self.key = key
        self.uploaded_keys = uploaded_keys or []


def _minio_remove_keys(client, bucket, keys):
    """Best-effort remove objects; ignore errors (e.g. key missing)."""
    for key in keys:
        try:
            client.remove_object(bucket, key)
        except Exception:
            pass


def ensure_minio_structure(client):
    """Ensure bucket exists. Folders are implicit via object keys."""
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            print(f"  Created bucket: {MINIO_BUCKET}")
        placeholders = [
            "metadata/.keep",
            "audio/warm-path/.keep",
            "images/warm-path/.keep",
            "videos/warm-path/.keep",
        ]
        for key in placeholders:
            try:
                client.stat_object(MINIO_BUCKET, key)
            except Exception:
                client.put_object(MINIO_BUCKET, key, io.BytesIO(b""), 0)
        print("  Bucket and folder structure ready.")
    except Exception as e:
        raise MinIOUploadError(
            f"MinIO structure setup failed (bucket={MINIO_BUCKET})",
            cause=e,
            bucket=MINIO_BUCKET,
        ) from e


def run():
    if not HAS_MINIO:
        raise RuntimeError("Install minio: pip install minio")

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    ensure_minio_structure(client)

    # --- Record 5 seconds ---
    print("\n🎤  Get ready to record…")
    for i in range(3, 0, -1):
        print(f"   {i}…", flush=True)
        time.sleep(1)
    print("   🔴  RECORDING — make some noise!\n")
    audio = sd.rec(int(DURATION * SAMPLE_RATE), samplerate=SAMPLE_RATE, channels=1, dtype="float32")
    sd.wait()
    audio = audio.flatten()
    print("✅  Recording complete!\n")

    try:
        dev_info = sd.query_devices(sd.default.device[0])
        device_name = dev_info.get("name", "-")
    except Exception:
        device_name = "-"

    pk = np.max(np.abs(audio))
    if pk > 1e-6:
        audio /= pk
    t_proc_start = time.perf_counter()
    samples_per_frame = len(audio) // NUM_FRAMES
    audio_duration_s = float(len(audio) / SAMPLE_RATE)

    # --- Peak detection (harmonic-aware: frequency that appeared most over 5s) ---
    window_len = int(SAMPLE_RATE * 0.25)
    hop = max(1, window_len // 2)
    window_candidates = []
    for off in range(0, len(audio) - window_len, hop):
        chunk = audio[off : off + window_len]
        hz = dominant_freq_hz_for_chunk(chunk)
        freq_bin = int(round(hz))
        energy = float(np.sum(np.abs(np.fft.rfft(chunk)) ** 2))
        window_candidates.append((freq_bin, chunk.copy(), off, energy, hz))
    if not window_candidates:
        raise RuntimeError("No window candidates for peak detection")
    dominant_freq_hz = harmonic_dominant_freq(window_candidates, max_harmonics=4, use_energy_weight=True)
    if dominant_freq_hz <= 0:
        raise RuntimeError("Could not determine dominant frequency")
    dominant_freq_bin = int(round(dominant_freq_hz))
    peak_freq_int = dominant_freq_bin
    best = max(
        (c for c in window_candidates if c[0] > 0 and dominant_freq_bin > 0 and c[0] % dominant_freq_bin == 0),
        key=lambda c: c[3],
        default=window_candidates[0],
    )
    _bin, peak_chunk, best_offset, _energy, _hz = best
    peak_chunk = np.asarray(peak_chunk, dtype=np.float64)
    peak_rms = float(np.sqrt(np.mean(peak_chunk**2)))
    peak_amplitude = float(np.max(np.abs(peak_chunk)))
    peak_time = best_offset / SAMPLE_RATE
    peak_fft = np.abs(np.fft.rfft(peak_chunk))
    peak_fft_norm = peak_fft / (np.max(peak_fft) + 1e-12)
    peaks_idx, _ = find_peaks(peak_fft_norm, height=0.06, distance=6, prominence=0.04)
    if len(peaks_idx) == 0:
        peaks_idx = np.array([np.argmax(peak_fft_norm)])
    peak_amps_arr = peak_fft_norm[peaks_idx]
    top_order = np.argsort(peak_amps_arr)[::-1][:8]
    peaks_idx = peaks_idx[top_order]
    freq_resolution = SAMPLE_RATE / window_len
    peak_hz_values = peaks_idx * freq_resolution

    print(f"   Dominant frequency: {dominant_freq_hz:.1f} Hz")

    sc_hz, sbw_hz, srol_hz, sflat, signal_energy = spectral_features_hz(audio)
    harm_ratio = harmonic_energy_ratio(audio, SAMPLE_RATE, dominant_freq_hz)
    mfccs_json = mfccs_mean_json(audio)
    spec_entropy = spectral_entropy_nats(audio)
    zcr = zero_crossing_rate(audio)

    # --- Build grids ---
    vz = build_zones(SIM_RES)
    v_sources = build_zone_sources(SIM_RES, vz)
    iz = build_zones(IMG_SIM)
    i_sources = build_zone_sources(IMG_SIM, iz)

    # Frame analysis
    frame_data = []
    for fi in range(NUM_FRAMES):
        s = fi * samples_per_frame
        chunk = audio[s : s + samples_per_frame]
        zone_info = [analyse_frame(chunk, zi) for zi in range(N_ZONES)]
        frame_data.append(zone_info)

    # --- Video ---
    vy_g, vx_g = np.mgrid[0:VIDEO_RES, 0:VIDEO_RES]
    vd = np.sqrt((vx_g - VIDEO_RES / 2.0) ** 2 + (vy_g - VIDEO_RES / 2.0) ** 2)
    vig = np.clip(1.0 - (vd / (VIDEO_RES * 0.50)) ** 4, 0.0, 1.0)
    frames = []
    font_vid = cv2.FONT_HERSHEY_SIMPLEX
    for fi in range(NUM_FRAMES):
        t = fi / FPS
        zone_br = []
        for zi in range(N_ZONES):
            rms, sfreqs, _ = frame_data[fi][zi]
            disp_x, disp_y = compute_interference(v_sources[zi], sfreqs, t, SIM_RES)
            br = displacement_to_brightness(disp_x, disp_y, vz["masks"][zi], SIM_RES, rms, t)
            zone_br.append(br)
        raw = render_composite(zone_br, vz, t, SIM_RES)
        raw = cv2.resize(raw, (VIDEO_RES, VIDEO_RES), interpolation=cv2.INTER_CUBIC)
        glow = cv2.GaussianBlur(raw, (0, 0), sigmaX=6)
        raw = cv2.addWeighted(raw, 0.82, glow, 0.28, 0)
        for ch in range(3):
            raw[:, :, ch] = (raw[:, :, ch].astype(np.float64) * vig).astype(np.uint8)
        # Per-frame RMS from analysis + higher-resolution frequency label (0.25s window)
        frame_rms = frame_data[fi][0][0]
        center = int((fi + 0.5) * samples_per_frame)
        win_len = int(SAMPLE_RATE * 0.25)
        start = max(0, center - win_len // 2)
        end = start + win_len
        if end > len(audio):
            end = len(audio)
            start = max(0, end - win_len)
        label_chunk = audio[start:end]
        frame_hz = dominant_freq_hz_for_chunk(label_chunk)
        freq_txt = f"{frame_hz:.0f} Hz"
        rms_txt = f"RMS: {frame_rms:.3f}"
        cv2.putText(raw, freq_txt, (12, VIDEO_RES - 42), font_vid, 0.65, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(raw, freq_txt, (10, VIDEO_RES - 40), font_vid, 0.65, (255, 255, 255), 2, cv2.LINE_AA)
        cv2.putText(raw, rms_txt, (12, VIDEO_RES - 14), font_vid, 0.55, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(raw, rms_txt, (10, VIDEO_RES - 12), font_vid, 0.55, (220, 220, 200), 2, cv2.LINE_AA)
        frames.append(raw)

    cf_len = int(FPS * 0.6)
    for i in range(cf_len):
        alpha = i / cf_len
        idx = NUM_FRAMES - cf_len + i
        frames[idx] = cv2.addWeighted(frames[idx], 1.0 - alpha, frames[i], alpha, 0)

    pattern_stability = pattern_stability_score(frames)

    # --- Image ---
    img_zone_br = []
    for zi in range(N_ZONES):
        _, sfreqs, _ = analyse_frame(peak_chunk, zi)
        disp_x, disp_y = compute_interference(i_sources[zi], sfreqs, peak_time, IMG_SIM)
        br = displacement_to_brightness(disp_x, disp_y, iz["masks"][zi], IMG_SIM, peak_rms, 0.0)
        img_zone_br.append(br)
    raw_img = render_composite(img_zone_br, iz, 0.0, IMG_SIM)
    img = cv2.resize(raw_img, (IMG_RES, IMG_RES), interpolation=cv2.INTER_LANCZOS4)
    glow_i = cv2.GaussianBlur(img, (0, 0), sigmaX=14)
    img = cv2.addWeighted(img, 0.82, glow_i, 0.30, 0)
    ivy, ivx = np.mgrid[0:IMG_RES, 0:IMG_RES]
    ivd = np.sqrt((ivx - IMG_RES / 2.0) ** 2 + (ivy - IMG_RES / 2.0) ** 2)
    ivig = np.clip(1.0 - (ivd / (IMG_RES * 0.50)) ** 4, 0.0, 1.0)
    for ch in range(3):
        img[:, :, ch] = (img[:, :, ch].astype(np.float64) * ivig).astype(np.uint8)

    # --- Labels (same as visualizer: CYMATICS title, freq, amp/rms, inner/outer zones) ---
    font = cv2.FONT_HERSHEY_SIMPLEX
    freq_label = f"{dominant_freq_hz:.0f} Hz"
    ts0 = cv2.getTextSize(freq_label, font, 2.0, 3)[0]
    tx = (IMG_RES - ts0[0]) // 2
    ty = IMG_RES - 80
    cv2.putText(img, freq_label, (tx + 3, ty + 3), font, 2.0, (0, 0, 0), 5, cv2.LINE_AA)
    cv2.putText(img, freq_label, (tx, ty), font, 2.0, (255, 255, 255), 3, cv2.LINE_AA)

    all_hz = "  |  ".join([f"{hz:.0f} Hz" for hz in peak_hz_values])
    ts1 = cv2.getTextSize(all_hz, font, 0.7, 2)[0]
    tx2 = (IMG_RES - ts1[0]) // 2
    cv2.putText(img, all_hz, (tx2 + 2, IMG_RES - 33), font, 0.7, (0, 0, 0), 3, cv2.LINE_AA)
    cv2.putText(img, all_hz, (tx2, IMG_RES - 35), font, 0.7, (220, 210, 180), 2, cv2.LINE_AA)

    title = "CYMATICS"
    ts2 = cv2.getTextSize(title, font, 1.2, 2)[0]
    ttx = (IMG_RES - ts2[0]) // 2
    cv2.putText(img, title, (ttx + 2, 62), font, 1.2, (0, 0, 0), 4, cv2.LINE_AA)
    cv2.putText(img, title, (ttx, 60), font, 1.2, (240, 230, 200), 2, cv2.LINE_AA)

    amp_label = f"Amplitude: {peak_amplitude:.3f}  |  RMS: {peak_rms:.3f}"
    ts3 = cv2.getTextSize(amp_label, font, 0.7, 2)[0]
    atx = (IMG_RES - ts3[0]) // 2
    cv2.putText(img, amp_label, (atx + 2, 97), font, 0.7, (0, 0, 0), 3, cv2.LINE_AA)
    cv2.putText(img, amp_label, (atx, 95), font, 0.7, (220, 210, 180), 2, cv2.LINE_AA)

    for zi, label in enumerate(["Center", "Outer"]):
        scale = ZONE_SPATIAL_SCALE[zi]
        ztxt = f"{label}: {N_SOURCES[zi]} src, k={scale:.0f}"
        y_pos = 140 + zi * 35
        cv2.putText(img, ztxt, (52, y_pos + 2), font, 0.55, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(img, ztxt, (50, y_pos), font, 0.55, (180, 220, 240), 2, cv2.LINE_AA)

    symmetry_score = symmetry_score_cymatics(img)
    image_resolution = f"{IMG_RES}x{IMG_RES}"
    video_resolution = f"{VIDEO_RES}x{VIDEO_RES}"

    # --- Save to temp files and upload (with error handling to avoid partial state) ---
    obs_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    audio_path = f"audio/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.wav"
    image_path = f"images/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.png"
    video_path = f"videos/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.mp4"
    # Track observation-specific keys for rollback only (never remove shared METADATA_KEY)
    uploaded_artifact_keys = []

    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as fa:
            wav_path = fa.name
        wavfile.write(wav_path, SAMPLE_RATE, (audio * 32767).astype(np.int16))
        audio_size = os.path.getsize(wav_path)
        with open(wav_path, "rb") as f:
            client.put_object(MINIO_BUCKET, audio_path, f, audio_size, "audio/wav")
        os.remove(wav_path)
        uploaded_artifact_keys.append(audio_path)
        print(f"  Uploaded: {audio_path}")

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as fi:
            img_path = fi.name
        cv2.imwrite(img_path, img)
        image_size = os.path.getsize(img_path)
        with open(img_path, "rb") as f:
            client.put_object(MINIO_BUCKET, image_path, f, image_size, "image/png")
        os.remove(img_path)
        uploaded_artifact_keys.append(image_path)
        print(f"  Uploaded: {image_path}")

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as fv:
            video_path_tmp = fv.name
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(video_path_tmp, fourcc, FPS, (VIDEO_RES, VIDEO_RES))
        for f in frames:
            writer.write(f)
        writer.release()
        video_size = os.path.getsize(video_path_tmp)
        with open(video_path_tmp, "rb") as f:
            client.put_object(MINIO_BUCKET, video_path, f, video_size, "video/mp4")
        os.remove(video_path_tmp)
        uploaded_artifact_keys.append(video_path)
        print(f"  Uploaded: {video_path}")

        # --- Metadata ---
        all_hz = "|".join(f"{hz:.0f}" for hz in peak_hz_values)
        processing_time_s = time.perf_counter() - t_proc_start
        row = {
            "uuid": obs_id,
            "source_id": obs_id,
            "time_recorded": ts,
            "processing_version": PROCESSING_VERSION,
            "duration": audio_duration_s,
            "spectral_centroid_hz": sc_hz,
            "spectral_bandwidth_hz": sbw_hz,
            "spectral_rolloff_hz": srol_hz,
            "spectral_flatness": sflat,
            "MFCCs": mfccs_json,
            "spectral_entropy": spec_entropy,
            "zero_crossing_rate": zcr,
            "signal_energy": signal_energy,
            "harmonic_energy_ratio": harm_ratio,
            "symmetry_score": symmetry_score,
            "pattern_stability_score": pattern_stability,
            "image_resolution": image_resolution,
            "video_resolution": video_resolution,
            "audio_size": audio_size,
            "image_size": image_size,
            "video_size": video_size,
            "processing_time(seconds)": processing_time_s,
            "kafka_topic": KAFKA_TOPIC_WARM,
            "device": device_name,
            "audio_format": "wav",
            "loudness": loudness_db(audio),
            "source": SOURCE,
            "peak_frequency_hz": dominant_freq_hz,
            "peak_time_s": peak_time,
            "peak_amplitude": peak_amplitude,
            "peak_rms": peak_rms,
            "audio_path": audio_path,
            "image_path": image_path,
            "video_path": video_path,
            "all_peak_frequencies_hz": all_hz,
        }

        try:
            resp = client.get_object(MINIO_BUCKET, METADATA_KEY)
            existing = resp.read().decode("utf-8")
            resp.close()
            resp.release_conn()
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
        print(f"  Updated: {METADATA_KEY}")

        # Update Parquet alongside CSV
        update_parquet(client, MINIO_BUCKET, [row])

    except Exception as e:
        if uploaded_artifact_keys:
            _minio_remove_keys(client, MINIO_BUCKET, uploaded_artifact_keys)
            raise MinIOUploadError(
                f"MinIO upload failed after uploading {uploaded_artifact_keys!r}; rolled back. Last error: {e}",
                cause=e,
                bucket=MINIO_BUCKET,
                key=uploaded_artifact_keys[-1] if uploaded_artifact_keys else None,
                uploaded_keys=list(uploaded_artifact_keys),
            ) from e
        raise MinIOUploadError(
            f"MinIO upload failed: {e}",
            cause=e,
            bucket=MINIO_BUCKET,
        ) from e

    # --- Notify via Kafka for transfer to landing-zone (same MinIO) ---
    if HAS_KAFKA and KAFKA_BOOTSTRAP_SERVERS:
        try:
            servers = KAFKA_BOOTSTRAP_SERVERS.split(",")
            # Ensure topic exists (create only if missing; kafka-python >= 2.0 returns response object, no .items())
            admin = KafkaAdminClient(bootstrap_servers=servers)
            existing_topics = admin.list_topics()
            if KAFKA_TOPIC_WARM not in existing_topics:
                try:
                    admin.create_topics(
                        [NewTopic(name=KAFKA_TOPIC_WARM, num_partitions=1, replication_factor=1)],
                        validate_only=False,
                    )
                except TopicAlreadyExistsError:
                    pass
            admin.close()

            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            payload = {
                "observation_id": obs_id,
                "bucket": MINIO_BUCKET,
                "timestamp": ts,
                "source": SOURCE,
                "peak_frequency_hz": dominant_freq_hz,
                "peak_time_s": peak_time,
                "peak_amplitude": peak_amplitude,
                "peak_rms": peak_rms,
                "audio_path": audio_path,
                "image_path": image_path,
                "video_path": video_path,
                "all_peak_frequencies_hz": all_hz,
            }
            producer.send(KAFKA_TOPIC_WARM, value=payload)
            producer.flush()
            producer.close()
            print(f"  Produced to Kafka: {KAFKA_TOPIC_WARM}")
        except Exception as e:
            print(f"  Kafka produce skipped: {e}")
    else:
        if not HAS_KAFKA:
            print("  Kafka skipped (install kafka-python to enable).")
        elif not KAFKA_BOOTSTRAP_SERVERS:
            print("  Kafka skipped (KAFKA_BOOTSTRAP_SERVERS not set).")

    print("\n✅  Observation stored in MinIO.")
    print(f"   Observation ID: {obs_id}")
    print(f"   Peak: {dominant_freq_hz:.1f} Hz  |  source: {SOURCE}")
    print(f"   Metadata: {METADATA_KEY}\n")


if __name__ == "__main__":
    run()
