#!/usr/bin/env python3
"""
Consumes metadata messages from Kafka (audio_path, bucket, timestamp, sample_rate),
downloads audio from MinIO using the path from kafka messages, processes cymatics, 
moves audio from raw folder to hot-path/<peak_freq>/<uuid>.wav, and removes it from raw fokder,
uploads image/video to MinIO, writes CSV with correct audio path. Kafka carries only references.

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
from io import BytesIO
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import numpy as np
import cv2
from scipy.signal import find_peaks
from scipy.io import wavfile

from minio import Minio
from minio.commonconfig import CopySource

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

# =============================================================================
#  Cymatics settings
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


def harmonic_dominant_freq(window_candidates, max_harmonics=5, use_energy_weight=True):
    """
    Pick the true fundamental by counting divisors of window-dominant frequencies.
    E.g. 60 Hz and 120 Hz windows both boost the 60 Hz candidate, so 60 wins over 120.
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
    """Return the dominant frequency in Hz for a single chunk (FFT + find_peaks, top peak)."""
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


class MinIOUploadError(Exception):
    def __init__(self, message, cause=None, bucket=None, key=None, uploaded_keys=None):
        super().__init__(message)
        self.message = message
        self.cause = cause
        self.bucket = bucket
        self.key = key
        self.uploaded_keys = uploaded_keys or []


def _minio_remove_keys(client, bucket, keys):
    for key in keys:
        try:
            client.remove_object(bucket, key)
        except Exception:
            pass


def load_processed_raw_paths(client):
    """Load set of raw audio_path values already recorded in metadata CSV (for deduplication)."""
    out = set()
    try:
        resp = client.get_object(MINIO_BUCKET, METADATA_KEY)
        raw = resp.read().decode("utf-8")
        resp.close()
        resp.release_conn()
    except Exception:
        return out
    reader = csv.DictReader(io.StringIO(raw))
    for row in reader:
        p = (row or {}).get("raw_audio_path", "").strip()
        if p:
            out.add(p)
    return out


def ensure_minio_structure(client):
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            print(f"  Created bucket: {MINIO_BUCKET}")
        placeholders = [
            "metadata/.keep",
            "audio/warm-path/.keep",
            "images/warm-path/.keep",
            "videos/warm-path/.keep",
            "audio/hot-path/.keep",
            "images/hot-path/.keep",
            "videos/hot-path/.keep",
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


def process_batch(client, audio, raw_audio_path=None, raw_bucket=None):
    """Process one 5s batch: peak detection, video, image, MinIO upload, CSV.
    If raw_audio_path and raw_bucket are set, move audio from raw to hot-path/<peak_freq>/<obs_id>-<peak_freq>.wav
    (copy then delete; if copy fails, re-upload from memory and delete raw). CSV always gets the final path.
    """
    if len(audio) < SAMPLE_RATE:
        return False
    audio = np.asarray(audio, dtype=np.float64).flatten()
    if len(audio) > DURATION * SAMPLE_RATE:
        audio = audio[-int(DURATION * SAMPLE_RATE) :]
    elif len(audio) < DURATION * SAMPLE_RATE:
        return False
    pk = np.max(np.abs(audio))
    if pk > 1e-6:
        audio /= pk
    samples_per_frame = len(audio) // NUM_FRAMES

    # Select peak = frequency that was dominant for the longest time over the 5s
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
        return False
    # Harmonic-aware: pick fundamental that gets most (energy-weighted) support from divisors of window peaks
    dominant_freq_hz = harmonic_dominant_freq(window_candidates, max_harmonics=4, use_energy_weight=True)
    if dominant_freq_hz <= 0:
        return False
    dominant_freq_bin = int(round(dominant_freq_hz))
    peak_freq_int = dominant_freq_bin
    # Best window = highest energy among windows whose peak is this fundamental or a harmonic of it
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
    # Top peak harmonics from this chunk for display / all_peak_frequencies_hz
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

    vz = build_zones(SIM_RES)
    v_sources = build_zone_sources(SIM_RES, vz)
    iz = build_zones(IMG_SIM)
    i_sources = build_zone_sources(IMG_SIM, iz)

    frame_data = []
    for fi in range(NUM_FRAMES):
        s = fi * samples_per_frame
        chunk = audio[s : s + samples_per_frame]
        zone_info = [analyse_frame(chunk, zi) for zi in range(N_ZONES)]
        frame_data.append(zone_info)

    vd = np.sqrt(
        (np.mgrid[0:VIDEO_RES, 0:VIDEO_RES][1] - VIDEO_RES / 2.0) ** 2
        + (np.mgrid[0:VIDEO_RES, 0:VIDEO_RES][0] - VIDEO_RES / 2.0) ** 2
    )
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
    font = cv2.FONT_HERSHEY_SIMPLEX
    freq_label = f"{dominant_freq_hz:.0f} Hz"
    ts0 = cv2.getTextSize(freq_label, font, 2.0, 3)[0]
    tx = (IMG_RES - ts0[0]) // 2
    ty = IMG_RES - 80
    cv2.putText(img, freq_label, (tx + 3, ty + 3), font, 2.0, (0, 0, 0), 5, cv2.LINE_AA)
    cv2.putText(img, freq_label, (tx, ty), font, 2.0, (255, 255, 255), 3, cv2.LINE_AA)
    all_hz_str = "  |  ".join([f"{hz:.0f} Hz" for hz in peak_hz_values])
    ts1 = cv2.getTextSize(all_hz_str, font, 0.7, 2)[0]
    tx2 = (IMG_RES - ts1[0]) // 2
    cv2.putText(img, all_hz_str, (tx2 + 2, IMG_RES - 33), font, 0.7, (0, 0, 0), 3, cv2.LINE_AA)
    cv2.putText(img, all_hz_str, (tx2, IMG_RES - 35), font, 0.7, (220, 210, 180), 2, cv2.LINE_AA)
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

    obs_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    # CSV always gets the canonical path: hot-path/<peak_freq>/<obs_id>-<peak_freq>.wav
    audio_path = f"audio/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.wav"
    image_path = f"images/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.png"
    video_path = f"videos/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.mp4"
    all_hz = "|".join(f"{hz:.0f}" for hz in peak_hz_values)
    uploaded_artifact_keys = []
    try:
        if raw_audio_path and raw_bucket:
            # Move raw → canonical path: try copy then delete; else re-upload and delete raw
            try:
                client.copy_object(
                    MINIO_BUCKET,
                    audio_path,
                    CopySource(raw_bucket, raw_audio_path),
                )
                client.remove_object(raw_bucket, raw_audio_path)
            except Exception:
                wav_buf = BytesIO()
                wavfile.write(wav_buf, SAMPLE_RATE, (audio * 32767).astype(np.int16))
                wav_buf.seek(0)
                client.put_object(MINIO_BUCKET, audio_path, wav_buf, wav_buf.getbuffer().nbytes, "audio/wav")
                try:
                    client.remove_object(raw_bucket, raw_audio_path)
                except Exception:
                    pass
            uploaded_artifact_keys.append(audio_path)
        else:
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as fa:
                wav_path = fa.name
            wavfile.write(wav_path, SAMPLE_RATE, (audio * 32767).astype(np.int16))
            with open(wav_path, "rb") as f:
                client.put_object(MINIO_BUCKET, audio_path, f, os.path.getsize(wav_path), "audio/wav")
            os.remove(wav_path)
            uploaded_artifact_keys.append(audio_path)
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as fi:
            img_path = fi.name
        cv2.imwrite(img_path, img)
        with open(img_path, "rb") as f:
            client.put_object(MINIO_BUCKET, image_path, f, os.path.getsize(img_path), "image/png")
        os.remove(img_path)
        uploaded_artifact_keys.append(image_path)
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as fv:
            video_path_tmp = fv.name
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(video_path_tmp, fourcc, FPS, (VIDEO_RES, VIDEO_RES))
        for f in frames:
            writer.write(f)
        writer.release()
        with open(video_path_tmp, "rb") as f:
            client.put_object(MINIO_BUCKET, video_path, f, os.path.getsize(video_path_tmp), "video/mp4")
        os.remove(video_path_tmp)
        uploaded_artifact_keys.append(video_path)
        row = {
            "observation_id": obs_id,
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
        if raw_audio_path:
            row["raw_audio_path"] = raw_audio_path
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
            fieldnames = list(reader.fieldnames or [])
            if "observation_id" not in fieldnames:
                fieldnames.insert(0, "observation_id")
            if "raw_audio_path" not in fieldnames and raw_audio_path:
                fieldnames.append("raw_audio_path")
            rows_list = list(reader)
        else:
            fieldnames = list(row.keys())
            rows_list = []
        rows_list.append(row)
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_list)
        csv_bytes = buf.getvalue().encode("utf-8")
        client.put_object(MINIO_BUCKET, METADATA_KEY, io.BytesIO(csv_bytes), len(csv_bytes), "text/csv")
    except Exception as e:
        if uploaded_artifact_keys:
            _minio_remove_keys(client, MINIO_BUCKET, uploaded_artifact_keys)
        print(f"  [hot] batch upload failed: {e}")
        return False
    print(f"  [hot] stored 5s batch → {peak_freq_int} Hz  obs_id={obs_id[:8]}...")
    return True


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
    consumer = KafkaConsumer(
        KAFKA_TOPIC_HOT,
        bootstrap_servers=servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="landing-zone-hot-consumer",
    )
    processed_raw_paths = load_processed_raw_paths(client)
    print(f"\n🔴  Hot-path consumer: topic={KAFKA_TOPIC_HOT} → MinIO (manual commit, skip already-processed)\n")

    def commit_message(msg):
        try:
            tp = TopicPartition(msg.topic, msg.partition)
            # offset and leader_epoch must be int (protocol uses Int32; -1 = unknown)
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
                resp.close()
                resp.release_conn()
            except Exception as e:
                # Already moved/deleted (e.g. already processed) → skip and commit to avoid NoSuchKey spam
                processed_raw_paths.add(raw_path)
                commit_message(message)
                continue
            sr, data = wavfile.read(BytesIO(wav_bytes))
            if data.dtype == np.int16:
                audio = data.astype(np.float32) / 32768.0
            else:
                audio = data.astype(np.float32).flatten()
            if audio.ndim > 1:
                audio = audio.mean(axis=1)
            ok = process_batch(client, audio, raw_audio_path=raw_path, raw_bucket=bucket)
            if ok:
                processed_raw_paths.add(raw_path)
                commit_message(message)
        except Exception as e:
            print(f"  [hot consumer] message error: {e}")


if __name__ == "__main__":
    run()
