#!/usr/bin/env python3
"""
Landing Zone — cold-path (Freesound).

Downloads a user-specified batch of audio files (≤10 s, original quality)
from Freesound API v2, generates cymatics image + video for each, uploads
everything to MinIO, and appends rows to the shared metadata CSV.

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
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
load_dotenv()

import numpy as np
import cv2
from scipy.signal import find_peaks
from scipy.io import wavfile
from scipy.fft import dct

from minio import Minio

# =============================================================================
#  Config from env
# =============================================================================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
LANDING_ZONE_BUCKET = os.environ.get("LANDING_ZONE_BUCKET", "landing-zone").strip() or "landing-zone"

FREESOUND_API_KEY = os.environ.get("FREESOUND_API_KEY", "")

SOURCE = "Freesound"
METADATA_KEY = "metadata/observations.csv"
PROCESSING_VERSION = "1.1.0"

QUERIES = [
    "rain", "church bells", "crickets",
    "thunder", "water drops", "sea waves",
    "wind", "singing bowl", "tuning fork",
    "bird song", "cat purring", "dog bark",
    "waterfall", "ocean", "forest",
]

# Column order: shared + cold-specific
METADATA_EXTRA_FIELDS = [
    "uuid",
    "time_recorded/added",
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
COLD_EXTRA_FIELDS = [
    "tags",
    "description",
    "category",
    "subcategory",
    "geotag",
    "created",
    "type",
    "loudness",
]

# =============================================================================
#  Cymatics settings (identical to warm / hot paths)
# =============================================================================
CYM_DURATION = 5
SAMPLE_RATE = 44100
FPS = 30
VIDEO_RES = 600
SIM_RES = 350
IMG_RES = 2048
IMG_SIM = 900

ZONE_FRAC = [0.62]
N_SOURCES = [8, 14]
ZONE_SPATIAL_SCALE = [15.0, 22.0]
N_ZONES = 2


# =============================================================================
#  Cymatics helpers (same as warm/hot)
# =============================================================================
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
    return dict(rr=rr, theta=theta, R=R, c=c, r1=r1,
                masks=masks, plate=plate,
                inner_r=inner_r, outer_r=outer_r,
                boundaries=boundaries)


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


# =============================================================================
#  Audio-feature helpers (identical to warm / hot)
# =============================================================================
def spectral_features_hz(audio, sample_rate=SAMPLE_RATE):
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
    x = np.asarray(audio, dtype=np.float64).flatten()
    n = len(x)
    if n < 2:
        return 0.0
    power = np.abs(np.fft.rfft(x)) ** 2
    p = power / (np.sum(power) + 1e-20)
    p = p[p > 1e-20]
    return float(-np.sum(p * np.log(p)))


def zero_crossing_rate(audio):
    x = np.asarray(audio, dtype=np.float64).flatten()
    n = len(x)
    if n < 2:
        return 0.0
    signs = np.sign(x)
    signs[signs == 0] = 1.0
    return float(np.sum(np.abs(np.diff(signs)) > 0) / (n - 1))


def harmonic_energy_ratio(audio, sample_rate, fundamental_hz, max_harmonics=8, half_width_hz=None):
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
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY).astype(np.float64)
    g = gray.ravel()
    gl = np.fliplr(gray).ravel()
    g = g - g.mean(); gl = gl - gl.mean()
    denom = (np.linalg.norm(g) * np.linalg.norm(gl)) + 1e-12
    c = float(np.dot(g, gl) / denom)
    return float(np.clip(0.5 * (c + 1.0), 0.0, 1.0))


def pattern_stability_score(frames_bgr, step=3):
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


def loudness_db(audio):
    """RMS loudness in dBFS."""
    rms = float(np.sqrt(np.mean(np.asarray(audio, dtype=np.float64) ** 2)))
    return float(20.0 * np.log10(rms + 1e-20))


def merge_metadata_fieldnames(existing_names, row_keys):
    out = list(existing_names or [])
    seen = set(out)
    for k in METADATA_EXTRA_FIELDS + LEGACY_METADATA_FIELDS + COLD_EXTRA_FIELDS:
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


def search_freesound(query, page_size=15, page=1):
    """Return list of sound dicts for ≤10 s sounds matching *query*."""
    url = "https://freesound.org/apiv2/search/text/"
    params = {
        "query": query,
        "token": FREESOUND_API_KEY,
        "fields": FREESOUND_SEARCH_FIELDS,
        "page_size": page_size,
        "page": page,
        "filter": "duration:[1 TO 10]",
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
def ensure_minio_structure(client):
    if not client.bucket_exists(LANDING_ZONE_BUCKET):
        client.make_bucket(LANDING_ZONE_BUCKET)
        print(f"  Created bucket: {LANDING_ZONE_BUCKET}")
    placeholders = [
        "metadata/.keep",
        f"audio/{SOURCE}/.keep",
        f"images/{SOURCE}/.keep",
        f"videos/{SOURCE}/.keep",
    ]
    for key in placeholders:
        try:
            client.stat_object(LANDING_ZONE_BUCKET, key)
        except Exception:
            client.put_object(LANDING_ZONE_BUCKET, key, io.BytesIO(b""), 0)
    print("  Bucket and folder structure ready.")


# =============================================================================
#  Process one sound — cymatics + upload + metadata row
# =============================================================================
def process_sound(client, sound, audio_np, query):
    """Generate cymatics, upload artifacts, return metadata row dict (or None on failure)."""
    t_proc_start = time.perf_counter()

    audio = audio_np.copy()
    actual_duration_s = float(len(audio) / SAMPLE_RATE)

    # Use full original duration for cymatics
    cym_audio = audio.copy()
    pk = np.max(np.abs(cym_audio))
    if pk > 1e-6:
        cym_audio /= pk

    num_frames = max(1, int(actual_duration_s * FPS))
    samples_per_frame = max(1, len(cym_audio) // num_frames)

    # Peak detection
    window_len = int(SAMPLE_RATE * 0.25)
    hop = max(1, window_len // 2)
    window_candidates = []
    for off in range(0, len(cym_audio) - window_len, hop):
        chunk = cym_audio[off : off + window_len]
        hz = dominant_freq_hz_for_chunk(chunk)
        freq_bin = int(round(hz))
        energy = float(np.sum(np.abs(np.fft.rfft(chunk)) ** 2))
        window_candidates.append((freq_bin, chunk.copy(), off, energy, hz))
    if not window_candidates:
        return None
    dominant_freq_hz = harmonic_dominant_freq(window_candidates, max_harmonics=4, use_energy_weight=True)
    if dominant_freq_hz <= 0:
        return None
    peak_freq_int = int(round(dominant_freq_hz))
    best = max(
        (c for c in window_candidates if c[0] > 0 and peak_freq_int > 0 and c[0] % peak_freq_int == 0),
        key=lambda c: c[3], default=window_candidates[0],
    )
    _, peak_chunk, best_offset, _, _ = best
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

    # Spectral features (on full original audio)
    audio_norm = audio.copy()
    pk2 = np.max(np.abs(audio_norm))
    if pk2 > 1e-6:
        audio_norm /= pk2
    sc_hz, sbw_hz, srol_hz, sflat, signal_energy = spectral_features_hz(audio_norm)
    harm_ratio = harmonic_energy_ratio(audio_norm, SAMPLE_RATE, dominant_freq_hz)
    mfccs_json = mfccs_mean_json(audio_norm)
    spec_entropy = spectral_entropy_nats(audio_norm)
    zcr = zero_crossing_rate(audio_norm)

    # --- Build grids ---
    vz = build_zones(SIM_RES)
    v_sources = build_zone_sources(SIM_RES, vz)
    iz = build_zones(IMG_SIM)
    i_sources = build_zone_sources(IMG_SIM, iz)

    frame_data = []
    for fi in range(num_frames):
        s = fi * samples_per_frame
        chunk = cym_audio[s : s + samples_per_frame]
        zone_info = [analyse_frame(chunk, zi) for zi in range(N_ZONES)]
        frame_data.append(zone_info)

    # --- Video ---
    vy_g, vx_g = np.mgrid[0:VIDEO_RES, 0:VIDEO_RES]
    vd = np.sqrt((vx_g - VIDEO_RES / 2.0) ** 2 + (vy_g - VIDEO_RES / 2.0) ** 2)
    vig = np.clip(1.0 - (vd / (VIDEO_RES * 0.50)) ** 4, 0.0, 1.0)
    frames = []
    font_vid = cv2.FONT_HERSHEY_SIMPLEX
    for fi in range(num_frames):
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
        frame_rms = frame_data[fi][0][0]
        center = int((fi + 0.5) * samples_per_frame)
        wl = int(SAMPLE_RATE * 0.25)
        start = max(0, center - wl // 2)
        end = min(len(cym_audio), start + wl)
        start = max(0, end - wl)
        label_chunk = cym_audio[start:end]
        frame_hz = dominant_freq_hz_for_chunk(label_chunk)
        cv2.putText(raw, f"{frame_hz:.0f} Hz", (12, VIDEO_RES - 42), font_vid, 0.65, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(raw, f"{frame_hz:.0f} Hz", (10, VIDEO_RES - 40), font_vid, 0.65, (255, 255, 255), 2, cv2.LINE_AA)
        rms_txt = f"RMS: {frame_rms:.3f}"
        cv2.putText(raw, rms_txt, (12, VIDEO_RES - 14), font_vid, 0.55, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(raw, rms_txt, (10, VIDEO_RES - 12), font_vid, 0.55, (220, 220, 200), 2, cv2.LINE_AA)
        frames.append(raw)

    cf_len = int(FPS * 0.6)
    for i in range(cf_len):
        alpha = i / cf_len
        idx = num_frames - cf_len + i
        frames[idx] = cv2.addWeighted(frames[idx], 1.0 - alpha, frames[i], alpha, 0)

    pat_stability = pattern_stability_score(frames)

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

    font = cv2.FONT_HERSHEY_SIMPLEX
    freq_label = f"{dominant_freq_hz:.0f} Hz"
    ts0 = cv2.getTextSize(freq_label, font, 2.0, 3)[0]
    tx, ty = (IMG_RES - ts0[0]) // 2, IMG_RES - 80
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

    sym_score = symmetry_score_cymatics(img)

    # --- Upload artifacts ---
    obs_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    audio_key = f"audio/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.wav"
    image_key = f"images/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.png"
    video_key = f"videos/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.mp4"

    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            wav_tmp = f.name
        wavfile.write(wav_tmp, SAMPLE_RATE, (audio_norm * 32767).astype(np.int16))
        audio_size = os.path.getsize(wav_tmp)
        with open(wav_tmp, "rb") as f:
            client.put_object(LANDING_ZONE_BUCKET, audio_key, f, audio_size, "audio/wav")
        os.remove(wav_tmp)

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            img_tmp = f.name
        cv2.imwrite(img_tmp, img)
        image_size = os.path.getsize(img_tmp)
        with open(img_tmp, "rb") as f:
            client.put_object(LANDING_ZONE_BUCKET, image_key, f, image_size, "image/png")
        os.remove(img_tmp)

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            vid_tmp = f.name
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(vid_tmp, fourcc, FPS, (VIDEO_RES, VIDEO_RES))
        for fr in frames:
            writer.write(fr)
        writer.release()
        video_size = os.path.getsize(vid_tmp)
        with open(vid_tmp, "rb") as f:
            client.put_object(LANDING_ZONE_BUCKET, video_key, f, video_size, "video/mp4")
        os.remove(vid_tmp)
    except Exception as e:
        print(f"    Upload failed: {e}")
        return None

    processing_time_s = time.perf_counter() - t_proc_start
    all_hz_csv = "|".join(f"{hz:.0f}" for hz in peak_hz_values)

    tags_raw = sound.get("tags", [])
    tags_str = "|".join(tags_raw) if isinstance(tags_raw, list) else str(tags_raw)
    geotag = sound.get("geotag") or "-"
    description = (sound.get("description") or "-").replace("\n", " ").replace("\r", "")[:500]

    row = {
        "uuid": obs_id,
        "time_recorded/added": ts,
        "processing_version": PROCESSING_VERSION,
        "duration": actual_duration_s,
        "spectral_centroid_hz": sc_hz,
        "spectral_bandwidth_hz": sbw_hz,
        "spectral_rolloff_hz": srol_hz,
        "spectral_flatness": sflat,
        "MFCCs": mfccs_json,
        "spectral_entropy": spec_entropy,
        "zero_crossing_rate": zcr,
        "signal_energy": signal_energy,
        "harmonic_energy_ratio": harm_ratio,
        "symmetry_score": sym_score,
        "pattern_stability_score": pat_stability,
        "image_resolution": f"{IMG_RES}x{IMG_RES}",
        "video_resolution": f"{VIDEO_RES}x{VIDEO_RES}",
        "audio_size": audio_size,
        "image_size": image_size,
        "video_size": video_size,
        "processing_time(seconds)": processing_time_s,
        "kafka_topic": "-",
        "device": "-",
        "source": SOURCE,
        "peak_frequency_hz": dominant_freq_hz,
        "peak_time_s": peak_time,
        "peak_amplitude": peak_amplitude,
        "peak_rms": peak_rms,
        "audio_path": audio_key,
        "image_path": image_key,
        "video_path": video_key,
        "all_peak_frequencies_hz": all_hz_csv,
        "tags": tags_str,
        "description": description,
        "category": query,
        "subcategory": "-",
        "geotag": geotag,
        "created": sound.get("created", "-"),
        "type": sound.get("type", "-"),
        "loudness": loudness_db(audio_norm),
    }
    return row


# =============================================================================
#  Main
# =============================================================================
def run(batch_size=10):
    if not FREESOUND_API_KEY:
        raise RuntimeError("Set FREESOUND_API_KEY in .env")
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY in .env")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    ensure_minio_structure(client)

    # Collect sounds from Freesound, cycling through QUERIES
    collected = []
    seen_ids = set()
    q_idx = 0
    page = 1
    while len(collected) < batch_size and q_idx < len(QUERIES) * 3:
        query = QUERIES[q_idx % len(QUERIES)]
        per_page = min(15, batch_size - len(collected) + 5)
        results = search_freesound(query, page_size=per_page, page=page)
        if not results:
            q_idx += 1
            page = 1
            continue
        for s in results:
            if s["id"] in seen_ids:
                continue
            seen_ids.add(s["id"])
            collected.append((s, query))
            if len(collected) >= batch_size:
                break
        q_idx += 1
        if q_idx % len(QUERIES) == 0:
            page += 1
        time.sleep(0.3)

    print(f"\n  Collected {len(collected)} sounds from Freesound.\n")

    rows = []
    for i, (sound, query) in enumerate(collected):
        name = sound.get("name", "?")
        print(f"  [{i+1}/{len(collected)}] {name} ({sound.get('duration',0):.1f}s, query={query})")

        audio_bytes, ext = download_sound_bytes(sound)
        if audio_bytes is None:
            print("    Skipped — download failed.")
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
        return

    # Append to shared CSV
    try:
        resp = client.get_object(LANDING_ZONE_BUCKET, METADATA_KEY)
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
    client.put_object(LANDING_ZONE_BUCKET, METADATA_KEY, io.BytesIO(csv_bytes), len(csv_bytes), "text/csv")

    print("\n  Cold-path ingestion complete.")
    print(f"   Batch: {len(rows)} sounds processed and stored.")
    print(f"   Metadata: {METADATA_KEY}\n")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
    else:
        n = int(input("Enter batch size (number of sounds to ingest): "))
    run(batch_size=n)
