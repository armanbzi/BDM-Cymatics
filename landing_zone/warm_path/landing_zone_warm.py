#!/usr/bin/env python3
"""
Landing Zone — Cymatics recording pipeline with MinIO storage.

Uses environment variables for MinIO config:
  MINIO_ENDPOINT   — e.g. localhost:9000
  MINIO_ACCESS_KEY — MinIO access key
  MINIO_SECRET_KEY — MinIO secret key
  MINIO_SECURE     — true/false (default: false for local dev)
  MINIO_BUCKET     — bucket name (default: landing-zone)

Run: records 5s audio, generates video/image, stores all in MinIO.
"""

import os
import io
import csv
import tempfile
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

# MinIO
try:
    from minio import Minio
    HAS_MINIO = True
except ImportError:
    HAS_MINIO = False

# =============================================================================
#  MinIO config from env
# =============================================================================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "landing-zone")

SOURCE = "warm-path"
METADATA_KEY = "metadata/observations.csv"

# =============================================================================
#  Cymatics settings (2 zones — same as visualizer)
# =============================================================================
DURATION = 5
SAMPLE_RATE = 44100
FPS = 30
OUTPUT_FPS = 15  # Slower playback
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
    return rms, sfreqs


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


def ensure_minio_structure(client):
    """Ensure bucket exists. Folders are implicit via object keys."""
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"  Created bucket: {MINIO_BUCKET}")
    # Placeholder objects to "create" folders (optional; MinIO creates on first real upload)
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

    pk = np.max(np.abs(audio))
    if pk > 1e-6:
        audio /= pk
    samples_per_frame = len(audio) // NUM_FRAMES

    # --- Peak detection ---
    window_len = int(SAMPLE_RATE * 0.25)
    best_energy = 0.0
    best_offset = 0
    hop = window_len // 4
    for off in range(0, len(audio) - window_len, hop):
        chunk = audio[off : off + window_len]
        energy = float(np.sum(np.abs(np.fft.rfft(chunk)) ** 2))
        if energy > best_energy:
            best_energy = energy
            best_offset = off

    peak_chunk = audio[best_offset : best_offset + window_len]
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
    dominant_freq_hz = float(peak_hz_values[0])
    peak_freq_int = int(round(dominant_freq_hz))

    print(f"   Dominant frequency: {dominant_freq_hz:.1f} Hz (folder: {peak_freq_int})")

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
    for fi in range(NUM_FRAMES):
        t = fi / FPS
        zone_br = []
        for zi in range(N_ZONES):
            rms, sfreqs = frame_data[fi][zi]
            disp_x, disp_y = compute_interference(v_sources[zi], sfreqs, t, SIM_RES)
            br = displacement_to_brightness(disp_x, disp_y, vz["masks"][zi], SIM_RES, rms, t)
            zone_br.append(br)
        raw = render_composite(zone_br, vz, t, SIM_RES)
        raw = cv2.resize(raw, (VIDEO_RES, VIDEO_RES), interpolation=cv2.INTER_CUBIC)
        glow = cv2.GaussianBlur(raw, (0, 0), sigmaX=6)
        raw = cv2.addWeighted(raw, 0.82, glow, 0.28, 0)
        for ch in range(3):
            raw[:, :, ch] = (raw[:, :, ch].astype(np.float64) * vig).astype(np.uint8)
        frames.append(raw)

    cf_len = int(FPS * 0.6)
    for i in range(cf_len):
        alpha = i / cf_len
        idx = NUM_FRAMES - cf_len + i
        frames[idx] = cv2.addWeighted(frames[idx], 1.0 - alpha, frames[i], alpha, 0)

    # --- Image ---
    img_zone_br = []
    for zi in range(N_ZONES):
        _, sfreqs = analyse_frame(peak_chunk, zi)
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

    # --- Save to temp files and upload ---
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    audio_path = f"audio/{SOURCE}/{peak_freq_int}/recording.wav"
    image_path = f"images/{SOURCE}/{peak_freq_int}/cymatics.png"
    video_path = f"videos/{SOURCE}/{peak_freq_int}/water_cymatics.mp4"

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as fa:
        wav_path = fa.name
    wavfile.write(wav_path, SAMPLE_RATE, (audio * 32767).astype(np.int16))
    with open(wav_path, "rb") as f:
        client.put_object(MINIO_BUCKET, audio_path, f, os.path.getsize(wav_path), "audio/wav")
    os.remove(wav_path)
    print(f"  Uploaded: {audio_path}")

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as fi:
        img_path = fi.name
    cv2.imwrite(img_path, img)
    with open(img_path, "rb") as f:
        client.put_object(MINIO_BUCKET, image_path, f, os.path.getsize(img_path), "image/png")
    os.remove(img_path)
    print(f"  Uploaded: {image_path}")

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as fv:
        video_path_tmp = fv.name
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(video_path_tmp, fourcc, OUTPUT_FPS, (VIDEO_RES, VIDEO_RES))
    for f in frames:
        writer.write(f)
    writer.release()
    with open(video_path_tmp, "rb") as f:
        client.put_object(MINIO_BUCKET, video_path, f, os.path.getsize(video_path_tmp), "video/mp4")
    os.remove(video_path_tmp)
    print(f"  Uploaded: {video_path}")

    # --- Metadata ---
    all_hz = "|".join(f"{hz:.0f}" for hz in peak_hz_values)
    row = {
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
        fieldnames = reader.fieldnames
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
    print(f"  Updated: {METADATA_KEY}")

    print("\n✅  Observation stored in MinIO.")
    print(f"   Peak: {dominant_freq_hz:.1f} Hz  |  source: {SOURCE}")
    print(f"   Metadata: {METADATA_KEY}\n")


if __name__ == "__main__":
    run()
