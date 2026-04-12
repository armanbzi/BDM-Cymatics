#!/usr/bin/env python3
"""
Landing Zone — warm-path.

Records 5 seconds of audio, generates a cymatics preview video (displayed
but NOT stored yet), and asks the user whether to keep the recording.

If approved:
  - Stores only the audio WAV in MinIO at
      audio/warm-path/<peak_freq>/<uuid>-<peak_freq>.wav, to be processed later in the trusted-zone.
  - Appends metadata row to csv and parquet.

Cymatics generations happen later in the trusted-zone.

"""

import os
import io
import csv
import json
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import numpy as np
import sounddevice as sd
import cv2
import time
from collections import Counter
from scipy.signal import find_peaks
from scipy.io import wavfile

try:
    from minio import Minio
    HAS_MINIO = True
except ImportError:
    HAS_MINIO = False

import pyarrow as pa
import pyarrow.parquet as pq

# =============================================================================
#  Config
# =============================================================================
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "landing-zone")

SOURCE = "warm-path"
PAUSE_MONITOR_FILE = os.path.join(tempfile.gettempdir(), ".cymatics_pause_monitor")
METADATA_KEY = "metadata/observations.csv"

METADATA_FIELDS = [
    "uuid",
    "source_id",
    "time_recorded/added",
    "duration",
    "audio_size",
    "audio_path",
    "audio_format",
    "source",
    "peak_frequency_hz",
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
#  Cymatics settings (preview only — not stored)
# =============================================================================
DURATION = 5
SAMPLE_RATE = 44100
FPS = 30
NUM_FRAMES = DURATION * FPS
VIDEO_RES = 600
SIM_RES = 350

ZONE_FRAC = [0.62]
N_SOURCES = [8, 14]
ZONE_SPATIAL_SCALE = [15.0, 22.0]
N_ZONES = 2


# =============================================================================
#  Cymatics helpers (preview generation)
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
#  MinIO helpers
# =============================================================================
def ensure_minio_structure(client):
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            print(f"  Created bucket: {MINIO_BUCKET}")
        placeholders = ["metadata/.keep", "audio/warm-path/.keep"]
        for key in placeholders:
            try:
                client.stat_object(MINIO_BUCKET, key)
            except Exception:
                client.put_object(MINIO_BUCKET, key, io.BytesIO(b""), 0)
        print("  Bucket and folder structure ready.")
    except Exception as e:
        raise RuntimeError(f"MinIO structure setup failed: {e}") from e


# =============================================================================
#  Main
# =============================================================================
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
    print("\n  Get ready to record...")
    for i in range(3, 0, -1):
        print(f"   {i}...", flush=True)
        time.sleep(1)
    print("   RECORDING — make some noise!\n")
    audio = sd.rec(int(DURATION * SAMPLE_RATE), samplerate=SAMPLE_RATE, channels=1, dtype="float32")
    sd.wait()
    audio = audio.flatten()
    print("  Recording complete!\n")

    pk = np.max(np.abs(audio))
    if pk > 1e-6:
        audio /= pk
    audio_duration_s = float(len(audio) / SAMPLE_RATE)
    samples_per_frame = len(audio) // NUM_FRAMES

    # --- Detect dominant frequency ---
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
    peak_freq_int = int(round(dominant_freq_hz))
    print(f"   Dominant frequency: {dominant_freq_hz:.1f} Hz\n")

    # --- Generate cymatics preview video (display only, not stored) ---
    print("  Generating cymatics preview...")
    vz = build_zones(SIM_RES)
    v_sources = build_zone_sources(SIM_RES, vz)

    frame_data = []
    for fi in range(NUM_FRAMES):
        s = fi * samples_per_frame
        chunk = audio[s : s + samples_per_frame]
        zone_info = [analyse_frame(chunk, zi) for zi in range(N_ZONES)]
        frame_data.append(zone_info)

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
        frame_rms = frame_data[fi][0][0]
        center = int((fi + 0.5) * samples_per_frame)
        wl = int(SAMPLE_RATE * 0.25)
        start = max(0, center - wl // 2)
        end = min(len(audio), start + wl)
        start = max(0, end - wl)
        label_chunk = audio[start:end]
        frame_hz = dominant_freq_hz_for_chunk(label_chunk)
        cv2.putText(raw, f"{frame_hz:.0f} Hz", (12, VIDEO_RES - 42), font_vid, 0.65, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(raw, f"{frame_hz:.0f} Hz", (10, VIDEO_RES - 40), font_vid, 0.65, (255, 255, 255), 2, cv2.LINE_AA)
        rms_txt = f"RMS: {frame_rms:.3f}"
        cv2.putText(raw, rms_txt, (12, VIDEO_RES - 14), font_vid, 0.55, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(raw, rms_txt, (10, VIDEO_RES - 12), font_vid, 0.55, (220, 220, 200), 2, cv2.LINE_AA)
        sec_now = fi / FPS
        sec_txt = f"{sec_now:.1f}s / {DURATION}s"
        cv2.putText(raw, sec_txt, (VIDEO_RES - 155, 32), font_vid, 0.6, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(raw, sec_txt, (VIDEO_RES - 157, 30), font_vid, 0.6, (255, 255, 255), 2, cv2.LINE_AA)
        frames.append(raw)

    cf_len = int(FPS * 0.6)
    for i in range(cf_len):
        alpha = i / cf_len
        idx = NUM_FRAMES - cf_len + i
        frames[idx] = cv2.addWeighted(frames[idx], 1.0 - alpha, frames[i], alpha, 0)

    # --- Display preview video (loops until user presses any key or closes window) ---
    Path(PAUSE_MONITOR_FILE).touch()
    try:
        print("  Playing cymatics preview (press any key or close window to stop)...\n")
        cv2.namedWindow("Cymatics Preview", cv2.WINDOW_AUTOSIZE)
        stopped = False
        while not stopped:
            for frame in frames:
                cv2.imshow("Cymatics Preview", frame)
                key = cv2.waitKey(int(1000 / FPS)) & 0xFF
                if key != 255:
                    stopped = True
                    break
                if cv2.getWindowProperty("Cymatics Preview", cv2.WND_PROP_VISIBLE) < 1:
                    stopped = True
                    break
        cv2.destroyAllWindows()

        # --- Ask user for approval ---
        print(f"\n   Dominant frequency: {dominant_freq_hz:.1f} Hz")
        print(f"   Duration: {audio_duration_s:.1f}s\n")
        choice = input("  Store this recording? (y/n): ").strip().lower()
    finally:
        try:
            os.remove(PAUSE_MONITOR_FILE)
        except FileNotFoundError:
            pass
    if choice not in ("y", "yes"):
        print("\n  Recording discarded.\n")
        return

    # --- Store audio only ---
    obs_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    audio_key = f"audio/{SOURCE}/{peak_freq_int}/{obs_id}-{peak_freq_int}.wav"

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as fa:
        wav_path = fa.name
    wavfile.write(wav_path, SAMPLE_RATE, (audio * 32767).astype(np.int16))
    audio_size = os.path.getsize(wav_path)
    with open(wav_path, "rb") as f:
        client.put_object(MINIO_BUCKET, audio_key, f, audio_size, "audio/wav")
    os.remove(wav_path)
    print(f"  Uploaded: {audio_key}")

    # --- Metadata (lightweight — enrichment happens in trusted-zone) ---
    row = {
        "uuid": obs_id,
        "source_id": obs_id,
        "time_recorded/added": ts,
        "duration": audio_duration_s,
        "audio_size": audio_size,
        "audio_path": audio_key,
        "audio_format": "wav",
        "source": SOURCE,
        "peak_frequency_hz": dominant_freq_hz,
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
    print(f"  Updated: {METADATA_KEY}")

    update_parquet(client, MINIO_BUCKET, [row])

    print(f"\n  Observation stored in MinIO.")
    print(f"   UUID: {obs_id}")
    print(f"   Peak: {dominant_freq_hz:.1f} Hz  |  source: {SOURCE}")
    print(f"   Audio: {audio_key}\n")


if __name__ == "__main__":
    run()
