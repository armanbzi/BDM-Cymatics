#!/usr/bin/env python3
"""
Trusted Zone — Processing stage.

Batch path (Apache Spark): landing-zone metadata CSV is loaded in Spark for
information-preserving QA between Landing and Trusted — trim fields, drop
invalid rows, deduplicate on uuid, anti-join rows already present in trusted
metadata in Docker (`docker compose run --rm trusted-spark-batch`), which also
merges hot-path Kafka JSON from `landing-zone/metadata/kafka-events/hot-path/`
(device, timestamp → time_recorded/added), then loads `metadata/pending_workset.json`.

Then for each pending record:

  1. Downloads the raw audio from the landing-zone bucket.
  2. Generates a cymatics image (2048×2048) and video (600×600 @ 30 fps).
  3. Uploads audio, image, and video to the trusted-zone bucket with structure:
       audio/<peak_freq>/<uuid>-<peak_freq>.wav
       images/<peak_freq>/<uuid>-<peak_freq>.png
       videos/<peak_freq>/<uuid>-<peak_freq>.mp4
  4. Appends cymatics + peak metadata to trusted-zone CSV + Parquet.

Spectral / MFCC features are derived later in exploitation_zone (Spark + Python).

No path-based subfolders (warm/hot/cold) 

"""

import os
import sys
import io
import json
import tempfile
import time
from io import BytesIO

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
_tz_dir = os.path.abspath(os.path.dirname(__file__))
if _tz_dir not in sys.path:
    sys.path.insert(1, _tz_dir)

from dotenv import load_dotenv
load_dotenv()

import numpy as np
import cv2
from scipy.signal import find_peaks
from scipy.io import wavfile
from minio.commonconfig import CopySource

from shared.sync_delta import sync_observations_to_delta
from shared.minio_helpers import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    LANDING_ZONE_BUCKET,
    create_minio_client,
    ensure_bucket,
    update_parquet,
    append_rows_to_csv,
    is_unreachable_minio,
)
from shared.freq_detection import (
    harmonic_dominant_freq, dominant_freq_hz_for_chunk,
    build_window_candidates,
)
from shared.cymatics_engine import (
    N_ZONES, build_zones, build_zone_sources, analyse_frame,
    compute_interference, displacement_to_brightness, render_composite,
)
from spark_trusted_zone import (
    load_pending_workset,
    run_spark_trusted_zone_subprocess,
)

# =============================================================================
#  Config
# =============================================================================
LANDING_BUCKET = LANDING_ZONE_BUCKET
TRUSTED_BUCKET = os.environ.get("TRUSTED_ZONE_BUCKET", "trusted-zone")

LANDING_META_KEY = "metadata/observations.csv"
TRUSTED_META_KEY = "metadata/observations.csv"

PROCESSING_VERSION = "2.0.0"
SAMPLE_RATE = 44100

DURATION = 5
FPS = 30
NUM_FRAMES = DURATION * FPS
VIDEO_RES = 600
SIM_RES = 350
IMG_RES = 2048
IMG_SIM = 900

TRUSTED_METADATA_FIELDS = [
    "uuid", "source_id", "time_recorded/added", "processing_version",
    "duration",
    "symmetry_score", "pattern_stability_score",
    "image_resolution", "video_resolution",
    "audio_size", "image_size", "video_size",
    "processing_time(seconds)", "device", "audio_format",
    "source", "peak_frequency_hz", "peak_time_s", "peak_amplitude",
    "peak_rms", "audio_path", "image_path", "video_path",
    "all_peak_frequencies_hz",
    "tags", "description", "category",
]


# =============================================================================
#  Cymatics quality scores
# =============================================================================
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


# =============================================================================
#  MinIO helpers (trusted-zone specific)
# =============================================================================
def ensure_trusted_bucket(client):
    ensure_bucket(client, TRUSTED_BUCKET,
                  ["metadata/.keep", "audio/.keep", "images/.keep", "videos/.keep"])
    print(f"  Trusted-zone bucket ready: {TRUSTED_BUCKET}")


def download_audio_from_landing(client, audio_path):
    """Download audio from landing-zone and return (sample_rate, audio_float64)."""
    resp = client.get_object(LANDING_BUCKET, audio_path)
    wav_bytes = resp.read()
    resp.close(); resp.release_conn()
    sr, data = wavfile.read(BytesIO(wav_bytes))
    if data.dtype == np.int16:
        audio = data.astype(np.float64) / 32768.0
    else:
        audio = data.astype(np.float64)
    if audio.ndim > 1:
        audio = audio.mean(axis=1)
    return sr, audio


# =============================================================================
#  Process a single record
# =============================================================================
def process_record(client, landing_row):
    """Cymatics image/video generation + peak metadata (features → exploitation zone)."""
    t_start = time.perf_counter()
    rec_uuid = landing_row.get("uuid", "")
    source_id = landing_row.get("source_id", rec_uuid)
    audio_path_landing = landing_row.get("audio_path", "")
    source = landing_row.get("source", "")
    time_recorded_added = landing_row.get("time_recorded/added", "")
    audio_format = landing_row.get("audio_format", "wav")
    device = landing_row.get("device", "-")
    tags = landing_row.get("tags", "")
    description = landing_row.get("description", "")
    category = landing_row.get("category", "")

    if not audio_path_landing:
        print(f"    Skipped — no audio_path for uuid={rec_uuid}")
        return None

    try:
        sr, audio = download_audio_from_landing(client, audio_path_landing)
    except Exception as e:
        print(f"    Skipped — download error: {e}")
        return None

    if len(audio) < sr:
        print(f"    Skipped — audio too short ({len(audio)} samples)")
        return None

    pk = np.max(np.abs(audio))
    if pk > 1e-6:
        audio /= pk

    audio_duration_s = float(len(audio) / sr)

    cym_samples = int(DURATION * sr)
    if len(audio) >= cym_samples:
        cym_audio = audio[:cym_samples]
    else:
        cym_audio = np.pad(audio, (0, cym_samples - len(audio)))
    samples_per_frame = cym_samples // NUM_FRAMES

    # --- Peak frequency detection ---
    window_candidates = build_window_candidates(audio, sr)
    if not window_candidates:
        print("    Skipped — no window candidates")
        return None

    dominant_freq_hz = harmonic_dominant_freq(window_candidates, max_harmonics=4, use_energy_weight=True)
    if dominant_freq_hz <= 0:
        print("    Skipped — could not determine dominant frequency")
        return None

    peak_freq_int = int(round(dominant_freq_hz))
    dominant_freq_bin = peak_freq_int

    best = max(
        (c for c in window_candidates if c[0] > 0 and dominant_freq_bin > 0 and c[0] % dominant_freq_bin == 0),
        key=lambda c: c[3],
        default=window_candidates[0],
    )
    _bin, peak_chunk, best_offset, _energy, _hz = best
    peak_chunk = np.asarray(peak_chunk, dtype=np.float64)
    peak_rms = float(np.sqrt(np.mean(peak_chunk**2)))
    peak_amplitude = float(np.max(np.abs(peak_chunk)))
    peak_time = best_offset / sr

    peak_fft = np.abs(np.fft.rfft(peak_chunk))
    peak_fft_norm = peak_fft / (np.max(peak_fft) + 1e-12)
    peaks_idx, _ = find_peaks(peak_fft_norm, height=0.06, distance=6, prominence=0.04)
    if len(peaks_idx) == 0:
        peaks_idx = np.array([np.argmax(peak_fft_norm)])
    peak_amps_arr = peak_fft_norm[peaks_idx]
    top_order = np.argsort(peak_amps_arr)[::-1][:8]
    peaks_idx = peaks_idx[top_order]
    window_len = int(sr * 0.25)
    freq_resolution = sr / window_len
    peak_hz_values = peaks_idx * freq_resolution

    # --- Cymatics: video ---
    vz = build_zones(SIM_RES)
    v_sources = build_zone_sources(SIM_RES, vz)
    iz = build_zones(IMG_SIM)
    i_sources = build_zone_sources(IMG_SIM, iz)

    frame_data = []
    for fi in range(NUM_FRAMES):
        s = fi * samples_per_frame
        chunk = cym_audio[s : s + samples_per_frame]
        zone_info = [analyse_frame(chunk, zi, sr) for zi in range(N_ZONES)]
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
        wl = int(sr * 0.25)
        start = max(0, center - wl // 2)
        end = min(len(cym_audio), start + wl)
        start = max(0, end - wl)
        label_chunk = cym_audio[start:end]
        frame_hz = dominant_freq_hz_for_chunk(label_chunk, sr)
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

    pat_stability = pattern_stability_score(frames)

    # --- Cymatics: high-res image ---
    img_zone_br = []
    for zi in range(N_ZONES):
        _, sfreqs, _ = analyse_frame(peak_chunk, zi, sr)
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
    freq_label = f"{dominant_freq_hz:.1f} Hz"
    ts_size = cv2.getTextSize(freq_label, font, 1.0, 2)[0]
    tx = (IMG_RES - ts_size[0]) // 2
    cv2.putText(img, freq_label, (tx + 2, IMG_RES - 72), font, 1.0, (0, 0, 0), 4, cv2.LINE_AA)
    cv2.putText(img, freq_label, (tx, IMG_RES - 70), font, 1.0, (255, 255, 255), 2, cv2.LINE_AA)
    all_hz_str = ", ".join(f"{h:.0f}" for h in peak_hz_values[:6])
    ts2 = cv2.getTextSize(all_hz_str, font, 0.7, 2)[0]
    tx2 = (IMG_RES - ts2[0]) // 2
    cv2.putText(img, all_hz_str, (tx2 + 2, IMG_RES - 37), font, 0.7, (0, 0, 0), 3, cv2.LINE_AA)
    cv2.putText(img, all_hz_str, (tx2, IMG_RES - 35), font, 0.7, (220, 210, 180), 2, cv2.LINE_AA)
    title = "CYMATICS"
    ts3 = cv2.getTextSize(title, font, 1.2, 2)[0]
    ttx = (IMG_RES - ts3[0]) // 2
    cv2.putText(img, title, (ttx + 2, 62), font, 1.2, (0, 0, 0), 4, cv2.LINE_AA)
    cv2.putText(img, title, (ttx, 60), font, 1.2, (240, 230, 200), 2, cv2.LINE_AA)
    amp_label = f"Amplitude: {peak_amplitude:.3f}  |  RMS: {peak_rms:.3f}"
    ts4 = cv2.getTextSize(amp_label, font, 0.7, 2)[0]
    atx = (IMG_RES - ts4[0]) // 2
    cv2.putText(img, amp_label, (atx + 2, 97), font, 0.7, (0, 0, 0), 3, cv2.LINE_AA)
    cv2.putText(img, amp_label, (atx, 95), font, 0.7, (220, 210, 180), 2, cv2.LINE_AA)

    sym_score = symmetry_score_cymatics(img)

    # --- Upload to trusted-zone ---
    audio_key = f"audio/{peak_freq_int}/{rec_uuid}-{peak_freq_int}.wav"
    image_key = f"images/{peak_freq_int}/{rec_uuid}-{peak_freq_int}.png"
    video_key = f"videos/{peak_freq_int}/{rec_uuid}-{peak_freq_int}.mp4"
    all_hz_csv = "|".join(f"{h:.0f}" for h in peak_hz_values)

    try:
        try:
            client.copy_object(
                TRUSTED_BUCKET, audio_key,
                CopySource(LANDING_BUCKET, audio_path_landing),
            )
            audio_size = int(client.stat_object(TRUSTED_BUCKET, audio_key).size)
        except Exception:
            wav_buf = BytesIO()
            wavfile.write(wav_buf, sr, (audio * 32767).astype(np.int16))
            audio_size = wav_buf.getbuffer().nbytes
            wav_buf.seek(0)
            client.put_object(TRUSTED_BUCKET, audio_key, wav_buf, audio_size, "audio/wav")

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as fi:
            img_path_tmp = fi.name
        cv2.imwrite(img_path_tmp, img)
        image_size = os.path.getsize(img_path_tmp)
        with open(img_path_tmp, "rb") as f:
            client.put_object(TRUSTED_BUCKET, image_key, f, image_size, "image/png")
        os.remove(img_path_tmp)

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as fv:
            video_path_tmp = fv.name
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(video_path_tmp, fourcc, FPS, (VIDEO_RES, VIDEO_RES))
        for f in frames:
            writer.write(f)
        writer.release()
        video_size = os.path.getsize(video_path_tmp)
        with open(video_path_tmp, "rb") as f:
            client.put_object(TRUSTED_BUCKET, video_key, f, video_size, "video/mp4")
        os.remove(video_path_tmp)

    except Exception as e:
        print(f"    Upload failed: {e}")
        return None

    processing_time_s = time.perf_counter() - t_start

    row = {
        "uuid": rec_uuid,
        "source_id": source_id,
        "time_recorded/added": time_recorded_added,
        "processing_version": PROCESSING_VERSION,
        "duration": audio_duration_s,
        "symmetry_score": sym_score,
        "pattern_stability_score": pat_stability,
        "image_resolution": f"{IMG_RES}x{IMG_RES}",
        "video_resolution": f"{VIDEO_RES}x{VIDEO_RES}",
        "audio_size": audio_size,
        "image_size": image_size,
        "video_size": video_size,
        "processing_time(seconds)": processing_time_s,
        "device": device,
        "audio_format": audio_format,
        "source": source,
        "peak_frequency_hz": dominant_freq_hz,
        "peak_time_s": peak_time,
        "peak_amplitude": peak_amplitude,
        "peak_rms": peak_rms,
        "audio_path": audio_key,
        "image_path": image_key,
        "video_path": video_key,
        "all_peak_frequencies_hz": all_hz_csv,
        "tags": tags,
        "description": description,
        "category": category,
    }
    return row


# =============================================================================
#  Main
# =============================================================================
def run():
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY")

    client = create_minio_client()
    try:
        ensure_trusted_bucket(client)
    except Exception as e:
        from minio.error import S3Error

        if is_unreachable_minio(e):
            raise RuntimeError(
                f"MinIO at {MINIO_ENDPOINT!r} is unreachable while preparing trusted bucket "
                f"{TRUSTED_BUCKET!r}. Start MinIO and check MINIO_ENDPOINT, MINIO_SECURE, and credentials."
            ) from e
        if isinstance(e, S3Error):
            code = getattr(e, "code", "") or ""
            raise RuntimeError(
                f"Cannot create or access trusted bucket {TRUSTED_BUCKET!r} (MinIO code={code}). "
                "Adjust TRUSTED_ZONE_BUCKET or MinIO user policy."
            ) from e
        raise RuntimeError(
            f"Trusted bucket setup failed for {TRUSTED_BUCKET!r}: {e}"
        ) from e

    print("\n[1/3] Spark batch (Docker): landing-zone metadata → pending trusted workset...")
    try:
        run_spark_trusted_zone_subprocess(project_root=_project_root)
        pending = load_pending_workset(client, bucket=TRUSTED_BUCKET)
    except Exception as e:
        low = repr(e).lower()
        msg = str(e)
        hint = ""
        if "metadata batch container failed" in msg.lower():
            hint = (
                " Start the stack from the repo root: docker compose up -d, "
                "then retry. Inspect trusted-spark-batch logs for S3A or Spark master errors."
            )
        elif is_unreachable_minio(e) or "cannot reach minio" in msg.lower():
            hint = " MinIO must be up before loading the pending workset (MINIO_ENDPOINT on the host)."
        elif "pending workset missing" in msg.lower():
            hint = (
                " The Spark container may have exited before writing metadata/pending_workset.json."
            )
        elif ("connection refused" in low or "could not connect" in low) and (
            "docker" in low or "daemon" in low
        ):
            hint = " Start Docker Desktop and run from the repo root: docker compose up -d."
        raise RuntimeError(
            "Trusted-zone Spark batch failed — " + str(e) + hint
        ) from e
    if not pending:
        print(
            "  No pending records — empty landing metadata, invalid rows filtered out,"
            " or all UUIDs already in trusted-zone.",
        )
        return

    print(f"  Pending rows after Spark metadata batch: {len(pending)}")

    by_source = {}
    for r in pending:
        s = r.get("source", "unknown")
        by_source[s] = by_source.get(s, 0) + 1
    print(f"\n[2/3] Processing {len(pending)} new records:")
    for s, cnt in sorted(by_source.items()):
        print(f"    {s}: {cnt}")

    rows = []
    for i, rec in enumerate(pending):
        uid = rec.get("uuid", "?")[:8]
        src = rec.get("source", "?")
        print(f"\n  [{i+1}/{len(pending)}] uuid={uid}...  source={src}")
        row = process_record(client, rec)
        if row:
            rows.append(row)
            print(f"    Done — {row['peak_frequency_hz']:.0f} Hz  ({row['processing_time(seconds)']:.1f}s)")

    if not rows:
        print("\n  No records processed.")
        return

    print(f"\n[3/3] Writing metadata ({len(rows)} rows)...")
    append_rows_to_csv(client, TRUSTED_BUCKET, rows, TRUSTED_METADATA_FIELDS,
                       key=TRUSTED_META_KEY)
    print(f"  Updated: {TRUSTED_META_KEY}")

    update_parquet(client, TRUSTED_BUCKET, rows)

    print("\n[4/4] Syncing Parquet → Delta Lake...")
    sync_observations_to_delta(client, TRUSTED_BUCKET, zone_label="trusted")

    print("\n  Trusted-zone processing complete.")
    print(f"   Processed: {len(rows)} records")
    print(f"   Bucket: {TRUSTED_BUCKET}")
    print(f"   Metadata: {TRUSTED_META_KEY}\n")


if __name__ == "__main__":
    run()
