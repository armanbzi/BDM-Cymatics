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
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

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

from shared.minio_helpers import (
    MINIO_BUCKET, create_minio_client, ensure_bucket,
    METADATA_KEY, PARQUET_KEY, update_parquet, append_rows_to_csv,
)
from shared.freq_detection import (
    harmonic_dominant_freq, dominant_freq_hz_for_chunk,
    build_window_candidates,
)
from shared.cymatics_engine import (
    N_ZONES, build_zones, build_zone_sources, analyse_frame,
    compute_interference, displacement_to_brightness, render_composite,
)

try:
    from minio import Minio
    HAS_MINIO = True
except ImportError:
    HAS_MINIO = False

# =============================================================================
#  Config
# =============================================================================
SOURCE = "warm-path"
PAUSE_MONITOR_FILE = os.path.join(tempfile.gettempdir(), ".cymatics_pause_monitor")

DURATION = 5
SAMPLE_RATE = 44100
FPS = 30
NUM_FRAMES = DURATION * FPS
VIDEO_RES = 600
SIM_RES = 350


# =============================================================================
#  Main
# =============================================================================
def run():
    if not HAS_MINIO:
        raise RuntimeError("Install minio: pip install minio")

    client = create_minio_client()
    ensure_bucket(client, MINIO_BUCKET, ["metadata/.keep", "audio/warm-path/.keep"])

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
    window_candidates = build_window_candidates(audio, SAMPLE_RATE)
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
        zone_info = [analyse_frame(chunk, zi, SAMPLE_RATE) for zi in range(N_ZONES)]
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

    append_rows_to_csv(client, MINIO_BUCKET, [row])
    print(f"  Updated: {METADATA_KEY}")

    update_parquet(client, MINIO_BUCKET, [row])

    print("\n  Observation stored in MinIO.")
    print(f"   UUID: {obs_id}")
    print(f"   Peak: {dominant_freq_hz:.1f} Hz  |  source: {SOURCE}")
    print(f"   Audio: {audio_key}\n")


if __name__ == "__main__":
    run()
