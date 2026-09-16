"""Frame-accurate audio feature extraction for the music visualizer.

Every feature is computed on the exact per-video-frame audio window, so when
the rendered frames are muxed back with the same audio the sync is
sample-accurate by construction.

Features per frame (all normalised ~0..1, envelope-followed where noted):
    bass / mid / high   band energies (env: fast attack, musical release)
    loud                RMS loudness (env)
    kick                bass-band spectral-flux hits (fast-decay env)
    onset               broadband spectral-flux hits (fast-decay env)
    strong_onset        boolean — the louder onsets (section/symmetry changes)
    hue                 0..1 colour hue from the smoothed dominant pitch
"""
from __future__ import annotations

import os
import subprocess
import tempfile

import numpy as np
from scipy.io import wavfile
from scipy.signal import find_peaks

DEFAULT_SR = 44100


# ---------------------------------------------------------------------------
#  Loading
# ---------------------------------------------------------------------------
def load_audio(path, sr=DEFAULT_SR):
    """Decode any audio file (wav/mp3/m4a/...) to mono float64 via ffmpeg."""
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", str(path), "-ac", "1", "-ar", str(sr), tmp.name],
            check=True, capture_output=True)
        rate, data = wavfile.read(tmp.name)
    finally:
        os.unlink(tmp.name)
    x = np.asarray(data, dtype=np.float64)
    x /= (np.max(np.abs(x)) + 1e-9)
    return x, rate


# ---------------------------------------------------------------------------
#  Envelope + normalisation helpers
# ---------------------------------------------------------------------------
def _peak_env(x, fps, release):
    """Instant attack, exponential release — the classic VU follower."""
    decay = np.exp(-1.0 / (fps * release))
    out = np.empty_like(x)
    e = 0.0
    for i, v in enumerate(x):
        e = v if v > e else e * decay
        out[i] = e
    return out


def _norm(x, pct=95):
    ref = np.percentile(x, pct) + 1e-9
    return np.clip(x / ref, 0.0, 1.3)


def _impulse_env(n_frames, hit_frames, strengths, fps, release):
    x = np.zeros(n_frames)
    for f, s in zip(hit_frames, strengths):
        if 0 <= f < n_frames:
            x[f] = max(x[f], s)
    return _peak_env(x, fps, release)


# ---------------------------------------------------------------------------
#  Main extraction
# ---------------------------------------------------------------------------
def extract_features(audio, sr, fps, win=4096):
    hop = int(round(sr / fps))
    n_frames = max(1, len(audio) // hop)
    hann = np.hanning(win)
    freqs = np.fft.rfftfreq(win, 1.0 / sr)
    m_bass = (freqs >= 25) & (freqs < 150)
    m_mid = (freqs >= 150) & (freqs < 2000)
    m_high = (freqs >= 2000) & (freqs < 9000)
    m_dom = (freqs >= 55) & (freqs < 2000)
    dom_freqs = freqs[m_dom]

    bass = np.zeros(n_frames); mid = np.zeros(n_frames); high = np.zeros(n_frames)
    loud = np.zeros(n_frames); flux = np.zeros(n_frames); bflux = np.zeros(n_frames)
    dom_hz = np.zeros(n_frames)
    prev_mag = None

    pad = np.pad(audio, (win // 2, win // 2))
    for i in range(n_frames):
        center = i * hop + hop // 2
        chunk = pad[center: center + win]
        if len(chunk) < win:
            chunk = np.pad(chunk, (0, win - len(chunk)))
        mag = np.abs(np.fft.rfft(chunk * hann))
        bass[i] = mag[m_bass].mean()
        mid[i] = mag[m_mid].mean()
        high[i] = mag[m_high].mean()
        seg = audio[i * hop: (i + 1) * hop]
        loud[i] = np.sqrt(np.mean(seg ** 2)) if len(seg) else 0.0
        dom_hz[i] = dom_freqs[int(np.argmax(mag[m_dom]))]
        if prev_mag is not None:
            d = mag - prev_mag
            d[d < 0] = 0.0
            flux[i] = d.sum()
            bflux[i] = d[m_bass].sum()
        prev_mag = mag

    # --- onset / kick picking on the flux curves -------------------------
    def _pick(curve, min_gap_s, k=1.4):
        c = _norm(curve)
        thr = np.median(c) * k + 0.08
        idx, props = find_peaks(c, height=thr, distance=max(1, int(min_gap_s * fps)))
        return idx, c[idx]

    onset_idx, onset_str = _pick(flux, 0.16)
    kick_idx, kick_str = _pick(bflux, 0.20)

    strong = np.zeros(n_frames, dtype=bool)
    if len(onset_idx):
        cut = np.percentile(onset_str, 60)
        strong[onset_idx[onset_str >= cut]] = True

    # --- optional librosa beat grid (nice-to-have, not required) ---------
    beat_frames = []
    try:
        import librosa
        _, beats = librosa.beat.beat_track(y=audio.astype(np.float32), sr=sr,
                                           hop_length=512)
        beat_t = librosa.frames_to_time(beats, sr=sr, hop_length=512)
        beat_frames = [int(t * fps) for t in beat_t]
    except Exception:
        beat_frames = list(kick_idx)

    # --- hue from smoothed dominant pitch (low warm -> high cool) --------
    lo, hi = np.log(55.0), np.log(1760.0)
    norm = np.clip((np.log(np.maximum(dom_hz, 1.0)) - lo) / (hi - lo), 0.0, 1.0)
    hue_raw = 0.02 + norm * 0.78
    hue = np.empty_like(hue_raw)
    alpha = 1.0 - np.exp(-1.0 / (fps * 0.6))  # ~0.6 s EMA
    h = hue_raw[0]
    for i, v in enumerate(hue_raw):
        h += alpha * (v - h)
        hue[i] = h

    return {
        "n_frames": n_frames, "fps": fps, "sr": sr, "hop": hop, "win": win,
        "audio": audio,
        "bass": np.clip(_peak_env(_norm(bass), fps, 0.28), 0, 1),
        "mid": np.clip(_peak_env(_norm(mid), fps, 0.30), 0, 1),
        "high": np.clip(_peak_env(_norm(high), fps, 0.16), 0, 1),
        "loud": np.clip(_peak_env(_norm(loud), fps, 0.35), 0, 1),
        "kick": np.clip(_impulse_env(n_frames, kick_idx, kick_str, fps, 0.13), 0, 1),
        "onset": np.clip(_impulse_env(n_frames, onset_idx, onset_str, fps, 0.18), 0, 1),
        "strong_onset": strong,
        "beat_frames": beat_frames,
        "hue": hue,
    }


def frame_chunk(feats, i):
    """The analysis window (centred on frame i) for the cymatics engine."""
    hop, win, audio = feats["hop"], feats["win"], feats["audio"]
    center = i * hop + hop // 2
    s = max(0, center - win // 2)
    return audio[s: s + win]
