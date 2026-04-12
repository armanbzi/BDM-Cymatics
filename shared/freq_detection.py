"""
Shared frequency-detection helpers (harmonic-aware peak detection).

Used by warm, hot, cold, and trusted-zone stages.
"""

from collections import Counter

import numpy as np
from scipy.signal import find_peaks

DEFAULT_SAMPLE_RATE = 44100


def harmonic_dominant_freq(window_candidates, max_harmonics=5,
                           use_energy_weight=True):
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
    return float(max(candidate_counts.keys(),
                     key=lambda k: candidate_counts[k]))


def dominant_freq_hz_for_chunk(chunk, sample_rate=DEFAULT_SAMPLE_RATE):
    if len(chunk) < 64:
        return 0.0
    fft = np.abs(np.fft.rfft(chunk))
    fft_norm = fft / (np.max(fft) + 1e-12)
    pidx, _ = find_peaks(fft_norm, height=0.06, distance=6, prominence=0.04)
    if len(pidx) == 0:
        pidx = np.array([np.argmax(fft_norm)])
    top_idx = pidx[np.argmax(fft_norm[pidx])]
    return float(top_idx * sample_rate / len(chunk))


def build_window_candidates(audio, sample_rate=DEFAULT_SAMPLE_RATE):
    """Sliding 0.25 s windows → list of (freq_bin, chunk, offset, energy, hz)."""
    audio = np.asarray(audio, dtype=np.float64).flatten()
    if len(audio) < 64:
        return []
    window_len = int(sample_rate * 0.25)
    hop = max(1, window_len // 2)
    candidates = []
    if len(audio) > window_len:
        for off in range(0, len(audio) - window_len, hop):
            chunk = audio[off: off + window_len]
            hz = dominant_freq_hz_for_chunk(chunk, sample_rate)
            freq_bin = int(round(hz))
            energy = float(np.sum(np.abs(np.fft.rfft(chunk)) ** 2))
            candidates.append((freq_bin, chunk.copy(), off, energy, hz))
    else:
        hz = dominant_freq_hz_for_chunk(audio, sample_rate)
        freq_bin = int(round(hz))
        energy = float(np.sum(np.abs(np.fft.rfft(audio)) ** 2))
        candidates.append((freq_bin, audio.copy(), 0, energy, hz))
    return candidates


def detect_peak_freq(audio, sample_rate=DEFAULT_SAMPLE_RATE):
    """Return dominant frequency (Hz) using sliding-window harmonic voting."""
    candidates = build_window_candidates(audio, sample_rate)
    if not candidates:
        return 0.0
    return harmonic_dominant_freq(candidates, max_harmonics=4,
                                  use_energy_weight=True)
