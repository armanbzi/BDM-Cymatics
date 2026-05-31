#!/usr/bin/env python3
"""
Audio similarity search — record from microphone and find closest matches.

Records 5 seconds of audio from the default microphone, computes a PANNs CNN14
embedding (2048-dim), and searches the Milvus ``sound_audio_embeddings``
collection for the most acoustically similar recordings.

Orchestrator: option 9 → Data consumption → Audio classification.

Run directly:
    python data_consumption/tasks/audio_classification.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
_EZ_DIR = _PROJECT_ROOT / "exploitation_zone"
if str(_EZ_DIR) not in sys.path:
    sys.path.insert(1, str(_EZ_DIR))

try:
    from dotenv import load_dotenv

    load_dotenv(_PROJECT_ROOT / ".env")
except ImportError:
    pass

import numpy as np

# ── Constants ──────────────────────────────────────────────────────────────

SAMPLE_RATE = 44100
DURATION = 5
TOP_K = 5


# ── Audio recording ──────────────────────────────────────────────────────


def record_audio(duration: int = DURATION, sample_rate: int = SAMPLE_RATE) -> np.ndarray:
    """Record *duration* seconds of mono audio from the default microphone.

    Returns a 1-D float32 array normalised to [-1, 1].
    """
    import sounddevice as sd

    print(f"\n  Recording {duration}s of audio (sample rate {sample_rate} Hz)...")
    print("  Speak / play a sound now!\n")

    audio = sd.rec(
        int(duration * sample_rate),
        samplerate=sample_rate,
        channels=1,
        dtype="float32",
    )
    sd.wait()
    audio = audio.flatten()

    # Peak-normalise to [-1, 1] (same as warm-path).
    peak = np.abs(audio).max()
    if peak > 0:
        audio = audio / peak

    rms = np.sqrt(np.mean(audio ** 2))
    print(f"  Recording complete — {len(audio)} samples, RMS={rms:.4f}")
    return audio


# ── Result display ───────────────────────────────────────────────────────


def _format_result(rank: int, hit: dict) -> None:
    """Pretty-print a single search result."""
    entity = hit["entity"]
    distance = hit["distance"]

    uuid = entity.get("uuid", "?")
    category = entity.get("category", "") or "—"
    source = entity.get("source", "") or "—"
    peak_hz = entity.get("peak_frequency_hz", 0)
    symmetry = entity.get("symmetry_score", 0)

    print(f"  {rank}. Similarity: {distance:.4f}")
    print(f"     UUID:       {uuid}")
    print(f"     Category:   {category}")
    print(f"     Source:      {source}")
    print(f"     Peak freq:  {peak_hz:.0f} Hz")
    print(f"     Symmetry:   {symmetry:.3f}")
    print()


def display_results(results: list[dict]) -> None:
    """Display all search results in a formatted table."""
    width = 62
    print(f"\n{'═' * width}")
    print(f"  Audio Similarity Search Results — Top {len(results)}")
    print(f"{'─' * width}")

    if not results:
        print("  No matching recordings found.")
        print(f"{'═' * width}\n")
        return

    for i, hit in enumerate(results):
        _format_result(i + 1, hit)

    print(f"{'═' * width}\n")


# ── Search ───────────────────────────────────────────────────────────────


def search_recorded_audio(
    audio: np.ndarray,
    sample_rate: int = SAMPLE_RATE,
    top_k: int = TOP_K,
) -> list[dict]:
    """Embed the recorded audio with PANNs CNN14 and search Milvus."""
    from milvus_embeddings import connect_milvus, search_similar_sounds

    print("  Connecting to Milvus...")
    milvus_client = connect_milvus()

    print("  Computing PANNs CNN14 embedding (2048-dim)...")
    results = search_similar_sounds(milvus_client, audio, sample_rate, top_k=top_k)
    print(f"  Found {len(results)} similar recordings.")
    return results


# ── Interactive CLI ──────────────────────────────────────────────────────


def run_interactive(*, from_orchestrator: bool = False) -> None:
    """Main loop: record → search → display → repeat."""
    width = 62
    print(f"\n{'─' * width}")
    print("  Audio Classification — Similarity Search")
    print(f"{'─' * width}")
    print("  Records 5 seconds of audio from your microphone,")
    print("  then finds the most similar sounds in the Milvus")
    print("  audio embedding collection (PANNs CNN14, 2048-dim).")
    print(f"{'─' * width}")
    print()
    print("  Requirements:")
    print("    - Milvus running  (docker compose up -d milvus)")
    print("    - Audio embeddings ingested  (orchestrate → [10])")
    print()

    while True:
        try:
            choice = input("  Press Enter to record, or [q] to quit: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Leaving audio classification.")
            break

        if choice in ("q", "quit", "exit"):
            break

        try:
            audio = record_audio()
        except Exception as e:
            print(f"\n  Recording failed: {e}")
            print("  Check that a microphone is connected and sounddevice is installed.")
            continue

        try:
            results = search_recorded_audio(audio)
        except Exception as e:
            print(f"\n  Search failed: {e}")
            print("  Check that Milvus is running and embeddings have been ingested.")
            continue

        display_results(results)


def main() -> None:
    run_interactive(from_orchestrator=False)


if __name__ == "__main__":
    main()
