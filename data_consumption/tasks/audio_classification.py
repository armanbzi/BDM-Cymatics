#!/usr/bin/env python3
"""
Audio classification and similarity search — microphone → label + neighbours.

Records 5 seconds of audio from the default microphone, computes a PANNs CNN14
embedding (2048-dim) and serves a two-part answer:

  1. The trained audio classifier head (logistic regression on PANNs vectors,
     persisted to MinIO by classifier_training.py) returns a predicted
     category together with a calibrated confidence.
  2. The same embedding is passed to the Milvus ``sound_audio_embeddings``
     collection, returning the top-k acoustically nearest recordings as
     supporting evidence behind the prediction.

When the trained head is not yet available (e.g. the exploitation pipeline
has not run with enough labelled data), the task gracefully falls back to
ANN-only search.

Orchestrator: option 8 → Data consumption → Audio classification.
Or in the provided Streamlit dashboard.

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

# ── Constants

SAMPLE_RATE = 44100
DURATION = 5
TOP_K = 5


# ── Audio recording — capture 5 s from the default microphone


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


# ── Result display — Milvus similarity results


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


def display_results(
    prediction: tuple[str, float] | None,
    results: list[dict],
) -> None:
    """Display the trained-head prediction and the ANN neighbours."""
    width = 62
    print(f"\n{'═' * width}")
    print("  Audio Classification Results")
    print(f"{'─' * width}")

    if prediction is not None:
        label, confidence = prediction
        print(f"  Predicted category: {label}")
        print(f"  Confidence:         {confidence:.1%}")
        print(f"{'─' * width}")
    else:
        print("  No trained head available — showing nearest neighbours only.")
        print(f"{'─' * width}")

    print(f"  Top {len(results)} acoustically similar recordings (supporting evidence)")
    print(f"{'─' * width}")

    if not results:
        print("  No matching recordings found.")
        print(f"{'═' * width}\n")
        return

    for i, hit in enumerate(results):
        _format_result(i + 1, hit)

    print(f"{'═' * width}\n")


# ── Search — embed once via PANNs CNN14, then predict + ANN search


def classify_recorded_audio(
    audio: np.ndarray,
    classifier: object | None,
    sample_rate: int = SAMPLE_RATE,
    top_k: int = TOP_K,
) -> tuple[tuple[str, float] | None, list[dict]]:
    """Embed audio once, run the trained head (if available), then ANN search.

    Returns ``(prediction, neighbours)`` where ``prediction`` is
    ``(label, confidence)`` or ``None`` when the classifier head is absent.
    """
    from milvus_embeddings import (
        compute_audio_embedding,
        connect_milvus,
        search_audio_by_embedding,
    )
    from classifier_training import predict_with_head

    print("  Connecting to Milvus...")
    milvus_client = connect_milvus()

    print("  Computing PANNs CNN14 embedding (2048-dim)...")
    embedding = compute_audio_embedding(audio, sample_rate)

    prediction: tuple[str, float] | None = None
    if classifier is not None:
        try:
            label, confidence = predict_with_head(classifier, embedding)
            prediction = (label, confidence)
            print(f"  Trained head prediction: {label}  ({confidence:.1%} confidence)")
        except Exception as e:
            print(f"  Trained head prediction failed: {e}")
    else:
        print("  Trained head not loaded — returning nearest neighbours only.")

    print(f"  Searching Milvus for top-{top_k} acoustically similar recordings...")
    results = search_audio_by_embedding(milvus_client, embedding, top_k=top_k)
    print(f"  Found {len(results)} similar recordings.")
    return prediction, results


# ── Interactive CLI — record → embed → search → display loop


def run_interactive(*, from_orchestrator: bool = False) -> None:
    """Main loop: record → predict (trained head) + ANN search → display."""
    width = 62
    print(f"\n{'─' * width}")
    print("  Audio Classification — Trained Head + Nearest Neighbours")
    print(f"{'─' * width}")
    print("  Records 5 seconds of audio from your microphone, computes a")
    print("  PANNs CNN14 embedding (2048-dim), runs the trained classifier")
    print("  head for the predicted category, and returns the top-k")
    print("  acoustically similar recordings as supporting evidence.")
    print(f"{'─' * width}")
    print()
    print("  Requirements:")
    print("    - Milvus running  (docker compose up -d milvus)")
    print("    - Audio embeddings ingested  (orchestrate → [6])")
    print("    - Trained head at exploitation-zone/models/audio_classifier.joblib")
    print("      (optional — falls back to ANN-only when missing)")
    print()

    # Load the trained head once for the whole session. Falls back gracefully
    # to ANN-only behaviour when the artefact is missing on MinIO.
    from shared.minio_helpers import create_minio_client
    from classifier_training import load_classifier_from_minio

    classifier = None
    try:
        minio_client = create_minio_client()
        loaded = load_classifier_from_minio(minio_client, "audio")
        if loaded is not None:
            classifier, metrics = loaded
            n_classes = metrics.get("n_classes", "?")
            cv_acc = metrics.get("cv_accuracy_mean")
            cv_str = f"CV acc {cv_acc:.3f}" if isinstance(cv_acc, (int, float)) else "CV acc n/a"
            print(f"  Loaded trained head — {n_classes} classes, {cv_str}.")
        else:
            print("  No trained head found on MinIO — running in ANN-only mode.")
    except Exception as e:
        print(f"  Could not load trained head ({e}) — running in ANN-only mode.")

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
            prediction, results = classify_recorded_audio(audio, classifier)
        except Exception as e:
            print(f"\n  Classification failed: {e}")
            print("  Check that Milvus is running and embeddings have been ingested.")
            continue

        display_results(prediction, results)


def main() -> None:
    run_interactive(from_orchestrator=False)


if __name__ == "__main__":
    main()
