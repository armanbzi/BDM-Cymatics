#!/usr/bin/env python3
"""
Cymatics pattern search — find similar visual patterns via CLIP embeddings.

Three search modes:
  1. Upload an image   → CLIP image embedding → Milvus ANN search.
  2. Record 5 s audio  → generate cymatics image → CLIP → Milvus.
  3. Text query         → CLIP text embedding → Milvus (text-to-image).

Orchestrator: option 9 → Data consumption → Cymatics classification.

Run directly:
    python data_consumption/tasks/cymatics_classification.py
"""

from __future__ import annotations

import os
import sys
import tempfile
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
IMG_SIM = 900
IMG_RES = 2048


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

    peak = np.abs(audio).max()
    if peak > 0:
        audio = audio / peak

    rms = np.sqrt(np.mean(audio ** 2))
    print(f"  Recording complete — {len(audio)} samples, RMS={rms:.4f}")
    return audio


# ── Cymatics image generation ────────────────────────────────────────────


def generate_cymatics_image(audio: np.ndarray, sample_rate: int = SAMPLE_RATE) -> bytes:
    """Generate a cymatics PNG from a float32 audio array.

    Replicates the trusted-zone cymatics pipeline:
      freq detection → simulation at 900 px → upscale to 2048 × 2048.

    Returns raw PNG bytes (suitable for CLIP embedding or display).
    """
    import cv2
    from shared.cymatics_engine import (
        N_ZONES,
        build_zones,
        build_zone_sources,
        analyse_frame,
        compute_interference,
        displacement_to_brightness,
        render_composite,
    )
    from shared.freq_detection import (
        build_window_candidates,
        harmonic_dominant_freq,
    )

    # Normalise to float64 [-1, 1].
    audio = audio.astype(np.float64)
    pk = np.max(np.abs(audio))
    if pk > 1e-6:
        audio /= pk

    # Detect dominant frequency and find best 0.25 s chunk.
    candidates = build_window_candidates(audio, sample_rate)
    dom_freq = harmonic_dominant_freq(
        candidates, max_harmonics=4, use_energy_weight=True,
    )
    dom_bin = int(round(dom_freq))

    if dom_bin > 0:
        matching = [
            c for c in candidates
            if c[0] > 0 and c[0] % dom_bin == 0
        ]
        best = max(matching, key=lambda c: c[3]) if matching else candidates[0]
    else:
        best = max(candidates, key=lambda c: c[3]) if candidates else candidates[0]

    peak_chunk = np.asarray(best[1], dtype=np.float64)
    peak_rms = float(np.sqrt(np.mean(peak_chunk ** 2)))
    peak_time = best[2] / sample_rate
    peak_freq = best[0]

    print(f"  Detected peak frequency: {peak_freq} Hz")

    # Build simulation geometry.
    iz = build_zones(IMG_SIM)
    i_sources = build_zone_sources(IMG_SIM, iz)

    img_zone_br: list = []
    for zi in range(N_ZONES):
        _, sfreqs, _ = analyse_frame(peak_chunk, zi, sample_rate)
        disp_x, disp_y = compute_interference(
            i_sources[zi], sfreqs, peak_time, IMG_SIM,
        )
        br = displacement_to_brightness(
            disp_x, disp_y, iz["masks"][zi], IMG_SIM, peak_rms, 0.0,
        )
        img_zone_br.append(br)

    raw_img = render_composite(img_zone_br, iz, 0.0, IMG_SIM)
    img = cv2.resize(raw_img, (IMG_RES, IMG_RES), interpolation=cv2.INTER_LANCZOS4)

    # Glow post-processing (matches trusted zone).
    glow = cv2.GaussianBlur(img, (0, 0), sigmaX=14)
    img = cv2.addWeighted(img, 0.82, glow, 0.30, 0)

    # Encode to PNG bytes.
    success, buf = cv2.imencode(".png", img)
    if not success:
        raise RuntimeError("Failed to encode cymatics image to PNG.")
    return bytes(buf)


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
    image_path = entity.get("image_path", "") or "—"

    print(f"  {rank}. Similarity: {distance:.4f}")
    print(f"     UUID:       {uuid}")
    print(f"     Category:   {category}")
    print(f"     Source:      {source}")
    print(f"     Peak freq:  {peak_hz:.0f} Hz")
    print(f"     Symmetry:   {symmetry:.3f}")
    print(f"     Image:      {image_path}")
    print()


def display_results(results: list[dict], title: str) -> None:
    """Display all search results in a formatted table."""
    width = 62
    print(f"\n{'═' * width}")
    print(f"  {title} — Top {len(results)}")
    print(f"{'─' * width}")

    if not results:
        print("  No matching patterns found.")
        print(f"{'═' * width}\n")
        return

    for i, hit in enumerate(results):
        _format_result(i + 1, hit)

    print(f"{'═' * width}\n")


# ── Search modes ─────────────────────────────────────────────────────────


def _connect_milvus():
    """Connect to Milvus and return the client."""
    from milvus_embeddings import connect_milvus

    return connect_milvus()


def search_by_image_path(milvus_client, image_path: str, top_k: int = TOP_K) -> list[dict]:
    """Load a local image file and search by CLIP image embedding."""
    from milvus_embeddings import search_similar_patterns

    with open(image_path, "rb") as f:
        image_bytes = f.read()

    print(f"  Image: {image_path} ({len(image_bytes) / 1024:.1f} KB)")
    print("  Computing CLIP image embedding (512-dim)...")
    return search_similar_patterns(milvus_client, image_bytes, top_k=top_k)


def search_by_recorded_audio(milvus_client, top_k: int = TOP_K) -> list[dict]:
    """Record audio, generate cymatics image, search by CLIP image embedding."""
    from milvus_embeddings import search_similar_patterns

    audio = record_audio()

    print("\n  Generating cymatics pattern from recorded audio...")
    image_bytes = generate_cymatics_image(audio)
    print(f"  Cymatics image generated ({len(image_bytes) / 1024:.1f} KB)")

    # Save preview so the user can see what was generated.
    preview_path = os.path.join(tempfile.gettempdir(), "cymatics_preview.png")
    with open(preview_path, "wb") as f:
        f.write(image_bytes)
    print(f"  Preview saved: {preview_path}")

    print("  Computing CLIP image embedding (512-dim)...")
    return search_similar_patterns(milvus_client, image_bytes, top_k=top_k)


def search_by_text_query(milvus_client, query: str, top_k: int = TOP_K) -> list[dict]:
    """Search cymatics patterns using a natural-language description."""
    from milvus_embeddings import search_patterns_by_text

    print(f"  Query: \"{query}\"")
    print("  Computing CLIP text embedding (512-dim)...")
    return search_patterns_by_text(milvus_client, query, top_k=top_k)


# ── Interactive CLI ──────────────────────────────────────────────────────


def _print_menu() -> None:
    width = 62
    print(f"\n{'─' * width}")
    print("  Search modes:")
    print(f"{'─' * width}")
    print("   [1]  Upload an image (provide file path)")
    print("   [2]  Record audio → generate cymatics → search")
    print("   [3]  Text query (describe a pattern)")
    print("   [b]  Back")
    print()


def run_interactive(*, from_orchestrator: bool = False) -> None:
    """Main loop: choose mode → search → display → repeat."""
    width = 62
    print(f"\n{'─' * width}")
    print("  Cymatics Classification — Pattern Search")
    print(f"{'─' * width}")
    print("  Find similar cymatics patterns using CLIP ViT-B/32")
    print("  embeddings on the Milvus cymatics collection.")
    print(f"{'─' * width}")
    print()
    print("  Requirements:")
    print("    - Milvus running  (docker compose up -d milvus)")
    print("    - Cymatics embeddings ingested  (orchestrate → [10])")
    print()

    try:
        milvus_client = _connect_milvus()
    except Exception as e:
        print(f"\n  Failed to connect to Milvus: {e}")
        print("  Start Milvus with: docker compose up -d milvus")
        if not from_orchestrator:
            raise SystemExit(1) from e
        return

    while True:
        _print_menu()
        try:
            choice = input("  Select mode [1-3, b]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Leaving cymatics classification.")
            break

        if choice in ("b", "q", "quit", "exit", ""):
            break

        results = None

        if choice == "1":
            try:
                path = input("  Enter image file path: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                continue
            if not path:
                print("  No path provided.")
                continue
            path = os.path.expanduser(path)
            if not os.path.isfile(path):
                print(f"  File not found: {path}")
                continue
            try:
                results = search_by_image_path(milvus_client, path)
            except Exception as e:
                print(f"\n  Image search failed: {e}")
                continue

        elif choice == "2":
            try:
                results = search_by_recorded_audio(milvus_client)
            except Exception as e:
                print(f"\n  Audio search failed: {e}")
                continue

        elif choice == "3":
            try:
                query = input("  Enter text query: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                continue
            if not query:
                print("  No query provided.")
                continue
            try:
                results = search_by_text_query(milvus_client, query)
            except Exception as e:
                print(f"\n  Text search failed: {e}")
                continue

        else:
            print("  Invalid choice.")
            continue

        if results is not None:
            titles = {
                "1": "Image Pattern Search Results",
                "2": "Audio → Cymatics Pattern Search Results",
                "3": "Text → Pattern Search Results",
            }
            display_results(results, titles.get(choice, "Search Results"))


def main() -> None:
    run_interactive(from_orchestrator=False)


if __name__ == "__main__":
    main()
