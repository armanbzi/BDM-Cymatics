#!/usr/bin/env python3
"""
Metadata semantic search — natural-language queries over sound metadata.

Uses the all-MiniLM-L6-v2 text embeddings in the Milvus
``sound_text_embeddings`` collection to find recordings whose
auto-generated descriptions best match a free-form question.

Orchestrator: option 9 → Data consumption → Metadata search.

Run directly:
    python data_consumption/tasks/search_metadata.py
"""

from __future__ import annotations

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

# ── Constants ──────────────────────────────────────────────────────────────

TOP_K = 5


# ── Result display ───────────────────────────────────────────────────────


def _format_result(rank: int, hit: dict) -> None:
    """Pretty-print a single search result."""
    entity = hit["entity"]
    distance = hit["distance"]

    uuid = entity.get("uuid", "?")
    category = entity.get("category", "") or "—"
    source = entity.get("source", "") or "—"
    peak_hz = entity.get("peak_frequency_hz", 0)
    description = entity.get("description_text", "") or "—"

    print(f"  {rank}. Similarity: {distance:.4f}")
    print(f"     UUID:       {uuid}")
    print(f"     Category:   {category}")
    print(f"     Source:      {source}")
    print(f"     Peak freq:  {peak_hz:.0f} Hz")
    print(f"     Description:")
    for line in _wrap_text(description, width=55):
        print(f"       {line}")
    print()


def _wrap_text(text: str, width: int = 55) -> list[str]:
    """Word-wrap *text* into lines of at most *width* characters."""
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        if current and len(current) + 1 + len(word) > width:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}" if current else word
    if current:
        lines.append(current)
    return lines or [""]


def display_results(results: list[dict], query: str) -> None:
    """Display all search results in a formatted table."""
    width = 62
    print(f"\n{'═' * width}")
    print(f"  Metadata Search — Top {len(results)}")
    print(f"  Query: \"{query}\"")
    print(f"{'─' * width}")

    if not results:
        print("  No matching recordings found.")
        print(f"{'═' * width}\n")
        return

    for i, hit in enumerate(results):
        _format_result(i + 1, hit)

    print(f"{'═' * width}\n")


# ── Search ───────────────────────────────────────────────────────────────


def search_metadata(query: str, top_k: int = TOP_K) -> list[dict]:
    """Embed the query with all-MiniLM-L6-v2 and search Milvus."""
    from milvus_embeddings import connect_milvus, search_by_text

    print("  Connecting to Milvus...")
    milvus_client = connect_milvus()

    print("  Computing text embedding (384-dim)...")
    results = search_by_text(milvus_client, query, top_k=top_k)
    print(f"  Found {len(results)} matching recording(s).")
    return results


# ── Interactive CLI ──────────────────────────────────────────────────────


def run_interactive(*, from_orchestrator: bool = False) -> None:
    """Main loop: ask → search → display → repeat."""
    width = 62
    print(f"\n{'─' * width}")
    print("  Metadata Search — Natural Language Query")
    print(f"{'─' * width}")
    print("  Ask questions about your sound library in plain English.")
    print("  Powered by all-MiniLM-L6-v2 text embeddings (384-dim)")
    print("  over auto-generated sound descriptions.")
    print(f"{'─' * width}")
    print()
    print("  Requirements:")
    print("    - Milvus running  (docker compose up -d milvus)")
    print("    - Text embeddings ingested  (orchestrate → [10])")
    print()
    print("  Example queries:")
    print("    - what frequency do rain sounds have?")
    print("    - which sounds are highly harmonic?")
    print("    - low frequency tonal sounds")
    print("    - complex noise-like high frequency recordings")
    print()

    while True:
        try:
            query = input("  Enter your question (or [q] to quit): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Leaving metadata search.")
            break

        if query.lower() in ("q", "quit", "exit"):
            break
        if not query:
            print("  No query provided.")
            continue

        try:
            results = search_metadata(query)
        except Exception as e:
            print(f"\n  Search failed: {e}")
            print("  Check that Milvus is running and text embeddings have been ingested.")
            continue

        display_results(results, query)


def main() -> None:
    run_interactive(from_orchestrator=False)


if __name__ == "__main__":
    main()
