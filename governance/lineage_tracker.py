#!/usr/bin/env python3
"""
Data Governance — Lightweight lineage tracking.

Builds and displays a lineage table that traces every data record (UUID)
across all zones, showing:

  • Which zone(s) the record appears in (Landing → Trusted → Exploitation).
  • What assets were generated at each stage (audio, image, video, embeddings).
  • Timestamps and transformation metadata at each step.
  • Completeness status — whether the full pipeline has been traversed.

The lineage table is built by cross-referencing metadata CSVs from all
three MinIO buckets and checking Milvus embedding collections.

Orchestrator: option 11 → Data governance → Lineage tracking.

Run directly:
    python governance/lineage_tracker.py
"""

from __future__ import annotations

import io
import json
import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
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

import pandas as pd

from shared.minio_helpers import create_minio_client

# ── Constants ──────────────────────────────────────────────────────────────

LANDING_BUCKET = os.environ.get("LANDING_ZONE_BUCKET", "landing-zone")
TRUSTED_BUCKET = os.environ.get("TRUSTED_ZONE_BUCKET", "trusted-zone")
EXPLOITATION_BUCKET = os.environ.get("EXPLOITATION_ZONE_BUCKET", "exploitation-zone")
METADATA_KEY = "metadata/observations.csv"

LINEAGE_KEY = "governance/lineage.json"


# ── Helpers ───────────────────────────────────────────────────────────────


def _load_csv_safe(minio_client, bucket: str) -> pd.DataFrame:
    """Load a metadata CSV from MinIO. Returns empty DataFrame on failure."""
    try:
        resp = minio_client.get_object(bucket, METADATA_KEY)
        data = resp.read()
        resp.close()
        resp.release_conn()
        return pd.read_csv(io.BytesIO(data))
    except Exception:
        return pd.DataFrame()


def _check_milvus_uuids() -> dict[str, set[str]]:
    """Return a dict mapping collection name → set of UUIDs stored."""
    collections: dict[str, set[str]] = {}
    try:
        from milvus_embeddings import connect_milvus

        client = connect_milvus()
        for coll in [
            "sound_audio_embeddings",
            "sound_text_embeddings",
            "sound_cymatics_embeddings",
        ]:
            if not client.has_collection(coll):
                collections[coll] = set()
                continue
            stats = client.get_collection_stats(coll)
            count = int(stats.get("row_count", 0))
            if count == 0:
                collections[coll] = set()
                continue
            # Query all UUIDs (primary key)
            results = client.query(
                collection_name=coll,
                filter="",
                output_fields=["uuid"],
                limit=count,
            )
            collections[coll] = {r["uuid"] for r in results}
    except Exception:
        pass
    return collections


# ── Lineage builder ──────────────────────────────────────────────────────


def build_lineage(minio_client) -> list[dict]:
    """Build a lineage record for every UUID found across all zones.

    Returns a list of dicts, one per UUID, with zone presence flags,
    asset paths, and transformation metadata.
    """
    print("  Loading metadata from all zones...")

    landing_df = _load_csv_safe(minio_client, LANDING_BUCKET)
    trusted_df = _load_csv_safe(minio_client, TRUSTED_BUCKET)
    exploit_df = _load_csv_safe(minio_client, EXPLOITATION_BUCKET)

    print(f"    Landing:      {len(landing_df)} rows")
    print(f"    Trusted:      {len(trusted_df)} rows")
    print(f"    Exploitation: {len(exploit_df)} rows")

    # Index by UUID for fast lookup.
    landing_idx = {}
    if not landing_df.empty and "uuid" in landing_df.columns:
        for _, row in landing_df.iterrows():
            uid = str(row.get("uuid", "")).strip()
            if uid:
                landing_idx[uid] = row.to_dict()

    trusted_idx = {}
    if not trusted_df.empty and "uuid" in trusted_df.columns:
        for _, row in trusted_df.iterrows():
            uid = str(row.get("uuid", "")).strip()
            if uid:
                trusted_idx[uid] = row.to_dict()

    exploit_idx = {}
    if not exploit_df.empty and "uuid" in exploit_df.columns:
        for _, row in exploit_df.iterrows():
            uid = str(row.get("uuid", "")).strip()
            if uid:
                exploit_idx[uid] = row.to_dict()

    # Check Milvus embeddings.
    print("  Checking Milvus embedding collections...")
    milvus_uuids = _check_milvus_uuids()
    audio_emb_uuids = milvus_uuids.get("sound_audio_embeddings", set())
    text_emb_uuids = milvus_uuids.get("sound_text_embeddings", set())
    cymatics_emb_uuids = milvus_uuids.get("sound_cymatics_embeddings", set())

    print(f"    Audio embeddings:    {len(audio_emb_uuids)} UUIDs")
    print(f"    Text embeddings:     {len(text_emb_uuids)} UUIDs")
    print(f"    Cymatics embeddings: {len(cymatics_emb_uuids)} UUIDs")

    # Collect all UUIDs.
    all_uuids = sorted(
        set(landing_idx.keys())
        | set(trusted_idx.keys())
        | set(exploit_idx.keys())
        | audio_emb_uuids
        | text_emb_uuids
        | cymatics_emb_uuids
    )

    print(f"\n  Building lineage for {len(all_uuids)} unique records...")

    lineage: list[dict] = []

    for uid in all_uuids:
        landing = landing_idx.get(uid)
        trusted = trusted_idx.get(uid)
        exploit = exploit_idx.get(uid)

        # Determine source and category.
        source = "—"
        category = "—"
        if landing:
            source = str(landing.get("source", "")) or "—"
            category = str(landing.get("category", "")) or "—"
        if trusted and category == "—":
            category = str(trusted.get("category", "")) or "—"

        # Zone presence.
        in_landing = uid in landing_idx
        in_trusted = uid in trusted_idx
        in_exploitation = uid in exploit_idx

        # Completeness: how far through the pipeline.
        stages_completed = []
        if in_landing:
            stages_completed.append("landing")
        if in_trusted:
            stages_completed.append("trusted")
        if in_exploitation:
            stages_completed.append("exploitation")

        # Assets generated.
        assets: dict[str, str | None] = {
            "landing_audio": landing.get("audio_path") if landing else None,
            "trusted_audio": trusted.get("audio_path") if trusted else None,
            "trusted_image": trusted.get("image_path") if trusted else None,
            "trusted_video": trusted.get("video_path") if trusted else None,
            "audio_embedding": "milvus" if uid in audio_emb_uuids else None,
            "text_embedding": "milvus" if uid in text_emb_uuids else None,
            "cymatics_embedding": "milvus" if uid in cymatics_emb_uuids else None,
        }

        # Transformation chain.
        transformations: list[str] = []
        if in_landing:
            transformations.append(
                f"Ingested via {source} → landing-zone audio"
            )
        if in_trusted:
            proc_ver = str(trusted.get("processing_version", "")) if trusted else ""
            transformations.append(
                f"Spark QA (dedup, schema) → trusted-zone "
                f"(image + video + peak detection, v{proc_ver})"
            )
        if in_exploitation:
            feat_ver = str(exploit.get("feature_version", "")) if exploit else ""
            transformations.append(
                f"Spark spectral features + Python MFCCs → "
                f"exploitation-zone (v{feat_ver})"
            )
        if uid in audio_emb_uuids:
            transformations.append("PANNs CNN14 → audio embedding (2048-dim)")
        if uid in text_emb_uuids:
            transformations.append("all-MiniLM-L6-v2 → text embedding (384-dim)")
        if uid in cymatics_emb_uuids:
            transformations.append("CLIP ViT-B/32 → cymatics embedding (512-dim)")

        # Pipeline completeness.
        total_stages = 6  # landing, trusted, exploitation, 3 embeddings
        completed = (
            int(in_landing) + int(in_trusted) + int(in_exploitation)
            + int(uid in audio_emb_uuids)
            + int(uid in text_emb_uuids)
            + int(uid in cymatics_emb_uuids)
        )
        completeness = completed / total_stages

        record = {
            "uuid": uid,
            "source": source,
            "category": category,
            "in_landing": in_landing,
            "in_trusted": in_trusted,
            "in_exploitation": in_exploitation,
            "has_audio_embedding": uid in audio_emb_uuids,
            "has_text_embedding": uid in text_emb_uuids,
            "has_cymatics_embedding": uid in cymatics_emb_uuids,
            "stages_completed": stages_completed,
            "assets": assets,
            "transformations": transformations,
            "completeness": completeness,
        }

        lineage.append(record)

    return lineage


# ── Persist lineage ──────────────────────────────────────────────────────


def save_lineage(minio_client, lineage: list[dict]) -> str:
    """Save lineage JSON to the exploitation-zone bucket in MinIO."""
    payload = json.dumps(lineage, indent=2, default=str).encode("utf-8")
    from io import BytesIO

    minio_client.put_object(
        EXPLOITATION_BUCKET,
        LINEAGE_KEY,
        BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    path = f"{EXPLOITATION_BUCKET}/{LINEAGE_KEY}"
    print(f"\n  Lineage table saved: {path} ({len(payload) / 1024:.1f} KB)")
    return path


# ── Display ──────────────────────────────────────────────────────────────


def display_lineage(lineage: list[dict]) -> None:
    """Pretty-print the lineage table."""
    width = 62
    print(f"\n{'═' * width}")
    print(f"  Data Lineage — {len(lineage)} records")
    print(f"{'─' * width}")

    if not lineage:
        print("  No lineage records found.")
        print(f"{'═' * width}\n")
        return

    # Summary statistics.
    full_pipeline = sum(1 for r in lineage if r["completeness"] == 1.0)
    partial = sum(1 for r in lineage if 0 < r["completeness"] < 1.0)
    landing_only = sum(
        1 for r in lineage
        if r["in_landing"] and not r["in_trusted"] and not r["in_exploitation"]
    )

    print(f"  Full pipeline (6/6):   {full_pipeline}")
    print(f"  Partial:               {partial}")
    print(f"  Landing only:          {landing_only}")
    print(f"{'─' * width}")

    # Show first 10 records in detail.
    show_count = min(len(lineage), 10)
    print(f"\n  Showing first {show_count} records:\n")

    for i, rec in enumerate(lineage[:show_count]):
        uid = rec["uuid"]
        comp = rec["completeness"]
        stages = " → ".join(rec["stages_completed"]) or "—"

        bar_len = 20
        filled = int(comp * bar_len)
        bar = "█" * filled + "░" * (bar_len - filled)

        print(f"  {i + 1}. {uid[:24]}…")
        print(f"     Category:   {rec['category']}")
        print(f"     Source:     {rec['source']}")
        print(f"     Stages:     {stages}")
        print(f"     Progress:   [{bar}] {comp:.0%}")
        print(f"     Chain:")
        for t in rec["transformations"]:
            print(f"       → {t}")
        print()

    if len(lineage) > show_count:
        print(f"  ... and {len(lineage) - show_count} more records.")

    print(f"{'═' * width}\n")


def display_lineage_for_uuid(lineage: list[dict], target_uuid: str) -> None:
    """Display detailed lineage for a single UUID."""
    matches = [r for r in lineage if r["uuid"] == target_uuid]
    if not matches:
        print(f"\n  UUID not found: {target_uuid}")
        return

    rec = matches[0]
    width = 62
    print(f"\n{'═' * width}")
    print(f"  Lineage for {rec['uuid']}")
    print(f"{'─' * width}")
    print(f"  Category:    {rec['category']}")
    print(f"  Source:       {rec['source']}")
    print(f"  Completeness: {rec['completeness']:.0%}")
    print(f"{'─' * width}")

    print(f"\n  Zone presence:")
    print(f"    Landing:      {'✓' if rec['in_landing'] else '✗'}")
    print(f"    Trusted:      {'✓' if rec['in_trusted'] else '✗'}")
    print(f"    Exploitation: {'✓' if rec['in_exploitation'] else '✗'}")

    print(f"\n  Embeddings:")
    print(f"    Audio (PANNs):    {'✓' if rec['has_audio_embedding'] else '✗'}")
    print(f"    Text (MiniLM):    {'✓' if rec['has_text_embedding'] else '✗'}")
    print(f"    Cymatics (CLIP):  {'✓' if rec['has_cymatics_embedding'] else '✗'}")

    print(f"\n  Assets:")
    for name, path in rec["assets"].items():
        status = path if path else "—"
        print(f"    {name:<24} {status}")

    print(f"\n  Transformation chain:")
    for j, t in enumerate(rec["transformations"], 1):
        print(f"    {j}. {t}")

    print(f"\n{'═' * width}\n")


# ── Interactive CLI ──────────────────────────────────────────────────────


def _print_menu() -> None:
    width = 62
    print(f"\n{'─' * width}")
    print("  Lineage tracking options:")
    print(f"{'─' * width}")
    print("   [1]  Build & display full lineage table")
    print("   [2]  Look up lineage for a specific UUID")
    print("   [3]  Show pipeline completeness summary")
    print("   [b]  Back")
    print()


def run_interactive(*, from_orchestrator: bool = False) -> None:
    """Main loop: build lineage → browse → repeat."""
    width = 62
    print(f"\n{'─' * width}")
    print("  Data Governance — Lineage Tracking")
    print(f"{'─' * width}")
    print("  Traces every record (UUID) across all pipeline zones:")
    print("  Landing → Trusted → Exploitation → Milvus embeddings")
    print(f"{'─' * width}")
    print()
    print("  Requirements:")
    print("    - MinIO running  (docker compose up -d minio)")
    print("    - At least one zone has processed data")
    print()

    try:
        minio_client = create_minio_client()
    except Exception as e:
        print(f"\n  Failed to connect to MinIO: {e}")
        print("  Start MinIO with: docker compose up -d minio")
        if not from_orchestrator:
            raise SystemExit(1) from e
        return

    lineage: list[dict] | None = None

    while True:
        _print_menu()
        try:
            choice = input("  Select option [1-3, b]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Leaving lineage tracking.")
            break

        if choice in ("b", "q", "quit", "exit", ""):
            break

        if choice == "1":
            lineage = build_lineage(minio_client)
            save_lineage(minio_client, lineage)
            display_lineage(lineage)

        elif choice == "2":
            if lineage is None:
                print("\n  Building lineage first...")
                lineage = build_lineage(minio_client)

            try:
                target = input("  Enter UUID (or prefix): ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                continue
            if not target:
                print("  No UUID provided.")
                continue

            # Support prefix matching.
            matches = [r for r in lineage if r["uuid"].startswith(target)]
            if len(matches) == 0:
                print(f"  No records matching '{target}'.")
            elif len(matches) == 1:
                display_lineage_for_uuid(lineage, matches[0]["uuid"])
            else:
                print(f"\n  Found {len(matches)} matches:")
                for m in matches[:10]:
                    comp = m["completeness"]
                    print(f"    {m['uuid'][:36]}…  {m['category']:<20} {comp:.0%}")
                if len(matches) > 10:
                    print(f"    ... and {len(matches) - 10} more.")
                try:
                    pick = input("\n  Enter full UUID: ").strip()
                except (EOFError, KeyboardInterrupt):
                    print()
                    continue
                if pick:
                    display_lineage_for_uuid(lineage, pick)

        elif choice == "3":
            if lineage is None:
                print("\n  Building lineage first...")
                lineage = build_lineage(minio_client)

            width = 62
            print(f"\n{'═' * width}")
            print("  Pipeline Completeness Summary")
            print(f"{'─' * width}")

            total = len(lineage)
            if total == 0:
                print("  No records found.")
                print(f"{'═' * width}\n")
                continue

            by_completeness: dict[str, int] = {}
            for r in lineage:
                pct = f"{r['completeness']:.0%}"
                by_completeness[pct] = by_completeness.get(pct, 0) + 1

            for pct in sorted(by_completeness.keys(), reverse=True):
                count = by_completeness[pct]
                bar_len = 30
                filled = int(count / total * bar_len)
                bar = "█" * filled + "░" * (bar_len - filled)
                print(f"  {pct:>5}  [{bar}]  {count}/{total}")

            # Per-stage counts.
            print(f"\n{'─' * width}")
            print(f"  Per-stage presence:")
            in_l = sum(1 for r in lineage if r["in_landing"])
            in_t = sum(1 for r in lineage if r["in_trusted"])
            in_e = sum(1 for r in lineage if r["in_exploitation"])
            has_ae = sum(1 for r in lineage if r["has_audio_embedding"])
            has_te = sum(1 for r in lineage if r["has_text_embedding"])
            has_ce = sum(1 for r in lineage if r["has_cymatics_embedding"])

            for label, count in [
                ("Landing zone", in_l),
                ("Trusted zone", in_t),
                ("Exploitation zone", in_e),
                ("Audio embeddings", has_ae),
                ("Text embeddings", has_te),
                ("Cymatics embeddings", has_ce),
            ]:
                pct = count / total
                filled = int(pct * 30)
                bar = "█" * filled + "░" * (30 - filled)
                print(f"    {label:<22} [{bar}] {count}/{total}")

            print(f"\n{'═' * width}\n")

        else:
            print("  Invalid choice.")


def main() -> None:
    run_interactive(from_orchestrator=False)


if __name__ == "__main__":
    main()
