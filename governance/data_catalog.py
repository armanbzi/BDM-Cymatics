#!/usr/bin/env python3
"""
Data Governance — Data product catalog (DCAT).

Builds and displays a data catalog that registers every data product in the
Sound Analysis & Cymatics domain, recording for each:

  • Ownership (the role responsible for the product).
  • Storage location, format and schema / dimensionality.
  • The data contract guarantees and the refresh cadence.
  • Declared consumers and upstream lineage (what each product derives from).
  • A live health status.

The catalog provides both a readable JSON registry and as a DCAT
(Data Catalog Vocabulary) JSON-LD document. Both are persisted to the governance
bucket.

Orchestrator: option 9 → Data governance → Data catalog,
or via Streamlit dashboard.

Run directly:
    python governance/data_catalog.py
"""

from __future__ import annotations

import json
import os
import sys
from io import BytesIO
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

from shared.minio_helpers import create_minio_client

# ── Constants

EXPLOITATION_BUCKET = os.environ.get("EXPLOITATION_ZONE_BUCKET", "exploitation-zone")
GOVERNANCE_BUCKET = os.environ.get("GOVERNANCE_BUCKET", "governance-zone")

CATALOG_KEY = "catalog/data_catalog.json"
CATALOG_DCAT_KEY = "catalog/data_catalog_dcat.jsonld"

DOMAIN = "Sound Analysis & Cymatics"
CATALOG_TITLE = "Cymatics Exploitation-Zone Data Catalog"

AUDIO_COLLECTION = "sound_audio_embeddings"
TEXT_COLLECTION = "sound_text_embeddings"
CYMATICS_COLLECTION = "sound_cymatics_embeddings"

DELTA_PREFIX = "metadata/observations_delta/"
MODELS_PREFIX = "models/"


# ── Data-product registry — the declared catalog (static metadata)


DATA_PRODUCTS: list[dict] = [
    {
        "id": "sound_observations_delta",
        "title": "Sound observations (curated wide table)",
        "type": "structured",
        "owner": "pipeline_admin",
        "description": (
            "Single table observation row per UUID: identifiers, "
            "peak-frequency statistics, cymatics quality scores, eight Spark "
            "spectral features and the two Python features (MFCC vector, "
            "harmonic-energy ratio), plus the audio / image / video asset "
            "paths. The analytical ground truth of the domain."
        ),
        "storage": {
            "system": "Delta Lake on MinIO",
            "location": f"{EXPLOITATION_BUCKET}/{DELTA_PREFIX}",
            "format": "Delta Lake (Parquet) + CSV mirror",
        },
        "schema": "Typed schema enforced by shared/sync_delta.py",
        "data_contract": (
            "Typed and fixed schema; refreshed every 15 days via the "
            "exploitation-zone Airflow DAG; quality monitored by Great "
            "Expectations (non-null UUID, no duplicates, scores in [0,1], "
            "valid asset paths)."
        ),
        "derived_from": ["trusted-zone observations"],
        "consumers": [
            "KPI discovery",
            "Streamlit dashboard",
            "lineage tracker",
            "analysts",
        ],
    },
    {
        "id": "sound_audio_embeddings",
        "title": "Audio embeddings (PANNs CNN14)",
        "type": "vector",
        "owner": "data_scientist",
        "description": (
            "Acoustic signature of every recording as a 2048-dim PANNs CNN14 "
            "vector, keyed by UUID, with categorical values for filtered ANN "
            "search."
        ),
        "storage": {
            "system": "Milvus (HNSW / COSINE)",
            "location": AUDIO_COLLECTION,
            "format": "FLOAT_VECTOR dim=2048",
        },
        "schema": "uuid (PK), category, source, peak_frequency_hz, "
                  "symmetry_score, pattern_stability_score, audio_embedding[2048]",
        "data_contract": (
            "Vectors L2-normalised, non-zero, of the exact declared "
            "dimensionality; idempotent upsert keyed on UUID."
        ),
        "derived_from": ["sound_observations_delta", "trusted-zone audio"],
        "consumers": ["audio classification", "audio similarity search"],
    },
    {
        "id": "sound_text_embeddings",
        "title": "Text embeddings (all-MiniLM-L6-v2)",
        "type": "vector",
        "owner": "data_scientist",
        "description": (
            "Natural-language description per observation embedded into a "
            "384-dim sentence-transformer vector for semantic metadata search."
        ),
        "storage": {
            "system": "Milvus (HNSW / COSINE)",
            "location": TEXT_COLLECTION,
            "format": "FLOAT_VECTOR dim=384",
        },
        "schema": "uuid (PK), category, source, peak_frequency_hz, "
                  "description_text, text_embedding[384]",
        "data_contract": (
            "Descriptions at most 4096 characters; vectors L2-normalised; "
            "idempotent upsert keyed on UUID."
        ),
        "derived_from": ["sound_observations_delta"],
        "consumers": ["metadata semantic search"],
    },
    {
        "id": "sound_cymatics_embeddings",
        "title": "Cymatics embeddings (CLIP ViT-B/32)",
        "type": "vector",
        "owner": "data_scientist",
        "description": (
            "Visual signature of every cymatics image as a 512-dim CLIP "
            "vector in a shared image-text space, enabling image-to-image "
            "and text-to-image queries."
        ),
        "storage": {
            "system": "Milvus (HNSW / COSINE)",
            "location": CYMATICS_COLLECTION,
            "format": "FLOAT_VECTOR dim=512",
        },
        "schema": "uuid (PK), category, source, peak_frequency_hz, "
                  "symmetry_score, pattern_stability_score, image_path, "
                  "cymatics_embedding[512]",
        "data_contract": (
            "Vectors L2-normalised; idempotent upsert keyed on UUID; refresh "
            "aligned with the exploitation-zone DAG."
        ),
        "derived_from": ["sound_observations_delta", "trusted-zone cymatics PNG"],
        "consumers": ["cymatics pattern search", "cymatics classifier training"],
    },
    {
        "id": "classifier_models",
        "title": "Classifier heads (model registry)",
        "type": "model",
        "owner": "data_scientist",
        "description": (
            "Supervised logistic-regression heads (StandardScaler -> "
            "LogisticRegression, balanced) mapping audio and cymatics "
            "embeddings to category predictions, each with a JSON model card."
        ),
        "storage": {
            "system": "MinIO object store",
            "location": f"{EXPLOITATION_BUCKET}/{MODELS_PREFIX}",
            "format": "joblib (.joblib) + metrics JSON (.metrics.json)",
        },
        "schema": "audio_classifier.joblib, cymatics_classifier.joblib "
                  "(+ matching .metrics.json model cards)",
        "data_contract": (
            "Each model carries a non-empty class list, a complete metrics "
            "report and a model_version; refreshed via the exploitation-zone "
            "DAG (every 15 days, or on demand)."
        ),
        "derived_from": ["sound_audio_embeddings", "sound_cymatics_embeddings"],
        "consumers": ["audio classification", "cymatics classification"],
    },
]


# ── Health probe — does each product physically exist right now?


def _milvus_collection_counts() -> dict[str, int]:
    """Return {collection name → entity count}, or -1 when unreachable."""
    counts: dict[str, int] = {}
    try:
        from milvus_embeddings import connect_milvus

        client = connect_milvus()
        for coll in (AUDIO_COLLECTION, TEXT_COLLECTION, CYMATICS_COLLECTION):
            if not client.has_collection(coll):
                counts[coll] = 0
                continue
            stats = client.get_collection_stats(coll)
            counts[coll] = int(stats.get("row_count", 0))
    except Exception:
        for coll in (AUDIO_COLLECTION, TEXT_COLLECTION, CYMATICS_COLLECTION):
            counts[coll] = -1
    return counts


def _object_exists(minio_client, bucket: str, key: str) -> bool:
    """True if an exact object exists in MinIO."""
    try:
        minio_client.stat_object(bucket, key)
        return True
    except Exception:
        return False


def _prefix_has_objects(minio_client, bucket: str, prefix: str) -> bool:
    """True if at least one object exists under a MinIO prefix."""
    try:
        objs = minio_client.list_objects(bucket, prefix=prefix, recursive=True)
        return any(True for _ in objs)
    except Exception:
        return False


def probe_product_health(minio_client, product: dict, milvus_counts: dict) -> dict:
    """Return a live health record for a single data product."""
    pid = product["id"]
    status = "missing"
    detail = ""

    if pid == "sound_observations_delta":
        present = _prefix_has_objects(minio_client, EXPLOITATION_BUCKET, DELTA_PREFIX)
        status = "available" if present else "missing"
        detail = "Delta table present" if present else "Delta table not found"

    elif pid in (AUDIO_COLLECTION, TEXT_COLLECTION, CYMATICS_COLLECTION):
        count = milvus_counts.get(pid, -1)
        if count > 0:
            status, detail = "available", f"{count} vectors"
        elif count == 0:
            status, detail = "empty", "collection exists but empty"
        else:
            status, detail = "unreachable", "Milvus not reachable"

    elif pid == "classifier_models":
        audio = _object_exists(
            minio_client, EXPLOITATION_BUCKET, f"{MODELS_PREFIX}audio_classifier.joblib"
        )
        cym = _object_exists(
            minio_client, EXPLOITATION_BUCKET, f"{MODELS_PREFIX}cymatics_classifier.joblib"
        )
        present = sum([audio, cym])
        if present == 2:
            status, detail = "available", "audio + cymatics heads present"
        elif present == 1:
            status, detail = "partial", "one head present"
        else:
            status, detail = "missing", "no model artefacts found"

    return {"status": status, "detail": detail}


# ── Catalog builder — merge static registry with live health


def build_catalog(minio_client) -> dict:
    """Build the full catalog: domain metadata + products with live status."""
    print("\n  Probing data-product health across MinIO and Milvus...")
    milvus_counts = _milvus_collection_counts()

    products: list[dict] = []
    for product in DATA_PRODUCTS:
        health = probe_product_health(minio_client, product, milvus_counts)
        record = dict(product)
        record["health"] = health
        products.append(record)
        print(f"    {product['id']:<28} {health['status']:<12} {health['detail']}")

    catalog = {
        "title": CATALOG_TITLE,
        "domain": DOMAIN,
        "n_products": len(products),
        "products": products,
    }
    return catalog


# ── DCAT serialisation — emit the catalog as DCAT JSON-LD (W3C standard)


def to_dcat_jsonld(catalog: dict) -> dict:
    """Render the catalog as a DCAT (Data Catalog Vocabulary) JSON-LD document."""
    datasets = []
    for p in catalog["products"]:
        datasets.append({
            "@type": "dcat:Dataset",
            "dct:identifier": p["id"],
            "dct:title": p["title"],
            "dct:description": p["description"],
            "dcat:theme": catalog["domain"],
            "dct:type": p["type"],
            "dct:publisher": {"@type": "foaf:Agent", "foaf:name": p["owner"]},
            "dct:conformsTo": p["data_contract"],
            "prov:wasDerivedFrom": p["derived_from"],
            "dcat:distribution": [{
                "@type": "dcat:Distribution",
                "dct:format": p["storage"]["format"],
                "dcat:accessURL": p["storage"]["location"],
                "dct:title": p["storage"]["system"],
            }],
            "cymatics:consumers": p["consumers"],
            "cymatics:status": p["health"]["status"],
        })

    return {
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dct": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "prov": "http://www.w3.org/ns/prov#",
            "cymatics": "urn:cymatics:catalog#",
        },
        "@type": "dcat:Catalog",
        "dct:title": catalog["title"],
        "dct:description": (
            "Catalog of data products in the Sound Analysis & Cymatics "
            "exploitation zone."
        ),
        "dcat:theme": catalog["domain"],
        "dcat:dataset": datasets,
    }


# ── Persist — save the readable catalog + DCAT JSON-LD to the governance bucket


def save_catalog(minio_client, catalog: dict) -> tuple[str, str]:
    """Persist the catalog JSON and the DCAT JSON-LD to the governance bucket."""
    readable = json.dumps(catalog, indent=2, default=str).encode("utf-8")
    minio_client.put_object(
        GOVERNANCE_BUCKET,
        CATALOG_KEY,
        BytesIO(readable),
        length=len(readable),
        content_type="application/json",
    )

    dcat = json.dumps(to_dcat_jsonld(catalog), indent=2, default=str).encode("utf-8")
    minio_client.put_object(
        GOVERNANCE_BUCKET,
        CATALOG_DCAT_KEY,
        BytesIO(dcat),
        length=len(dcat),
        content_type="application/ld+json",
    )

    catalog_path = f"{GOVERNANCE_BUCKET}/{CATALOG_KEY}"
    dcat_path = f"{GOVERNANCE_BUCKET}/{CATALOG_DCAT_KEY}"
    print(f"\n  Catalog saved:      {catalog_path} ({len(readable) / 1024:.1f} KB)")
    print(f"  DCAT JSON-LD saved: {dcat_path} ({len(dcat) / 1024:.1f} KB)")
    return catalog_path, dcat_path


# ── Display — catalog overview + per-product detail


def display_catalog(catalog: dict) -> None:
    """Pretty-print the catalog."""
    width = 62
    print(f"\n{'═' * width}")
    print(f"  Data Catalog — {catalog['domain']}")
    print(f"{'─' * width}")
    print(f"  Products: {catalog['n_products']}")

    status_icons = {
        "available": "✓",
        "empty": "○",
        "partial": "◐",
        "missing": "✗",
        "unreachable": "?",
    }

    print(f"{'─' * width}")
    for p in catalog["products"]:
        st = p["health"]["status"]
        icon = status_icons.get(st, "·")
        print(f"\n  {icon} {p['id']}  [{st}]")
        print(f"     Title:     {p['title']}")
        print(f"     Type:      {p['type']}")
        print(f"     Owner:     {p['owner']}")
        print(f"     Storage:   {p['storage']['system']} — {p['storage']['location']}")
        print(f"     Format:    {p['storage']['format']}")
        print(f"     Derived:   {', '.join(p['derived_from'])}")
        print(f"     Consumers: {', '.join(p['consumers'])}")
        print(f"     Status:    {p['health']['detail']}")

    print(f"\n{'═' * width}\n")


# ── Interactive CLI — build catalog, show it, persist it


def _print_menu() -> None:
    width = 62
    print(f"\n{'─' * width}")
    print("  Data catalog options:")
    print(f"{'─' * width}")
    print("   [1]  Build & display catalog (with live health)")
    print("   [2]  Build & save catalog (JSON + DCAT JSON-LD)")
    print("   [3]  Show DCAT JSON-LD")
    print("   [b]  Back")
    print()


def run_interactive(*, from_orchestrator: bool = False) -> None:
    """Main loop: build catalog → display / save → repeat."""
    width = 62
    print(f"\n{'─' * width}")
    print("  Data Governance — Data Catalog (DCAT)")
    print(f"{'─' * width}")
    print("  Registers the data products of the Sound Analysis & Cymatics")
    print("  domain with ownership, storage, contracts and live health.")
    print(f"{'─' * width}")
    print()
    print("  Requirements:")
    print("    - MinIO running  (docker compose up -d minio)")
    print("    - Milvus running for vector-product health  (optional)")
    print()

    try:
        minio_client = create_minio_client()
    except Exception as e:
        print(f"\n  Failed to connect to MinIO: {e}")
        print("  Start MinIO with: docker compose up -d minio")
        if not from_orchestrator:
            raise SystemExit(1) from e
        return

    catalog: dict | None = None

    while True:
        _print_menu()
        try:
            choice = input("  Select option [1-3, b]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Leaving data catalog.")
            break

        if choice in ("b", "q", "quit", "exit", ""):
            break

        if choice == "1":
            catalog = build_catalog(minio_client)
            display_catalog(catalog)

        elif choice == "2":
            if catalog is None:
                catalog = build_catalog(minio_client)
            try:
                save_catalog(minio_client, catalog)
            except Exception as e:
                print(f"\n  Failed to save catalog: {e}")

        elif choice == "3":
            if catalog is None:
                catalog = build_catalog(minio_client)
            print()
            print(json.dumps(to_dcat_jsonld(catalog), indent=2))

        else:
            print("  Invalid choice.")


def main() -> None:
    run_interactive(from_orchestrator=False)


if __name__ == "__main__":
    main()
