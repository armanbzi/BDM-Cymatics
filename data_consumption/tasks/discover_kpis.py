#!/usr/bin/env python3
"""
Discover defined KPI queries on the exploitation-zone Delta table.

Orchestrator: option 9 → Data consumption → Discover defined queries (KPIs).

Run directly:
    python data_consumption/tasks/discover_kpis.py
"""

from __future__ import annotations

import os
import sys
from itertools import combinations
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_PROJECT_ROOT / ".env")
except ImportError:
    pass

import numpy as np
import pandas as pd
from deltalake import DeltaTable

from shared.sync_delta import OBSERVATIONS_DELTA_PATH, s3_storage_options


# ── KPI helpers ─────────────────────────────────────────────────────────────


def _clean_category(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.mask(s.isin(["", "-", "nan", "None"]), np.nan)


def _clean_source(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.mask(s.isin(["", "-", "nan", "None"]), np.nan)


def _clean_peak_frequency_hz(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _frequency_band(hz: float) -> str:
    if hz < 200:
        return "Low (0-200 Hz)"
    if hz < 1000:
        return "Mid (200-1000 Hz)"
    return "High (1000+ Hz)"


def kpi_most_repeated_frequency_per_category(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["category"] = _clean_category(work["category"])
    work["peak_frequency_hz"] = _clean_peak_frequency_hz(work["peak_frequency_hz"])
    work = work.dropna(subset=["category", "peak_frequency_hz"])
    if work.empty:
        return pd.DataFrame(
            columns=[
                "category",
                "most_repeated_frequency_hz",
                "occurrence_count",
                "total_observations_in_category",
                "share_within_category",
            ]
        )

    work["frequency_hz_rounded"] = work["peak_frequency_hz"].round().astype(int)
    freq_counts = (
        work.groupby(["category", "frequency_hz_rounded"], as_index=False)
        .size()
        .rename(columns={"size": "occurrence_count"})
    )
    totals = work.groupby("category").size().rename("total_observations_in_category")
    idx = freq_counts.groupby("category")["occurrence_count"].idxmax()
    top = freq_counts.loc[idx].rename(
        columns={"frequency_hz_rounded": "most_repeated_frequency_hz"}
    )
    top = top.merge(totals, on="category", how="left")
    top["share_within_category"] = (
        top["occurrence_count"] / top["total_observations_in_category"]
    ).round(4)
    return top.sort_values(
        ["share_within_category", "category"],
        ascending=[False, True],
    ).reset_index(drop=True)[
        [
            "category",
            "most_repeated_frequency_hz",
            "occurrence_count",
            "total_observations_in_category",
            "share_within_category",
        ]
    ]


def kpi_top_categories_by_frequency_share(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    full = kpi_most_repeated_frequency_per_category(df)
    if full.empty:
        return full
    out = full.nlargest(top_n, "share_within_category").copy()
    out.insert(0, "rank", range(1, len(out) + 1))
    return out.reset_index(drop=True)


def kpi_best_cymatics_candidates(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    work = df.copy()
    for col in ("symmetry_score", "pattern_stability_score"):
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["symmetry_score", "pattern_stability_score"])
    if work.empty:
        return pd.DataFrame()

    work["combined_cymatics_score"] = (
        work["symmetry_score"] + work["pattern_stability_score"]
    ) / 2.0
    cols = [
        "uuid",
        "source_id",
        "category",
        "source",
        "symmetry_score",
        "pattern_stability_score",
        "combined_cymatics_score",
        "peak_frequency_hz",
        "audio_path",
    ]
    cols = [c for c in cols if c in work.columns]
    out = work.nlargest(top_n, "combined_cymatics_score").loc[:, cols].reset_index(drop=True)
    out.insert(0, "rank", range(1, len(out) + 1))
    return out


def kpi_frequency_clusters(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["peak_frequency_hz"] = _clean_peak_frequency_hz(work["peak_frequency_hz"])
    work["category"] = _clean_category(work.get("category", pd.Series(dtype=object)))
    work = work.dropna(subset=["peak_frequency_hz"])
    if work.empty:
        return pd.DataFrame(
            columns=[
                "frequency_band",
                "recording_count",
                "distinct_category_count",
                "share_of_recordings",
            ]
        )

    work["frequency_band"] = work["peak_frequency_hz"].map(_frequency_band)
    total = len(work)
    out = work.groupby("frequency_band", as_index=False).agg(
        recording_count=("uuid", "count"),
        distinct_category_count=("category", lambda s: s.dropna().nunique()),
    )
    out["share_of_recordings"] = (out["recording_count"] / total).round(4)
    band_order = ["Low (0-200 Hz)", "Mid (200-1000 Hz)", "High (1000+ Hz)"]
    out["frequency_band"] = pd.Categorical(out["frequency_band"], categories=band_order, ordered=True)
    return out.sort_values("frequency_band").reset_index(drop=True)


def kpi_frequency_clusters_top_categories(
    df: pd.DataFrame, top_categories: int = 3
) -> pd.DataFrame:
    work = df.copy()
    work["peak_frequency_hz"] = _clean_peak_frequency_hz(work["peak_frequency_hz"])
    work["category"] = _clean_category(work["category"])
    work = work.dropna(subset=["peak_frequency_hz", "category"])
    if work.empty:
        return pd.DataFrame()

    work["frequency_band"] = work["peak_frequency_hz"].map(_frequency_band)
    band_totals = work.groupby("frequency_band").size().rename("band_recording_count")
    cat_counts = (
        work.groupby(["frequency_band", "category"], as_index=False)
        .size()
        .rename(columns={"size": "recording_count_in_band"})
    )
    cat_counts = cat_counts.merge(band_totals.reset_index(), on="frequency_band", how="left")
    cat_counts["share_within_band"] = (
        cat_counts["recording_count_in_band"] / cat_counts["band_recording_count"]
    ).round(4)

    rows = []
    for _, grp in cat_counts.groupby("frequency_band", sort=False):
        top = grp.nlargest(top_categories, "recording_count_in_band").copy()
        top.insert(0, "rank_in_band", range(1, len(top) + 1))
        rows.append(top)
    out = pd.concat(rows, ignore_index=True)
    band_order = ["Low (0-200 Hz)", "Mid (200-1000 Hz)", "High (1000+ Hz)"]
    out["frequency_band"] = pd.Categorical(out["frequency_band"], categories=band_order, ordered=True)
    return out.sort_values(["frequency_band", "rank_in_band"]).reset_index(drop=True)[
        [
            "frequency_band",
            "rank_in_band",
            "category",
            "recording_count_in_band",
            "band_recording_count",
            "share_within_band",
        ]
    ]


def _all_category_pair_similarities(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["category"] = _clean_category(work["category"])
    work["peak_frequency_hz"] = _clean_peak_frequency_hz(work["peak_frequency_hz"])
    work = work.dropna(subset=["category", "peak_frequency_hz"])
    if work.empty:
        return pd.DataFrame()

    cat_mean = (
        work.groupby("category", as_index=False)["peak_frequency_hz"]
        .mean()
        .rename(columns={"peak_frequency_hz": "avg_peak_frequency_hz"})
    )
    if len(cat_mean) < 2:
        return pd.DataFrame()

    means = cat_mean.set_index("category")["avg_peak_frequency_hz"]
    pairs = []
    for c1, c2 in combinations(cat_mean["category"].tolist(), 2):
        pairs.append(
            {
                "category_a": c1,
                "category_b": c2,
                "avg_peak_frequency_hz_a": round(means[c1], 2),
                "avg_peak_frequency_hz_b": round(means[c2], 2),
                "frequency_difference_hz": round(abs(means[c1] - means[c2]), 2),
            }
        )
    return pd.DataFrame(pairs).sort_values("frequency_difference_hz").reset_index(drop=True)


def kpi_most_similar_frequency_categories_unique(
    df: pd.DataFrame, top_pairs: int = 10
) -> pd.DataFrame:
    pairs = _all_category_pair_similarities(df)
    if pairs.empty:
        return pairs

    used: set[str] = set()
    selected = []
    for _, row in pairs.iterrows():
        a, b = row["category_a"], row["category_b"]
        if a in used or b in used:
            continue
        selected.append(row)
        used.add(a)
        used.add(b)
        if len(selected) >= top_pairs:
            break

    if not selected:
        return pd.DataFrame()
    out = pd.DataFrame(selected).reset_index(drop=True)
    out.insert(0, "rank", range(1, len(out) + 1))
    return out


def kpi_avg_processing_time_per_source(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["source"] = _clean_source(work["source"])
    work["processing_time(seconds)"] = pd.to_numeric(
        work["processing_time(seconds)"], errors="coerce"
    )
    work = work.dropna(subset=["source", "processing_time(seconds)"])
    if work.empty:
        return pd.DataFrame()

    out = work.groupby("source", as_index=False).agg(
        avg_processing_time_seconds=("processing_time(seconds)", "mean"),
        std_processing_time_seconds=("processing_time(seconds)", "std"),
        min_processing_time_seconds=("processing_time(seconds)", "min"),
        max_processing_time_seconds=("processing_time(seconds)", "max"),
        recording_count=("uuid", "count"),
    )
    for col in out.columns:
        if col.endswith("_seconds"):
            out[col] = out[col].round(3)
    return out.sort_values("avg_processing_time_seconds", ascending=False).reset_index(drop=True)


def kpi_most_complex_categories(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    work = df.copy()
    work["category"] = _clean_category(work["category"])
    work["spectral_entropy"] = pd.to_numeric(work["spectral_entropy"], errors="coerce")
    work = work.dropna(subset=["category", "spectral_entropy"])
    if work.empty:
        return pd.DataFrame()

    out = work.groupby("category", as_index=False).agg(
        avg_spectral_entropy=("spectral_entropy", "mean"),
        max_spectral_entropy=("spectral_entropy", "max"),
        recording_count=("uuid", "count"),
    )
    out["avg_spectral_entropy"] = out["avg_spectral_entropy"].round(4)
    out["max_spectral_entropy"] = out["max_spectral_entropy"].round(4)
    out = out.nlargest(top_n, "avg_spectral_entropy").reset_index(drop=True)
    out.insert(0, "rank", range(1, len(out) + 1))
    return out


def kpi_brightest_darkest_categories(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    work = df.copy()
    work["category"] = _clean_category(work["category"])
    work["spectral_centroid_hz"] = pd.to_numeric(work["spectral_centroid_hz"], errors="coerce")
    work = work.dropna(subset=["category", "spectral_centroid_hz"])
    if work.empty:
        return pd.DataFrame()

    cat_avg = work.groupby("category", as_index=False).agg(
        avg_spectral_centroid_hz=("spectral_centroid_hz", "mean"),
        recording_count=("uuid", "count"),
    )
    cat_avg["avg_spectral_centroid_hz"] = cat_avg["avg_spectral_centroid_hz"].round(2)

    brightest = cat_avg.nlargest(top_n, "avg_spectral_centroid_hz").copy()
    brightest["brightness_group"] = "brightest"
    darkest = cat_avg.nsmallest(top_n, "avg_spectral_centroid_hz").copy()
    darkest["brightness_group"] = "darkest"

    out = pd.concat([brightest, darkest], ignore_index=True)
    out.insert(0, "rank", range(1, len(out) + 1))
    return out


KPI_MENU: dict[str, dict] = {
    "1": {
        "title": "Most repeated peak frequency — top 10 categories by share",
        "description": (
            "Categories where one dominant peak frequency accounts for the largest "
            "share of observations within that category."
        ),
        "runner": lambda df: kpi_top_categories_by_frequency_share(df, top_n=10),
    },
    "2": {
        "title": "Best cymatics candidates (top 10)",
        "description": (
            "Individual recordings with the highest combined symmetry and pattern "
            "stability scores."
        ),
        "runner": lambda df: kpi_best_cymatics_candidates(df, top_n=10),
    },
    "3": {
        "title": "Frequency clusters — top 3 categories per band",
        "description": (
            "Low (0–200 Hz), Mid (200–1000 Hz), High (1000+ Hz): the three most "
            "represented categories in each band."
        ),
        "runner": lambda df: kpi_frequency_clusters_top_categories(df, top_categories=3),
    },
    "4": {
        "title": "Most similar frequency categories (top 10, unique)",
        "description": (
            "Category pairs with the closest average peak frequency; each category "
            "appears at most once in the list."
        ),
        "runner": lambda df: kpi_most_similar_frequency_categories_unique(df, top_pairs=10),
    },
    "5": {
        "title": "Average processing time per source",
        "description": (
            "Trusted-zone cymatics processing duration (seconds) averaged by landing "
            "source (warm-path, hot-path, cold-path)."
        ),
        "runner": kpi_avg_processing_time_per_source,
    },
    "6": {
        "title": "Most complex categories (top 10)",
        "description": (
            "Categories ranked by mean spectral_entropy (high = complex / noise-like)."
        ),
        "runner": lambda df: kpi_most_complex_categories(df, top_n=10),
    },
    "7": {
        "title": "Brightest & darkest categories (top 5 each)",
        "description": (
            "Categories with the highest and lowest mean spectral_centroid_hz "
            "(brighter vs darker timbre)."
        ),
        "runner": lambda df: kpi_brightest_darkest_categories(df, top_n=5),
    },
}


# ── Delta load + CLI ──────────────────────────────────────────────────────────


def _exploitation_delta_uri() -> str:
    bucket = os.environ.get("EXPLOITATION_ZONE_BUCKET", "exploitation-zone").strip()
    return f"s3a://{bucket}/{OBSERVATIONS_DELTA_PATH}"


def load_exploitation_observations() -> tuple[pd.DataFrame, str, int]:
    uri = _exploitation_delta_uri()
    storage = s3_storage_options()
    if not storage.get("AWS_ACCESS_KEY_ID") or not storage.get("AWS_SECRET_ACCESS_KEY"):
        raise RuntimeError(
            "MinIO credentials missing. Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY in .env."
        )
    dt = DeltaTable(uri, storage_options=storage)
    return dt.to_pandas(), uri, dt.version()


def _format_value(val) -> str:
    if pd.isna(val):
        return "—"
    if isinstance(val, float):
        return f"{val:.4f}".rstrip("0").rstrip(".")
    return str(val)


def _wrap_text(text: str, width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current: list[str] = []
    length = 0
    for word in words:
        if length + len(word) + (1 if current else 0) > width:
            lines.append(" ".join(current))
            current = [word]
            length = len(word)
        else:
            if current:
                length += 1 + len(word)
            else:
                length = len(word)
            current.append(word)
    if current:
        lines.append(" ".join(current))
    return lines or [""]


def print_results_table(title: str, description: str, df: pd.DataFrame, *, row_count: int) -> None:
    width = 72
    print(f"\n{'═' * width}")
    print(f"  {title}")
    print(f"{'─' * width}")
    for line in _wrap_text(description, width - 4):
        print(f"  {line}")
    print(f"{'─' * width}")
    print(f"  Corpus: {row_count} observation(s) in exploitation Delta")
    print(f"{'─' * width}")

    if df is None or df.empty:
        print("  (no data — check exploitation processing and column values)")
        print(f"{'═' * width}\n")
        return

    display_df = df.copy()
    for col in display_df.select_dtypes(include=["float"]).columns:
        display_df[col] = display_df[col].map(
            lambda x: round(x, 4) if pd.notna(x) else x
        )

    headers = [str(c) for c in display_df.columns]
    rows = [
        [_format_value(v) for v in display_df.iloc[i].tolist()]
        for i in range(len(display_df))
    ]
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(cells: list[str]) -> str:
        parts = [cells[i].ljust(widths[i]) for i in range(len(cells))]
        return "  " + " │ ".join(parts)

    print(fmt_row(headers))
    print("  " + "─┼─".join("─" * w for w in widths))
    for row in rows:
        print(fmt_row(row))
    print(f"{'═' * width}\n")


def print_kpi_menu() -> None:
    print(f"\n{'─' * 62}")
    print("  Discover defined queries (KPIs)")
    print(f"  Source: {_exploitation_delta_uri()}")
    print(f"{'─' * 62}")
    for key in sorted(KPI_MENU.keys(), key=lambda k: int(k)):
        print(f"   [{key}]  {KPI_MENU[key]['title']}")
    print("   [a]  Run all KPIs")
    print("   [b]  Back")
    print()


def run_kpi(choice: str, obs: pd.DataFrame, row_count: int) -> None:
    meta = KPI_MENU[choice]
    print("\n  Querying Delta table...")
    result = meta["runner"](obs)

    if choice == "3":
        summary = kpi_frequency_clusters(obs)
        if not summary.empty:
            print(f"\n{'─' * 62}")
            print("  Band summary")
            print_results_table(
                "Frequency band overview",
                "Recording counts and share of corpus per band.",
                summary,
                row_count=row_count,
            )

    print_results_table(meta["title"], meta["description"], result, row_count=row_count)


def run_all_kpis(obs: pd.DataFrame, row_count: int) -> None:
    for key in sorted(KPI_MENU.keys(), key=lambda k: int(k)):
        run_kpi(key, obs, row_count)


def run_interactive(*, from_orchestrator: bool = False) -> None:
    try:
        obs, uri, version = load_exploitation_observations()
    except Exception as e:
        print(f"\n  Failed to load exploitation Delta table: {e}")
        print("  Ensure MinIO is running and exploitation zone processing has completed.")
        if not from_orchestrator:
            raise SystemExit(1) from e
        return

    row_count = len(obs)
    print(f"\n  Loaded {row_count} row(s) from {uri} (Delta version {version})")

    while True:
        print_kpi_menu()
        try:
            choice = input("  Select KPI [1-7, a, b]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Leaving KPI discovery.")
            break

        if choice in ("b", "q", ""):
            break
        if choice == "a":
            run_all_kpis(obs, row_count)
            continue
        if choice not in KPI_MENU:
            print("  Invalid choice.")
            continue
        run_kpi(choice, obs, row_count)


def main() -> None:
    run_interactive(from_orchestrator=False)


if __name__ == "__main__":
    main()
