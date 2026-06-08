#!/usr/bin/env python3
"""
Streamlit dashboard — BDM Cymatics data consumption layer.

Here we provides an interactive web UI for the Data-Consumption tasks & governance:
 - pre-defined KPI queries with appropriate charts for each.
 - Interaction with audio classification.
 - Interaction with Cymatics classification.
 - Interaction with metadata search assistant.
 - Apply governance and view results.

Run directly:
    streamlit run data_consumption/data_consumption_all.py

Requires:
    pip install streamlit plotly
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
_DC_DIR = Path(__file__).resolve().parent
if str(_DC_DIR) not in sys.path:
    sys.path.insert(1, str(_DC_DIR))
_EZ_DIR = _PROJECT_ROOT / "exploitation_zone"
if str(_EZ_DIR) not in sys.path:
    sys.path.insert(2, str(_EZ_DIR))

try:
    from dotenv import load_dotenv

    load_dotenv(_PROJECT_ROOT / ".env")
except ImportError:
    pass

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from tasks.discover_kpis import (
    kpi_top_categories_by_frequency_share,
    kpi_best_cymatics_candidates,
    kpi_frequency_clusters,
    kpi_frequency_clusters_top_categories,
    kpi_most_similar_frequency_categories_unique,
    kpi_avg_processing_time_per_source,
    kpi_most_complex_categories,
    kpi_brightest_darkest_categories,
    load_exploitation_observations,
)

# Milvus imports are deferred (only loaded when the user opens the tab)
# to avoid startup errors when Milvus is not running.
_MILVUS_AVAILABLE = None


# ── Page config — set Streamlit page title, icon, and layout

st.set_page_config(
    page_title="BDM Cymatics — Dashboard",
    page_icon="~",
    layout="wide",
)


# ── Data loading (cached) — read exploitation Delta table via MinIO with 120 s TTL


@st.cache_data(ttl=120, show_spinner="Loading exploitation-zone Delta table...")
def _load_data() -> tuple[pd.DataFrame, str, int]:
    """Load exploitation-zone observations from Delta Lake via MinIO."""
    return load_exploitation_observations()


def _try_load_data():
    """Attempt data load and show friendly error on failure."""
    try:
        obs, uri, version = _load_data()
        return obs, uri, version
    except Exception as e:
        st.error(
            f"Failed to load exploitation-zone Delta table: {e}\n\n"
            "Ensure MinIO is running (`docker compose up -d minio`) and "
            "exploitation-zone processing has completed."
        )
        st.stop()


@st.cache_data(ttl=120, show_spinner=False)
def _image_path_map() -> dict[str, str]:
    """Build UUID → image_path lookup from exploitation observations."""
    try:
        obs, _, _ = _load_data()
        if "image_path" in obs.columns and "uuid" in obs.columns:
            return dict(
                zip(
                    obs["uuid"].astype(str),
                    obs["image_path"].fillna("").astype(str),
                )
            )
    except Exception:
        pass
    return {}


# ── KPI chart renders — one function per KPI, each builds a Plotly figure


def _render_frequency_share(obs: pd.DataFrame) -> None:
    """KPI 1 — Horizontal bar: top categories by dominant-frequency share."""
    st.subheader("Top Categories by Dominant-Frequency Share")
    st.caption(
        "Categories where one peak frequency accounts for the largest "
        "share of observations within that category."
    )

    df = kpi_top_categories_by_frequency_share(obs, top_n=10)
    if df.empty:
        st.info("No data available for this KPI.")
        return

    fig = px.bar(
        df.sort_values("share_within_category"),
        x="share_within_category",
        y="category",
        orientation="h",
        color="most_repeated_frequency_hz",
        color_continuous_scale="Viridis",
        labels={
            "share_within_category": "Share within category",
            "category": "Category",
            "most_repeated_frequency_hz": "Dominant freq (Hz)",
        },
        text=df.sort_values("share_within_category")["share_within_category"].apply(
            lambda v: f"{v:.0%}"
        ),
    )
    fig.update_layout(yaxis=dict(autorange="reversed"), height=420)
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(df, use_container_width=True, hide_index=True)


def _render_cymatics_candidates(obs: pd.DataFrame) -> None:
    """KPI 2 — Scatter: best cymatics candidates (symmetry vs stability)."""
    st.subheader("Best Cymatics Candidates")
    st.caption(
        "Recordings with the highest combined symmetry and pattern stability scores."
    )

    df = kpi_best_cymatics_candidates(obs, top_n=10)
    if df.empty:
        st.info("No data available for this KPI.")
        return

    fig = px.scatter(
        df,
        x="symmetry_score",
        y="pattern_stability_score",
        size="combined_cymatics_score",
        color="category",
        hover_data=["uuid", "peak_frequency_hz", "source"],
        labels={
            "symmetry_score": "Symmetry score",
            "pattern_stability_score": "Pattern stability score",
            "combined_cymatics_score": "Combined score",
            "category": "Category",
        },
        size_max=20,
    )
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(df, use_container_width=True, hide_index=True)


def _render_frequency_clusters(obs: pd.DataFrame) -> None:
    """KPI 3 — Pie (band overview) + grouped bar (top categories per band)."""
    st.subheader("Frequency Clusters")
    st.caption(
        "Distribution of recordings across Low (0-200 Hz), Mid (200-1000 Hz), "
        "and High (1000+ Hz) bands, with the top 3 categories in each."
    )

    summary = kpi_frequency_clusters(obs)
    detail = kpi_frequency_clusters_top_categories(obs, top_categories=3)

    if summary.empty:
        st.info("No data available for this KPI.")
        return

    col_pie, col_bar = st.columns(2)

    with col_pie:
        fig_pie = px.pie(
            summary,
            names="frequency_band",
            values="recording_count",
            color="frequency_band",
            color_discrete_map={
                "Low (0-200 Hz)": "#636EFA",
                "Mid (200-1000 Hz)": "#EF553B",
                "High (1000+ Hz)": "#00CC96",
            },
            hole=0.35,
        )
        fig_pie.update_layout(height=380, title_text="Recordings per band")
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_bar:
        if not detail.empty:
            fig_bar = px.bar(
                detail,
                x="category",
                y="share_within_band",
                color="frequency_band",
                barmode="group",
                color_discrete_map={
                    "Low (0-200 Hz)": "#636EFA",
                    "Mid (200-1000 Hz)": "#EF553B",
                    "High (1000+ Hz)": "#00CC96",
                },
                labels={
                    "share_within_band": "Share within band",
                    "category": "Category",
                    "frequency_band": "Band",
                },
                text=detail["share_within_band"].apply(lambda v: f"{v:.0%}"),
            )
            fig_bar.update_layout(
                height=380,
                title_text="Top categories per band",
                xaxis_tickangle=-35,
            )
            fig_bar.update_traces(textposition="outside")
            st.plotly_chart(fig_bar, use_container_width=True)

    with st.expander("Raw data"):
        st.write("**Band summary**")
        st.dataframe(summary, use_container_width=True, hide_index=True)
        if not detail.empty:
            st.write("**Top categories per band**")
            st.dataframe(detail, use_container_width=True, hide_index=True)


def _render_similar_categories(obs: pd.DataFrame) -> None:
    """KPI 4 — Dumbbell / paired bar: most similar frequency category pairs."""
    st.subheader("Most Similar Frequency Categories")
    st.caption(
        "Category pairs with the closest average peak frequency; "
        "each category appears at most once."
    )

    df = kpi_most_similar_frequency_categories_unique(obs, top_pairs=10)
    if df.empty:
        st.info("No data available for this KPI.")
        return

    pair_labels = [
        f"{r['category_a']}  ~  {r['category_b']}"
        for _, r in df.iterrows()
    ]

    fig = go.Figure()
    for i, (_, row) in enumerate(df.iterrows()):
        label = pair_labels[i]
        fig.add_trace(
            go.Scatter(
                x=[row["avg_peak_frequency_hz_a"], row["avg_peak_frequency_hz_b"]],
                y=[label, label],
                mode="lines+markers",
                marker=dict(size=10),
                line=dict(width=3),
                name=label,
                showlegend=False,
                hovertemplate=(
                    f"{row['category_a']}: %{{x:.0f}} Hz<br>"
                    f"Diff: {row['frequency_difference_hz']:.1f} Hz"
                    "<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        xaxis_title="Average peak frequency (Hz)",
        height=max(300, len(df) * 40 + 80),
        margin=dict(l=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(df, use_container_width=True, hide_index=True)


def _render_processing_time(obs: pd.DataFrame) -> None:
    """KPI 5 — Bar with error bars: avg processing time per source."""
    st.subheader("Average Processing Time per Source")
    st.caption(
        "Trusted-zone cymatics processing duration (seconds) "
        "averaged by landing source."
    )

    df = kpi_avg_processing_time_per_source(obs)
    if df.empty:
        st.info("No data available for this KPI.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["source"],
            y=df["avg_processing_time_seconds"],
            error_y=dict(
                type="data",
                array=df["std_processing_time_seconds"].fillna(0).tolist(),
                visible=True,
            ),
            marker_color="#636EFA",
            text=df["avg_processing_time_seconds"].apply(lambda v: f"{v:.2f}s"),
            textposition="outside",
            hovertemplate=(
                "Source: %{x}<br>"
                "Avg: %{y:.2f}s<br>"
                "Min: %{customdata[0]:.2f}s<br>"
                "Max: %{customdata[1]:.2f}s<br>"
                "Count: %{customdata[2]}"
                "<extra></extra>"
            ),
            customdata=df[
                [
                    "min_processing_time_seconds",
                    "max_processing_time_seconds",
                    "recording_count",
                ]
            ].values,
        )
    )
    fig.update_layout(
        xaxis_title="Source",
        yaxis_title="Processing time (seconds)",
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(df, use_container_width=True, hide_index=True)


def _render_complex_categories(obs: pd.DataFrame) -> None:
    """KPI 6 — Horizontal bar: most complex categories by spectral entropy."""
    st.subheader("Most Complex Categories")
    st.caption(
        "Categories ranked by mean spectral entropy "
        "(high = complex / noise-like, low = tonal)."
    )

    df = kpi_most_complex_categories(obs, top_n=10)
    if df.empty:
        st.info("No data available for this KPI.")
        return

    fig = px.bar(
        df.sort_values("avg_spectral_entropy"),
        x="avg_spectral_entropy",
        y="category",
        orientation="h",
        color="avg_spectral_entropy",
        color_continuous_scale="YlOrRd",
        labels={
            "avg_spectral_entropy": "Avg spectral entropy",
            "category": "Category",
        },
        hover_data=["max_spectral_entropy", "recording_count"],
        text=df.sort_values("avg_spectral_entropy")["avg_spectral_entropy"].apply(
            lambda v: f"{v:.2f}"
        ),
    )
    fig.update_layout(yaxis=dict(autorange="reversed"), height=420)
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(df, use_container_width=True, hide_index=True)


def _render_brightness(obs: pd.DataFrame) -> None:
    """KPI 7 — Diverging bar: brightest vs darkest categories by spectral centroid."""
    st.subheader("Brightest & Darkest Categories")
    st.caption(
        "Categories with the highest (brightest) and lowest (darkest) "
        "mean spectral centroid."
    )

    df = kpi_brightest_darkest_categories(obs, top_n=5)
    if df.empty:
        st.info("No data available for this KPI.")
        return

    brightest = df[df["brightness_group"] == "brightest"].sort_values(
        "avg_spectral_centroid_hz"
    )
    darkest = df[df["brightness_group"] == "darkest"].sort_values(
        "avg_spectral_centroid_hz", ascending=False
    )

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=brightest["category"],
            x=brightest["avg_spectral_centroid_hz"],
            orientation="h",
            name="Brightest",
            marker_color="#FECB52",
            text=brightest["avg_spectral_centroid_hz"].apply(lambda v: f"{v:.0f} Hz"),
            textposition="outside",
        )
    )
    fig.add_trace(
        go.Bar(
            y=darkest["category"],
            x=darkest["avg_spectral_centroid_hz"],
            orientation="h",
            name="Darkest",
            marker_color="#636EFA",
            text=darkest["avg_spectral_centroid_hz"].apply(lambda v: f"{v:.0f} Hz"),
            textposition="outside",
        )
    )
    fig.update_layout(
        xaxis_title="Avg spectral centroid (Hz)",
        height=420,
        barmode="group",
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw data"):
        st.dataframe(df, use_container_width=True, hide_index=True)


# ── KPI registry — maps KPI number to its render function (mirrors discover_kpis.KPI_MENU)

KPI_RENDERERS: dict[str, dict] = {
    "1": {
        "title": "Dominant-frequency share",
        "renderer": _render_frequency_share,
    },
    "2": {
        "title": "Best cymatics candidates",
        "renderer": _render_cymatics_candidates,
    },
    "3": {
        "title": "Frequency clusters",
        "renderer": _render_frequency_clusters,
    },
    "4": {
        "title": "Similar frequency categories",
        "renderer": _render_similar_categories,
    },
    "5": {
        "title": "Processing time per source",
        "renderer": _render_processing_time,
    },
    "6": {
        "title": "Most complex categories",
        "renderer": _render_complex_categories,
    },
    "7": {
        "title": "Brightest & darkest categories",
        "renderer": _render_brightness,
    },
}


# ── Audio classification — record mic → PANNs CNN14 embedding → Milvus ANN search


def _check_milvus() -> bool:
    """Return True if Milvus modules are importable and the server is reachable.

    Only caches a *successful* connection — failures are rechecked on every
    page load so the UI recovers automatically once Milvus comes up.
    """
    global _MILVUS_AVAILABLE
    if _MILVUS_AVAILABLE is True:
        return True
    try:
        from milvus_embeddings import connect_milvus

        connect_milvus()
        _MILVUS_AVAILABLE = True
    except Exception:
        _MILVUS_AVAILABLE = False
    return _MILVUS_AVAILABLE


def _render_audio_classification() -> None:
    """Audio classification tab — record or upload audio, find similar sounds."""
    st.subheader("Audio Classification — Similarity Search")
    st.caption(
        "Record audio from your microphone or upload a WAV file, then find "
        "the most acoustically similar sounds using PANNs CNN14 embeddings."
    )

    if not _check_milvus():
        st.warning(
            "Milvus is not reachable. Start it with "
            "`docker compose up -d milvus` and ensure audio embeddings "
            "have been ingested (orchestrate → [6])."
        )
        return

    # ── Audio input
    col_rec, col_upload = st.columns(2)

    audio_bytes = None
    audio_source = None

    with col_rec:
        st.markdown("**Record from microphone**")
        recorded = st.audio_input("Record a sound clip")
        if recorded is not None:
            audio_bytes = recorded.getvalue()
            audio_source = "microphone"

    with col_upload:
        st.markdown("**Upload a WAV file**")
        uploaded = st.file_uploader(
            "Choose a WAV file",
            type=["wav"],
            key="audio_upload",
        )
        if uploaded is not None:
            audio_bytes = uploaded.getvalue()
            audio_source = "upload"

    if audio_bytes is None:
        st.info("Record or upload audio to begin the similarity search.")
        return

    # ── Playback
    st.audio(audio_bytes, format="audio/wav")
    st.markdown(
        f"**Source:** {audio_source} &nbsp;|&nbsp; "
        f"**Size:** {len(audio_bytes) / 1024:.1f} KB"
    )

    # ── Search
    top_k = st.slider("Number of results", min_value=1, max_value=20, value=5)

    if st.button("Find similar sounds", type="primary"):
        with st.spinner("Computing PANNs CNN14 embedding and searching Milvus..."):
            try:
                from scipy.io import wavfile as scipy_wav
                from milvus_embeddings import (
                    connect_milvus,
                    search_similar_sounds,
                    _load_audio_float32,
                )

                sr, audio = _load_audio_float32(audio_bytes)
                milvus_client = connect_milvus()
                results = search_similar_sounds(
                    milvus_client, audio, sr, top_k=top_k,
                )
            except Exception as e:
                st.error(f"Search failed: {e}")
                return

        if not results:
            st.warning("No similar recordings found.")
            return

        st.success(f"Found {len(results)} similar recording(s).")
        st.divider()

        img_map = _image_path_map()

        # ── Results
        for i, hit in enumerate(results):
            entity = hit["entity"]
            distance = hit["distance"]

            uuid = entity.get("uuid", "?")
            category = entity.get("category", "") or "—"
            source = entity.get("source", "") or "—"
            peak_hz = entity.get("peak_frequency_hz", 0)
            symmetry = entity.get("symmetry_score", 0)
            image_path = img_map.get(uuid, "")

            with st.container():
                col_img, col_info = st.columns([1, 2])

                with col_img:
                    img_data = _load_cymatics_image(image_path)
                    if img_data is not None:
                        st.image(img_data, caption=f"#{i + 1} — {category}", width=280)
                    else:
                        st.markdown(f"**#{i + 1}** — *image not available*")

                with col_info:
                    m_cols = st.columns([2, 2, 2])
                    m_cols[0].metric("Category", category)
                    m_cols[1].metric("Similarity", f"{distance:.4f}")
                    m_cols[2].metric("Peak freq", f"{peak_hz:.0f} Hz")

                    with st.expander(f"Details — {uuid[:12]}…"):
                        st.markdown(
                            f"- **UUID:** `{uuid}`\n"
                            f"- **Source:** {source}\n"
                            f"- **Peak frequency:** {peak_hz:.1f} Hz\n"
                            f"- **Symmetry score:** {symmetry:.3f}\n"
                            f"- **Image path:** `{image_path}`\n"
                            f"- **Cosine similarity:** {distance:.6f}"
                        )

            if i < len(results) - 1:
                st.divider()


# ── Cymatics classification — image/audio/text → CLIP ViT-B/32 → Milvus pattern search


def _load_cymatics_image(image_path: str) -> bytes | None:
    """Fetch a cymatics PNG from the trusted-zone MinIO bucket.

    Uses analyst (read-only) credentials so data-consumption tasks
    never hold write access to upstream zones.

    Returns raw PNG bytes, or *None* if the image cannot be loaded.
    """
    if not image_path or image_path == "—":
        return None
    try:
        from shared.minio_helpers import create_minio_client_readonly

        bucket = os.environ.get("TRUSTED_ZONE_BUCKET", "trusted-zone")
        client = create_minio_client_readonly()
        resp = client.get_object(bucket, image_path)
        data = resp.read()
        resp.close()
        resp.release_conn()
        return data
    except Exception:
        return None


def _render_cymatics_results(results: list[dict]) -> None:
    """Shared result renderer for cymatics pattern search hits."""
    if not results:
        st.warning("No matching patterns found.")
        return

    st.success(f"Found {len(results)} similar pattern(s).")
    st.divider()

    for i, hit in enumerate(results):
        entity = hit["entity"]
        distance = hit["distance"]

        uuid = entity.get("uuid", "?")
        category = entity.get("category", "") or "—"
        source = entity.get("source", "") or "—"
        peak_hz = entity.get("peak_frequency_hz", 0)
        symmetry = entity.get("symmetry_score", 0)
        image_path = entity.get("image_path", "") or "—"

        with st.container():
            col_img, col_info = st.columns([1, 2])

            with col_img:
                img_data = _load_cymatics_image(image_path)
                if img_data is not None:
                    st.image(img_data, caption=f"#{i + 1} — {category}", width=280)
                else:
                    st.markdown(f"**#{i + 1}** — *image not available*")

            with col_info:
                m_cols = st.columns([2, 2, 2])
                m_cols[0].metric("Category", category)
                m_cols[1].metric("Similarity", f"{distance:.4f}")
                m_cols[2].metric("Peak freq", f"{peak_hz:.0f} Hz")

                with st.expander(f"Details — {uuid[:12]}…"):
                    st.markdown(
                        f"- **UUID:** `{uuid}`\n"
                        f"- **Source:** {source}\n"
                        f"- **Peak frequency:** {peak_hz:.1f} Hz\n"
                        f"- **Symmetry score:** {symmetry:.3f}\n"
                        f"- **Image path:** `{image_path}`\n"
                        f"- **Cosine similarity:** {distance:.6f}"
                    )

        if i < len(results) - 1:
            st.divider()


def _render_cymatics_classification() -> None:
    """Cymatics classification tab — image, audio-to-image, or text search."""
    st.subheader("Cymatics Classification — Pattern Search")
    st.caption(
        "Find similar cymatics patterns using CLIP ViT-B/32 embeddings. "
        "Upload an image, record audio to generate a pattern, or describe "
        "a pattern in natural language."
    )

    if not _check_milvus():
        st.warning(
            "Milvus is not reachable. Start it with "
            "`docker compose up -d milvus` and ensure cymatics embeddings "
            "have been ingested (orchestrate → [6])."
        )
        return

    # ── Search mode selector
    mode = st.radio(
        "Search mode",
        options=[
            "Upload image",
            "Record audio → generate cymatics",
            "Text query",
        ],
        horizontal=True,
        key="cymatics_mode",
    )

    top_k = st.slider(
        "Number of results",
        min_value=1, max_value=20, value=5,
        key="cymatics_top_k",
    )

    # ── Mode 1: Upload image
    if mode == "Upload image":
        uploaded = st.file_uploader(
            "Upload a cymatics or pattern image",
            type=["png", "jpg", "jpeg"],
            key="cymatics_image_upload",
        )
        if uploaded is None:
            st.info("Upload a pattern image to search for similar cymatics.")
            return

        image_bytes = uploaded.getvalue()
        st.image(image_bytes, caption="Uploaded pattern", width=300)

        if st.button("Find similar patterns", type="primary", key="cymatics_img_btn"):
            with st.spinner("Computing CLIP image embedding and searching..."):
                try:
                    from milvus_embeddings import connect_milvus, search_similar_patterns

                    milvus_client = connect_milvus()
                    results = search_similar_patterns(
                        milvus_client, image_bytes, top_k=top_k,
                    )
                except Exception as e:
                    st.error(f"Search failed: {e}")
                    return
            _render_cymatics_results(results)

    # ── Mode 2: Record audio → generate cymatics → search
    elif mode == "Record audio → generate cymatics":
        col_rec, col_upload = st.columns(2)

        audio_bytes = None
        with col_rec:
            st.markdown("**Record from microphone**")
            recorded = st.audio_input(
                "Record a sound clip",
                key="cymatics_audio_rec",
            )
            if recorded is not None:
                audio_bytes = recorded.getvalue()

        with col_upload:
            st.markdown("**Or upload a WAV file**")
            uploaded_wav = st.file_uploader(
                "Choose a WAV file",
                type=["wav"],
                key="cymatics_wav_upload",
            )
            if uploaded_wav is not None:
                audio_bytes = uploaded_wav.getvalue()

        if audio_bytes is None:
            st.info("Record or upload audio to generate a cymatics pattern.")
            return

        st.audio(audio_bytes, format="audio/wav")

        if st.button("Generate cymatics & search", type="primary", key="cymatics_aud_btn"):
            with st.spinner("Generating cymatics image from audio..."):
                try:
                    from milvus_embeddings import (
                        connect_milvus,
                        search_similar_patterns,
                        _load_audio_float32,
                    )
                    from tasks.cymatics_classification import generate_cymatics_image

                    sr, audio = _load_audio_float32(audio_bytes)
                    cymatics_png = generate_cymatics_image(audio, sr)
                except Exception as e:
                    st.error(f"Cymatics generation failed: {e}")
                    return

            st.image(cymatics_png, caption="Generated cymatics pattern", width=400)

            with st.spinner("Computing CLIP embedding and searching Milvus..."):
                try:
                    milvus_client = connect_milvus()
                    results = search_similar_patterns(
                        milvus_client, cymatics_png, top_k=top_k,
                    )
                except Exception as e:
                    st.error(f"Search failed: {e}")
                    return

            _render_cymatics_results(results)

    # ── Mode 3: Text query
    else:
        st.markdown(
            "Describe the pattern you're looking for — CLIP maps text and "
            "images to the same embedding space."
        )

        if "_cym_pending_query" in st.session_state:
            st.session_state["cymatics_text_query"] = st.session_state.pop(
                "_cym_pending_query"
            )

        query = st.text_input(
            "Pattern description",
            placeholder="e.g. star shaped pattern, circular rings, complex chaotic...",
            key="cymatics_text_query",
        )

        col_examples = st.columns(4)
        example_queries = [
            "star shaped pattern",
            "circular symmetric rings",
            "complex chaotic pattern",
            "simple geometric shape",
        ]
        for col, eq in zip(col_examples, example_queries):
            if col.button(eq, key=f"cym_ex_{eq[:8]}"):
                st.session_state["_cym_pending_query"] = eq
                st.rerun()

        if not query:
            st.info("Enter a description or click an example above.")
            return

        if st.button("Search by text", type="primary", key="cymatics_txt_btn"):
            with st.spinner("Computing CLIP text embedding and searching..."):
                try:
                    from milvus_embeddings import connect_milvus, search_patterns_by_text

                    milvus_client = connect_milvus()
                    results = search_patterns_by_text(
                        milvus_client, query, top_k=top_k,
                    )
                except Exception as e:
                    st.error(f"Search failed: {e}")
                    return

            _render_cymatics_results(results)


# ── Metadata search — natural-language query → MiniLM text embedding → Milvus ANN


def _render_metadata_search() -> None:
    """Metadata search tab — natural-language queries over sound descriptions."""
    st.subheader("Metadata Search — Natural Language Query")
    st.caption(
        "Ask questions about your sound library in plain English. "
        "Powered by all-MiniLM-L6-v2 text embeddings (384-dim) over "
        "auto-generated sound descriptions."
    )

    if not _check_milvus():
        st.warning(
            "Milvus is not reachable. Start it with "
            "`docker compose up -d milvus` and ensure text embeddings "
            "have been ingested (orchestrate → [6])."
        )
        return

    # ── Query input
    if "_meta_pending_query" in st.session_state:
        st.session_state["meta_text_query"] = st.session_state.pop(
            "_meta_pending_query"
        )

    query = st.text_input(
        "Your question",
        placeholder="e.g. what frequency do rain sounds have?",
        key="meta_text_query",
    )

    col_examples = st.columns(4)
    example_queries = [
        "rain sounds",
        "highly harmonic tonal",
        "low frequency sounds",
        "complex noisy recordings",
    ]
    for col, eq in zip(col_examples, example_queries):
        if col.button(eq, key=f"meta_ex_{eq[:8]}"):
            st.session_state["_meta_pending_query"] = eq
            st.rerun()

    if not query:
        st.info("Enter a question or click an example above.")
        return

    top_k = st.slider(
        "Number of results",
        min_value=1, max_value=20, value=5,
        key="meta_top_k",
    )

    if st.button("Search metadata", type="primary", key="meta_search_btn"):
        with st.spinner("Computing text embedding and searching Milvus..."):
            try:
                from milvus_embeddings import connect_milvus, search_by_text

                milvus_client = connect_milvus()
                results = search_by_text(
                    milvus_client, query, top_k=top_k,
                )
            except Exception as e:
                st.error(f"Search failed: {e}")
                return

        if not results:
            st.warning("No matching recordings found.")
            return

        st.success(f"Found {len(results)} matching recording(s).")
        st.divider()

        img_map = _image_path_map()

        for i, hit in enumerate(results):
            entity = hit["entity"]
            distance = hit["distance"]

            uuid = entity.get("uuid", "?")
            category = entity.get("category", "") or "—"
            source = entity.get("source", "") or "—"
            peak_hz = entity.get("peak_frequency_hz", 0)
            description = entity.get("description_text", "") or "—"
            image_path = img_map.get(uuid, "")

            with st.container():
                col_img, col_info = st.columns([1, 2])

                with col_img:
                    img_data = _load_cymatics_image(image_path)
                    if img_data is not None:
                        st.image(img_data, caption=f"#{i + 1} — {category}", width=280)
                    else:
                        st.markdown(f"**#{i + 1}** — *image not available*")

                with col_info:
                    m_cols = st.columns([2, 2, 2])
                    m_cols[0].metric("Category", category)
                    m_cols[1].metric("Similarity", f"{distance:.4f}")
                    m_cols[2].metric("Peak freq", f"{peak_hz:.0f} Hz")

                    with st.expander(f"Description — {uuid[:12]}…"):
                        st.markdown(f"**{category}** — {source}")
                        st.info(description)
                        st.markdown(
                            f"- **UUID:** `{uuid}`\n"
                            f"- **Source:** {source}\n"
                            f"- **Peak frequency:** {peak_hz:.1f} Hz\n"
                            f"- **Image path:** `{image_path}`\n"
                            f"- **Cosine similarity:** {distance:.6f}"
                        )

            if i < len(results) - 1:
                st.divider()


# ── Data governance — run quality checks (Great Expectations) and display lineage


def _render_governance() -> None:
    """Data governance tab — quality checks, lineage tracking, and data security."""
    st.subheader("Data Governance")
    st.caption(
        "File integrity and completeness checks via Great Expectations, "
        "cross-zone lineage tracking, and MinIO role-based access control."
    )

    mode = st.radio(
        "Governance task",
        options=["Quality Checks", "Lineage Tracking", "Data Security", "Data Catalog"],
        horizontal=True,
        key="gov_mode",
    )

    if mode == "Quality Checks":
        st.markdown(
            "Validates data quality at every zone boundary — structured "
            "metadata via Great Expectations, plus unstructured file "
            "integrity checks (WAV, PNG, video) against MinIO."
        )

        if st.button("Run quality checks", type="primary", key="gov_quality_btn"):
            with st.spinner("Running quality checks across all zones..."):
                try:
                    sys.path.insert(
                        0, str(Path(__file__).resolve().parents[1] / "governance"),
                    )
                    from data_quality import (
                        validate_landing_zone,
                        validate_trusted_zone,
                        validate_exploitation_zone,
                    )
                    from shared.minio_helpers import create_minio_client

                    minio_client = create_minio_client()

                    all_results: list[dict] = []
                    for zone_name, validator in [
                        ("Landing Zone", validate_landing_zone),
                        ("Trusted Zone", validate_trusted_zone),
                        ("Exploitation Zone", validate_exploitation_zone),
                    ]:
                        try:
                            results = validator(minio_client)
                            all_results.append({"zone": zone_name, "checks": results})
                        except Exception as e:
                            all_results.append({
                                "zone": zone_name,
                                "checks": [{"name": "Connection", "passed": 0, "failed": 1}],
                                "error": str(e),
                            })
                except Exception as e:
                    st.error(f"Quality checks failed: {e}")
                    return

            # Display results.
            total_pass = 0
            total_fail = 0

            for zone_result in all_results:
                zone = zone_result["zone"]
                checks = zone_result["checks"]

                if "error" in zone_result:
                    st.warning(f"**{zone}**: {zone_result['error']}")
                    continue

                z_pass = sum(c["passed"] for c in checks)
                z_fail = sum(c["failed"] for c in checks)
                total_pass += z_pass
                total_fail += z_fail

                icon = "✅" if z_fail == 0 else "⚠️"
                st.markdown(f"### {icon} {zone}")

                for check in checks:
                    p = check["passed"]
                    f = check["failed"]
                    total = p + f
                    name = check["name"]
                    failed_recs = check.get("failed_records", [])

                    if f == 0:
                        st.markdown(f"- ✓ **{name}** — {p}/{total} passed")
                    else:
                        st.markdown(
                            f"- ✗ **{name}** — {p}/{total} passed, "
                            f"{f} failed"
                        )
                        if failed_recs:
                            with st.expander(
                                f"Show {len(failed_recs)} failing record(s)"
                            ):
                                for rec in failed_recs:
                                    uid = rec.get("uuid", "?")
                                    val = rec.get("value", "")
                                    cat = rec.get("category", "")
                                    src = rec.get("source", "")
                                    path = rec.get("path", "")
                                    reason = rec.get("reason", "")

                                    parts = [f"**UUID:** `{uid}`"]
                                    if cat:
                                        parts.append(f"**Category:** {cat}")
                                    if src:
                                        parts.append(f"**Source:** {src}")
                                    if val != "":
                                        parts.append(f"**Value:** `{val}`")
                                    if path:
                                        parts.append(f"**Path:** `{path}`")
                                    if reason:
                                        parts.append(f"**Reason:** {reason}")

                                    st.markdown(
                                        " &nbsp;|&nbsp; ".join(parts)
                                    )

                st.divider()

            if total_fail == 0:
                st.success(
                    f"All checks passed — {total_pass} expectations met."
                )
            else:
                st.error(
                    f"{total_fail} check(s) failed — "
                    f"{total_pass}/{total_pass + total_fail} passed."
                )

    elif mode == "Lineage Tracking":
        st.markdown(
            "Traces every record (UUID) across all pipeline zones: "
            "Landing → Trusted → Exploitation → Milvus embeddings."
        )

        if st.button("Build lineage table", type="primary", key="gov_lineage_btn"):
            with st.spinner("Building lineage across all zones and Milvus..."):
                try:
                    sys.path.insert(
                        0, str(Path(__file__).resolve().parents[1] / "governance"),
                    )
                    from lineage_tracker import build_lineage, save_lineage
                    from shared.minio_helpers import create_minio_client

                    minio_client = create_minio_client()
                    lineage = build_lineage(minio_client)
                    save_lineage(minio_client, lineage)
                except Exception as e:
                    st.error(f"Lineage tracking failed: {e}")
                    return

            if not lineage:
                st.warning("No lineage records found.")
                return

            # Summary metrics.
            total = len(lineage)
            full = sum(1 for r in lineage if r["completeness"] == 1.0)
            partial = sum(1 for r in lineage if 0 < r["completeness"] < 1.0)

            col1, col2, col3 = st.columns(3)
            col1.metric("Total records", total)
            col2.metric("Full pipeline", full)
            col3.metric("Partial", partial)

            st.divider()

            # Per-stage bar.
            stage_data = {
                "Stage": [
                    "Landing", "Trusted", "Exploitation",
                    "Audio emb.", "Text emb.", "Cymatics emb.",
                ],
                "Count": [
                    sum(1 for r in lineage if r["in_landing"]),
                    sum(1 for r in lineage if r["in_trusted"]),
                    sum(1 for r in lineage if r["in_exploitation"]),
                    sum(1 for r in lineage if r["has_audio_embedding"]),
                    sum(1 for r in lineage if r["has_text_embedding"]),
                    sum(1 for r in lineage if r["has_cymatics_embedding"]),
                ],
            }
            stage_df = pd.DataFrame(stage_data)

            fig = px.bar(
                stage_df,
                x="Stage",
                y="Count",
                color="Stage",
                labels={"Count": "Records present"},
                text="Count",
            )
            fig.update_layout(
                height=350,
                title_text="Records per pipeline stage",
                showlegend=False,
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)

            # Detailed records.
            st.subheader("Record details")
            show_count = min(total, 20)

            for i, rec in enumerate(lineage[:show_count]):
                uid = rec["uuid"]
                comp = rec["completeness"]
                category = rec["category"]

                with st.expander(
                    f"{uid[:24]}… — {category} — {comp:.0%} complete"
                ):
                    st.progress(comp)

                    ic, tc, ec = st.columns(3)
                    ic.markdown(
                        f"**Landing:** {'✓' if rec['in_landing'] else '✗'}"
                    )
                    tc.markdown(
                        f"**Trusted:** {'✓' if rec['in_trusted'] else '✗'}"
                    )
                    ec.markdown(
                        f"**Exploitation:** "
                        f"{'✓' if rec['in_exploitation'] else '✗'}"
                    )

                    st.markdown("**Transformation chain:**")
                    for j, t in enumerate(rec["transformations"], 1):
                        st.markdown(f"{j}. {t}")

            if total > show_count:
                st.info(f"Showing first {show_count} of {total} records.")

    elif mode == "Data Security":
        st.markdown(
            "Role-based access control for MinIO pipeline zones. "
            "Creates IAM users and attaches policies per role."
        )

        # Show access matrix.
        st.markdown("#### Access Control Matrix")
        matrix_data = {
            "Role": ["pipeline_admin", "data_engineer", "data_scientist", "analyst"],
            "Landing": ["RW", "RW", "—", "R"],
            "Trusted": ["RW", "RW", "R", "R"],
            "Exploitation": ["RW", "R", "RW", "R"],
        }
        st.table(pd.DataFrame(matrix_data).set_index("Role"))

        col_apply, col_verify = st.columns(2)

        with col_apply:
            if st.button(
                "Apply security policies",
                type="primary",
                key="gov_security_apply_btn",
            ):
                with st.spinner("Creating users and attaching MinIO policies..."):
                    try:
                        sys.path.insert(
                            0,
                            str(Path(__file__).resolve().parents[1] / "governance"),
                        )
                        from data_security import (
                            apply_security_policies,
                            save_security_report,
                            display_access_matrix,
                        )
                        from shared.minio_helpers import create_minio_client

                        results = apply_security_policies()
                        minio_client = create_minio_client()
                        save_security_report(minio_client, results, {})
                        st.session_state["_security_results"] = results
                    except Exception as e:
                        st.error(f"Failed to apply policies: {e}")
                        return

                st.success(
                    f"Applied {len(results)} role policies successfully."
                )

                for r in results:
                    role = r["role"]
                    status = r["user_status"]
                    perms = r["permissions"]
                    buckets = ", ".join(
                        f"{b} ({a})" for b, a in perms.items()
                    )
                    st.markdown(
                        f"- **{role}** ({status}): {buckets}"
                    )

        with col_verify:
            if st.button(
                "Verify access controls",
                type="secondary",
                key="gov_security_verify_btn",
            ):
                with st.spinner("Testing read/write access for each role..."):
                    try:
                        sys.path.insert(
                            0,
                            str(Path(__file__).resolve().parents[1] / "governance"),
                        )
                        from data_security import (
                            ROLES,
                            apply_security_policies,
                            verify_access,
                            save_security_report,
                        )
                        from shared.minio_helpers import create_minio_client

                        results = st.session_state.get("_security_results")
                        if results is None:
                            results = apply_security_policies()
                            st.session_state["_security_results"] = results

                        verification = {}
                        for role_name, role_config in ROLES.items():
                            checks = verify_access(role_name, role_config)
                            verification[role_name] = checks

                        minio_client = create_minio_client()
                        save_security_report(
                            minio_client, results, verification,
                        )
                    except Exception as e:
                        st.error(f"Access verification failed: {e}")
                        return

                all_passed = True
                for role_name, checks in verification.items():
                    role_ok = all(c["passed"] for c in checks)
                    icon = "✅" if role_ok else "❌"
                    st.markdown(f"### {icon} {role_name}")

                    for c in checks:
                        bucket = c["bucket"]
                        expected = c["expected"]
                        actual_parts = []
                        if c["can_read"]:
                            actual_parts.append("R")
                        if c["can_write"]:
                            actual_parts.append("W")
                        actual = "".join(actual_parts) or "—"
                        exp_label = {
                            "readwrite": "RW",
                            "readonly": "R",
                            "none": "—",
                        }.get(expected, expected)

                        check_icon = "✓" if c["passed"] else "✗"
                        st.markdown(
                            f"- {check_icon} **{bucket}** — "
                            f"expected: {exp_label}, actual: {actual}"
                        )

                        if not c["passed"]:
                            all_passed = False

                st.divider()
                if all_passed:
                    st.success("All access checks passed.")
                else:
                    st.error("Some access checks failed.")

    elif mode == "Data Catalog":
        st.markdown(
            "Registers the five data products of the Sound Analysis & Cymatics "
            "domain (ownership, storage, contract, consumers, lineage) and "
            "monitors their live health. Persisted as a DCAT JSON-LD catalog in "
            "the governance bucket."
        )

        if st.button("Build catalog", type="primary", key="gov_catalog_btn"):
            with st.spinner("Probing data-product health across MinIO and Milvus..."):
                try:
                    sys.path.insert(
                        0, str(Path(__file__).resolve().parents[1] / "governance"),
                    )
                    from data_catalog import build_catalog, save_catalog
                    from shared.minio_helpers import create_minio_client

                    minio_client = create_minio_client()
                    catalog = build_catalog(minio_client)
                    save_catalog(minio_client, catalog)
                except Exception as e:
                    st.error(f"Data catalog failed: {e}")
                    return

            products = catalog["products"]
            available = sum(1 for p in products if p["health"]["status"] == "available")

            col1, col2, col3 = st.columns(3)
            col1.metric("Data products", catalog["n_products"])
            col2.metric("Available", available)
            col3.metric("Domain", catalog["domain"])

            st.divider()

            status_icon = {
                "available": "✅",
                "empty": "⚪",
                "partial": "🟠",
                "missing": "❌",
                "unreachable": "❔",
            }

            catalog_rows = [
                {
                    "Product": p["id"],
                    "Type": p["type"],
                    "Owner": p["owner"],
                    "Storage": p["storage"]["system"],
                    "Status": f"{status_icon.get(p['health']['status'], '·')} "
                              f"{p['health']['status']}",
                    "Detail": p["health"]["detail"],
                }
                for p in products
            ]
            st.dataframe(pd.DataFrame(catalog_rows), use_container_width=True)

            for p in products:
                with st.expander(f"{p['id']} — {p['title']}"):
                    st.markdown(f"**Owner:** `{p['owner']}`")
                    st.markdown(f"**Description:** {p['description']}")
                    st.markdown(
                        f"**Storage:** {p['storage']['system']} "
                        f"(`{p['storage']['location']}`, {p['storage']['format']})"
                    )
                    st.markdown(f"**Schema:** {p['schema']}")
                    st.markdown(f"**Data contract:** {p['data_contract']}")
                    st.markdown(f"**Derived from:** {', '.join(p['derived_from'])}")
                    st.markdown(f"**Consumers:** {', '.join(p['consumers'])}")


# ── Main layout — sidebar navigation + tab routing to each consumption task


def main() -> None:
    st.title("BDM Cymatics — Data Consumption Dashboard")

    # ── Top-level tabs
    tab_kpis, tab_audio, tab_cymatics, tab_metadata, tab_gov = st.tabs([
        "KPI Dashboard",
        "Audio Classification",
        "Cymatics Classification",
        "Metadata Search",
        "Data Governance",
    ])

    # ── Tab 1: KPI Dashboard
    with tab_kpis:
        obs, uri, version = _try_load_data()
        row_count = len(obs)

        st.markdown(
            f"**Source:** `{uri}` &nbsp;|&nbsp; "
            f"**Delta version:** {version} &nbsp;|&nbsp; "
            f"**Observations:** {row_count}"
        )
        st.divider()

        # ── Sidebar: KPI selector
        with st.sidebar:
            st.header("KPI Selection")
            selected = st.radio(
                "Choose a KPI to display",
                options=["All KPIs"] + [
                    f"{k}. {v['title']}" for k, v in KPI_RENDERERS.items()
                ],
                index=0,
            )

            st.divider()
            if st.button("Refresh data"):
                _load_data.clear()
                st.rerun()

        # ── Render selected KPI(s)
        if selected == "All KPIs":
            for key in sorted(KPI_RENDERERS.keys(), key=int):
                KPI_RENDERERS[key]["renderer"](obs)
                st.divider()
        else:
            key = selected.split(".")[0]
            KPI_RENDERERS[key]["renderer"](obs)

    # ── Tab 2: Audio Classification
    with tab_audio:
        _render_audio_classification()

    # ── Tab 3: Cymatics Classification
    with tab_cymatics:
        _render_cymatics_classification()

    # ── Tab 4: Metadata Search
    with tab_metadata:
        _render_metadata_search()

    # ── Tab 5: Data Governance
    with tab_gov:
        _render_governance()


if __name__ == "__main__":
    main()
