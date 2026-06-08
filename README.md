# Cymatics — Sound-Driven Pattern Generation & Analysis Pipeline

Big Data Management project that transforms audio files into visual cymatics patterns, extracts acoustic features, generates audio / text / image embeddings, and organizes all artifacts into a structured, queryable data lakehouse with governance, role-based access and a Streamlit consumption dashboard.

## Prerequisites

- **Docker** and **Docker Compose** (for MinIO, Kafka, Zookeeper, Airflow, Milvus, Spark, Streamlit, SonarQube)
- **Python 3.10+**
- **ffmpeg** (for audio decoding in cold path)
- A microphone (for warm path, hot path, audio similarity search and cymatics search)

## Quick Start

### 1. Environment setup

```bash
cp env.example .env
# Edit .env — set FREESOUND_API_KEY, MINIO credentials, Airflow credentials, role-based keys, ...
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start infrastructure

```bash
# Build custom Airflow image (first time only)
docker compose build

# Start all services
docker compose up -d
```

This starts:

| Service | Endpoint | Purpose |
|---|---|---|
| **MinIO** | http://localhost:9001 | Object store for all zones |
| **Zookeeper + Kafka + Kafka UI** | http://localhost:8085 | Hot-path event streaming |
| **Spark master + 2 workers + history** | http://localhost:8081 | Distributed batch processing |
| **Milvus + Attu** | http://localhost:3000 | Vector store + admin UI |
| **Airflow webserver + scheduler** | http://localhost:8080 | DAG-based scheduling |
| **Streamlit dashboard** | http://localhost:8501 | Unified consumption + governance UI |
| **SonarQube** | http://localhost:9090 | Code quality (first boot ~1 min) |
| **PostgreSQL** (Airflow, SonarQube) | — | Metadata DBs |

### 4. Apply MinIO access policies (first run only)

The pipeline scripts authenticate against MinIO with the `pipeline_admin` role and the consumption layer with the `analyst` role declared in `.env`. Those IAM users do **not** exist on a fresh MinIO, so before running any ingestion you must create them once:

```bash
python governance/data_security.py
# choose [1] Apply security policies
```

This provisions the four roles (`pipeline_admin`, `data_engineer`, `data_scientist`, `analyst`) with the bucket-level RW/R/no-access matrix described in the report. You only need to do this again if you wipe MinIO (`docker compose down -v` plus `rm -rf minio/*`).

### 5. Run pipelines

#### Option A: Manual Orchestrator (recommended)

```bash
python orchestrate.py
```

Interactive menu with live CPU/RAM monitoring. Supports all flows:

| # | Flow | Description |
|---|------|-------------|
| 1 | Warm path | Record 5s → preview cymatics → approve/reject → store audio |
| 2 | Hot path | Producer (live viz + Kafka) + consumer in parallel |
| 3 | Cold: Freesound | Batch ingest from Freesound API |
| 4 | Cold: ESC-50 | Batch ingest from ESC-50 dataset |
| 5 | Trusted zone | Spark cleaning + cymatics PNG/MP4 + peak metadata |
| 6 | Exploitation zone | Spark spectral features → Python MFCC/harmonic → Milvus embeddings → classifier-head training |
| 7 | SonarQube | Run `sonar-scanner` against this repo and print quality metrics |
| 8 | Data consumption | Sub-menu: KPI discovery, audio search, cymatics search, metadata search, Streamlit |
| 9 | Data governance | Sub-menu: Great Expectations checkpoints, MinIO RBAC, lineage tracking |

> Delta Lake sync runs automatically at the end of each zone’s processing script — no separate menu entry. Milvus embedding ingestion **and classifier-head training** are part of the exploitation-zone flow.

#### Option B: Run scripts directly

**Warm path** (records from microphone, user approval):

```bash
python landing_zone/warm_path/landing_zone_warm.py
```

**Hot path** (two terminals):

```bash
# Terminal 1 — consumer (waits for Kafka messages)
python landing_zone/hot_path/landing_zone_hot_consumer.py

# Terminal 2 — producer (live cymatics + uploads every 5s)
python landing_zone/hot_path/landing_zone_hot_producer.py
```

**Cold path — Freesound** (batch, prompts for batch size):

```bash
python landing_zone/cold_path/cold_freesound.py 50
```

**Cold path — ESC-50** (batch, prompts for batch size):

```bash
python landing_zone/cold_path/cold_esc50.py 50
```

**Trusted zone** (Spark cleaning + cymatics image/video + peak metadata):

```bash
python trusted_zone/trusted_zone_processing.py
```

**Exploitation zone** (Spark spectral features → Python MFCC/harmonic → Milvus embeddings → classifier-head training):

```bash
docker compose up -d
python exploitation_zone/exploitation_zone_processing.py
# Spark-only batch: docker compose run --rm exploitation-spark-batch
# Re-run Milvus ingestion only: python exploitation_zone/milvus_embeddings.py
# Re-train classifier heads only: python exploitation_zone/classifier_training.py
```

**Delta sync** (runs automatically at the end of each zone; manual override):

```bash
python shared/sync_delta.py              # all zones
python shared/sync_delta.py trusted      # one zone
```

**Data consumption tasks** (each module also runs through the orchestrator or the Streamlit dashboard):

```bash
python data_consumption/tasks/discover_kpis.py          # KPI catalogue
python data_consumption/tasks/audio_classification.py    # mic → PANNs → trained head + Milvus ANN evidence
python data_consumption/tasks/cymatics_classification.py # image/audio/text → CLIP → trained head + Milvus ANN evidence
python data_consumption/tasks/search_metadata.py         # natural-language metadata search (retrieval only)
streamlit run data_consumption/data_consumption_all.py   # unified dashboard
```

**Governance tasks**:

```bash
python governance/data_quality.py     # Great Expectations checkpoints (Landing → Trusted → Exploitation → Milvus)
python governance/data_security.py    # MinIO IAM: 4 roles, bucket-level policies
python governance/lineage_tracker.py  # cross-zone lineage table per UUID
python governance/data_catalog.py     # DCAT data-product catalog with live health
```

#### Option C: Airflow (automated scheduling)

Open http://localhost:8080, log in with `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD` from `.env`, and unpause DAGs:

- `cold_freesound_ingestion` — weekly, up to 250 new Freesound sounds into landing-zone.
- `trusted_zone_processing` — every 2 weeks, full Spark + Python trusted pipeline (deduplicates on UUIDs already in trusted metadata).
- `exploitation_zone_processing` — every 15 days (first run 15 days after trusted anchor), Spark + Python exploitation pipeline + Milvus embedding ingestion (anti-join on exploitation UUIDs).

## Environment Variables

See `env.example` for all options:

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO API endpoint |
| `MINIO_ACCESS_KEY` | `admin` | MinIO root access key (used when role-based keys are unset) |
| `MINIO_SECRET_KEY` | `password` | MinIO root secret key |
| `MINIO_PIPELINE_ACCESS_KEY` / `MINIO_PIPELINE_SECRET_KEY` | — | RW credentials used by ingestion / Spark / exploitation scripts |
| `MINIO_ANALYST_ACCESS_KEY` / `MINIO_ANALYST_SECRET_KEY` | — | Read-only credentials used by data consumption + Streamlit |
| `LANDING_ZONE_BUCKET` | `landing-zone` | Landing-zone object-store bucket |
| `TRUSTED_ZONE_BUCKET` | `trusted-zone` | Trusted-zone object-store bucket |
| `EXPLOITATION_ZONE_BUCKET` | `exploitation-zone` | Exploitation-zone object-store bucket |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `MILVUS_HOST` | `localhost` | Milvus host |
| `MILVUS_PORT` | `19530` | Milvus gRPC port |
| `FREESOUND_API_KEY` | — | Freesound API key ([get one](https://freesound.org/apiv2/apply/)) |
| `ESC50_BASE_PATH` | — | Local ESC-50 path (auto-downloads if missing) |
| `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` | `admin` / `admin` | Airflow web UI credentials |
| `SONAR_TOKEN` | — | SonarQube user token (preferred for scans) |
| `SONAR_USERNAME` / `SONAR_PASSWORD` | `admin` / `admin` | Fallback scanner login |
| `SONAR_JDBC_USERNAME` / `SONAR_JDBC_PASSWORD` | `sonar` / `sonar` | PostgreSQL credentials for the SonarQube container |

## Embeddings & Milvus Collections

The exploitation pipeline materialises three Milvus collections (HNSW / COSINE, `M=16`, `efConstruction=256`), all keyed on UUID with idempotent upsert:

| Collection | Model | Dim | Use case |
|---|---|---|---|
| `sound_audio_embeddings` | PANNs CNN14 | 2048 | Audio similarity search + feature input for the trained audio classifier head |
| `sound_text_embeddings` | all-MiniLM-L6-v2 | 384 | Natural-language metadata search (RAG-ready) |
| `sound_cymatics_embeddings` | CLIP ViT-B/32 | 512 | Image / audio / text → cymatics pattern search + feature input for the trained cymatics classifier head |

Inspect collections through Attu at http://localhost:3000.

## Trained Classifier Heads

The exploitation pipeline closes by training two logistic-regression heads on top of the frozen PANNs and CLIP embeddings (audio and cymatics modalities only — text is excluded because its description embeds the category). Each head is a `StandardScaler → LogisticRegression(class_weight="balanced")` pipeline fitted with a stratified 80/20 hold-out split + 5-fold stratified cross-validation and persisted to MinIO with a JSON model card:

```
exploitation-zone/models/
├── audio_classifier.joblib          # joblib-serialised sklearn pipeline
├── audio_classifier.metrics.json    # model card: CV + test accuracy / F1, per-class P/R, CM, model_version
├── cymatics_classifier.joblib
└── cymatics_classifier.metrics.json
```

Training is gated on having at least 2 categories with ≥3 samples each, so it skips cleanly on tiny datasets. The validation notebook (`exploitation_zone/notebooks/classifier_validation.ipynb`) renders generalisation plots (base-vs-trained comparison, per-class metrics, learning curves, confusion matrices) and saves the headline figures to `docs/`.

## Data Governance

Four governance mechanisms ship with the project; each runs via the orchestrator (option 9) and via the Streamlit dashboard.

- **Data quality** (`governance/data_quality.py`) — Great Expectations checkpoints at the three zone boundaries (Landing → Trusted, Trusted → Exploitation, and Milvus collections).
- **Data security** (`governance/data_security.py`) — MinIO IAM with four roles enforced through `mc admin`:

  | Role | Landing | Trusted | Exploitation | Governance |
  |---|---|---|---|---|
  | `pipeline_admin` | RW | RW | RW | RW |
  | `data_engineer` | RW | RW | R | R |
  | `data_scientist` | — | R | RW | R |
  | `analyst` | R | R | R | R |

- **Lineage tracking** (`governance/lineage_tracker.py`) — UUID-indexed cross-zone table reporting which assets exist at each stage (WAV / PNG / MP4 / audio / text / cymatics embeddings) and pipeline completeness.
- **Data catalog** (`governance/data_catalog.py`) — registers the five data products (owner, storage, schema, contract, consumers, lineage) and probes their live health, persisted as a DCAT JSON-LD catalog under `governance-zone/catalog/`.

## SonarQube (code quality)

SonarQube tracks **bugs**, **vulnerabilities**, **code smells**, **duplication**, and **maintainability** so you can prioritize refactors and harden the Python pipelines over time. Configuration lives in `sonar-project.properties` (project key `bdm-cymatics`, Python sources and sensible exclusions for `data/`, `venv/`, etc.).

1. **Start the server** (if it is not already up from `docker compose up -d`):

   ```bash
   docker compose up -d sonarqube
   ```

   Open http://localhost:9090, complete the first-time setup if prompted, and align `.env` with `env.example` for `SONAR_JDBC_*` and scanner auth.

2. **Install the scanner** on your machine (the UI alone does not analyze code):

   ```bash
   brew install sonar-scanner
   ```

   Or follow the [official SonarScanner install guide](https://docs.sonarsource.com/sonarqube/latest/analyzing-source-code/scanners/sonarscanner/).

3. **Run an analysis** from the project root:

   - **Recommended:** `python orchestrate.py` → choose **\[7\] SonarQube code analysis**. The script checks that SonarQube is ready, runs the scanner using `sonar-project.properties`, then prints a short summary and links to the full dashboard: http://localhost:9090/dashboard?id=bdm-cymatics.

   - **Manual:** after setting `SONAR_TOKEN` (or `SONAR_USERNAME` + `SONAR_PASSWORD`) in `.env`, run `sonar-scanner` in this directory.

Use the SonarQube **Issues** and **Measures** views to drive incremental improvements (fix hotspots, reduce duplication, clear security hotspots) before merging larger changes.

## Repository Layout

```
landing_zone/
├── warm_path/                       # mic → preview → approval → store
├── hot_path/                        # mic → Kafka producer + consumer
├── cold_path/                       # Freesound + ESC-50 batch ingestion
└── notebooks/

trusted_zone/
├── trusted_zone_processing.py       # Spark cleaning + cymatics PNG/MP4 + metadata persist
├── spark_trusted_zone.py            # Spark batch (dedup, schema, Kafka merge)
├── dags/                            # Airflow DAG (every 2 weeks)
└── notebooks/

exploitation_zone/
├── exploitation_zone_processing.py  # orchestrates Spark → Python → Milvus → classifier training
├── spark_exploitation_zone.py       # 8 spectral features via Spark
├── milvus_embeddings.py             # PANNs / MiniLM / CLIP → 3 Milvus collections
├── classifier_training.py           # logistic-regression heads on audio + cymatics embeddings → MinIO
├── dags/                            # Airflow DAG (every 15 days)
└── notebooks/                       # includes classifier_training.ipynb and classifier_validation.ipynb

data_consumption/
├── data_consumption_all.py          # unified Streamlit dashboard
├── tasks/
│   ├── discover_kpis.py             # KPI catalogue over exploitation Delta
│   ├── audio_classification.py      # mic → PANNs → trained head + Milvus ANN evidence
│   ├── cymatics_classification.py   # image / audio / text → CLIP → trained head + Milvus ANN evidence
│   └── search_metadata.py           # text → MiniLM → Milvus ANN (retrieval only)
└── notebooks/

governance/
├── data_quality.py                  # Great Expectations checkpoints
├── data_security.py                 # MinIO IAM (4 roles, bucket policies)
├── lineage_tracker.py               # UUID-indexed cross-zone lineage
├── data_catalog.py                  # DCAT data-product catalog + live health
└── notebooks/

shared/
├── minio_helpers.py                 # MinIO clients (pipeline/analyst) + CSV/Parquet helpers
├── sync_delta.py                    # Parquet → Delta sync with typed per-zone schema
├── cymatics_engine.py               # cymatics rendering engine
└── freq_detection.py                # peak-frequency / harmonic detection

docker/                              # Custom Dockerfiles (Airflow, Spark batches, Streamlit)
kafka/                               # Kafka config
minio/                               # MinIO config
docs/                                # Project statement, P2 final report (LaTeX), diagrams
orchestrate.py                       # CLI orchestrator with resource monitor
docker-compose.yml                   # Full stack
```

## MinIO Bucket Layout

```
landing-zone/
├── audio/
│   ├── warm-path/<peak_freq>/<uuid>-<peak_freq>.wav
│   ├── hot-path/<peak_freq>/<uuid>-<peak_freq>.wav
│   ├── Freesound/<peak_freq>/<category>_<freq>_<uuid>.wav
│   └── ESC-50/<peak_freq>/<category>_<freq>_<uuid>.wav
└── metadata/
    ├── observations.csv
    ├── observations.parquet
    ├── observations_delta/          # Delta Lake (synced with Parquet, typed schema)
    └── freesound_last_ingestion.txt # Checkpoint for incremental ingestion

trusted-zone/
├── audio/<peak_freq>/<uuid>-<peak_freq>.wav
├── images/<peak_freq>/<uuid>-<peak_freq>.png
├── videos/<peak_freq>/<uuid>-<peak_freq>.mp4
└── metadata/
    ├── observations.csv
    ├── observations.parquet
    ├── observations_delta/          # Delta Lake (typed schema)
    └── pending_workset.json         # Spark batch handoff

exploitation-zone/
├── metadata/
│   ├── observations.csv             # trusted columns + derived features
│   ├── observations.parquet
│   ├── observations_delta/          # Delta Lake (typed schema)
│   └── spark_pending_workset.json   # Spark batch handoff
└── models/                          # trained classifier heads + JSON model cards
    ├── audio_classifier.joblib
    ├── audio_classifier.metrics.json
    ├── cymatics_classifier.joblib
    └── cymatics_classifier.metrics.json

governance-zone/
├── security/
│   └── security_report.json         # data-security audit report (created by data_security.py)
└── catalog/
    ├── data_catalog.json            # readable data-product registry + live health
    └── data_catalog_dcat.jsonld     # DCAT (W3C) JSON-LD catalog (created by data_catalog.py)
```

**Feature split (exploitation zone):**

| Layer | Features |
|-------|----------|
| Spark (`spark_exploitation_zone.py`) | `spectral_centroid_hz`, `spectral_bandwidth_hz`, `spectral_rolloff_hz`, `spectral_flatness`, `signal_energy`, `spectral_entropy`, `zero_crossing_rate`, `loudness` |
| Python (`exploitation_zone_processing.py`) | `MFCCs`, `harmonic_energy_ratio` |
| Milvus (`milvus_embeddings.py`) | PANNs audio (2048-d), MiniLM text (384-d), CLIP cymatics (512-d) |
| Models (`classifier_training.py`) | `audio_classifier.joblib`, `cymatics_classifier.joblib` (logistic-regression heads on the audio and cymatics embeddings) |

## Authors

- Arman Bazarchi
- Brisa Fernanda Cisneros Cervantes
</content>
</invoke>