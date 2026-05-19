# Cymatics — Sound-Driven Pattern Generation & Analysis Pipeline

Big Data Management project that transforms audio files into visual cymatics patterns, extracts acoustic features, and organizes all artifacts into a structured, queryable data lakehouse.


## Prerequisites

- **Docker** and **Docker Compose** (for MinIO, Kafka, Zookeeper, Airflow, optional SonarQube)
- **Python 3.10+**
- **ffmpeg** (for audio decoding in cold path)
- A microphone (for warm and hot paths)

## Quick Start

### 1. Environment setup

```bash
cp env.example .env
# Edit .env — set FREESOUND_API_KEY, MINIO credentials, Airflow credentials, ...
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

This starts: **Zookeeper**, **Kafka**, **MinIO** (console at http://localhost:9001), **PostgreSQL** (Airflow metadata DB), **Airflow webserver** (http://localhost:8080), **Airflow scheduler**, and **SonarQube** (http://localhost:9090 — first boot can take about a minute before the UI is ready).

### 4. Run pipelines

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
| 5 | Trusted zone | Cymatics + peak metadata from landing-zone |
| 6 | Exploitation zone | Spark spectral features + Python MFCC/harmonic |
| 7 | Sync Delta Lake | All zones: Parquet → Delta tables (also runs automatically after each zone) |
| 8 | SonarQube | Run `sonar-scanner` against this repo and print quality metrics |

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

**Trusted zone** (cymatics image/video + peak metadata; no spectral features):

```bash
python trusted_zone/trusted_zone_processing.py
```

**Exploitation zone** (feature derivation — Spark batch in Docker, then host Python):

```bash
docker compose up -d
python exploitation_zone/exploitation_zone_processing.py
# Spark-only batch: docker compose run --rm exploitation-spark-batch
```

**Delta sync** (runs automatically at the end of each zone’s processing script; manual):

```bash
python shared/sync_delta.py              # all zones
python shared/sync_delta.py trusted        # one zone
```

**Data consumption** (Jupyter — inspect Delta tables across zones):

Open `data_consumption/notebooks/delta_lake_discovery.ipynb` after MinIO is up and zone processing has run. KPIs: orchestrator option **9** → `data_consumption/tasks/discover_kpis.py`, or `data_consumption/notebooks/kpi_queries.ipynb`.

#### Option C: Airflow (automated scheduling)

Open http://localhost:8080, log in with `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD` from `.env`, and unpause DAGs:

- `cold_freesound_ingestion` — weekly, up to 250 new Freesound sounds into landing-zone.
- `trusted_zone_processing` — every 2 weeks, full Spark + Python trusted pipeline (deduplicates on UUIDs already in trusted metadata).
- `exploitation_zone_processing` — every 15 days (first run 15 days after trusted anchor), Spark + Python exploitation pipeline (anti-join on exploitation UUIDs).

## Environment Variables

See `env.example` for all options:

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO API endpoint |
| `MINIO_ACCESS_KEY` | `admin` | MinIO access key |
| `MINIO_SECRET_KEY` | `password` | MinIO secret key |
| `LANDING_ZONE_BUCKET` | `landing-zone` | Landing-zone object-store bucket |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `FREESOUND_API_KEY` | — | Freesound API key ([get one](https://freesound.org/apiv2/apply/)) |
| `ESC50_BASE_PATH` | — | Local ESC-50 path (auto-downloads if missing) |
| `AIRFLOW_USERNAME` | `admin` | Airflow web UI username |
| `AIRFLOW_PASSWORD` | `admin` | Airflow web UI password |
| `SONAR_TOKEN` | — | SonarQube user token (preferred for scans); create at http://localhost:9090/account/security |
| `SONAR_USERNAME` | `admin` | Fallback scanner login if `SONAR_TOKEN` is unset |
| `SONAR_PASSWORD` | `admin` | Password for `SONAR_USERNAME` |
| `SONAR_JDBC_USERNAME` | `sonar` | PostgreSQL user for the SonarQube container (must match `docker-compose`) |
| `SONAR_JDBC_PASSWORD` | `sonar` | PostgreSQL password for SonarQube |

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

   - **Recommended:** `python orchestrate.py` → choose **\[8\] SonarQube code analysis**. The script checks that SonarQube is ready, runs the scanner using `sonar-project.properties`, then prints a short summary and links to the full dashboard: http://localhost:9090/dashboard?id=bdm-cymatics.

   - **Manual:** after setting `SONAR_TOKEN` (or `SONAR_USERNAME` + `SONAR_PASSWORD`) in `.env`, run `sonar-scanner` in this directory.

Use the SonarQube **Issues** and **Measures** views to drive incremental improvements (fix hotspots, reduce duplication, clear security hotspots) before merging larger changes.

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
    ├── observations_delta/          # Delta Lake (synced with Parquet)
    └── freesound_last_ingestion.txt # Checkpoint for incremental ingestion

trusted-zone/
├── audio/<peak_freq>/<uuid>-<peak_freq>.wav
├── images/<peak_freq>/<uuid>-<peak_freq>.png
├── videos/<peak_freq>/<uuid>-<peak_freq>.mp4
└── metadata/
    ├── observations.csv
    ├── observations.parquet
    ├── observations_delta/           # Delta Lake (synced with Parquet)
    └── pending_workset.json          # Spark batch → trusted_zone_processing

exploitation-zone/
└── metadata/
    ├── observations.csv              # trusted columns + derived features
    ├── observations.parquet
    ├── observations_delta/           # Delta Lake (synced with Parquet)
    └── spark_pending_workset.json    # Spark batch → exploitation_zone_processing

data_consumption/
├── tasks/
│   └── discover_kpis.py              # interactive KPI queries (orchestrator option 9)
└── notebooks/
    ├── delta_lake_discovery.ipynb    # inspect zone Delta tables
    └── kpi_queries.ipynb             # KPIs on exploitation-zone Delta (notebook exploration)
```

**Feature split (exploitation zone):**

| Layer | Features |
|-------|----------|
| Spark (`spark_exploitation_zone.py`) | `spectral_centroid_hz`, `spectral_bandwidth_hz`, `spectral_rolloff_hz`, `spectral_flatness`, `signal_energy`, `spectral_entropy`, `zero_crossing_rate`, `loudness` |
| Python (`exploitation_zone_processing.py`) | `MFCCs`, `harmonic_energy_ratio` |

## Authors

- Arman Bazarchi
- Brisa Fernanda Cisneros Cervantes
