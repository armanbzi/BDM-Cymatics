# Cymatics — Sound-Driven Pattern Generation & Analysis Pipeline

Big Data Management project that transforms audio files into visual cymatics patterns, extracts acoustic features, and organizes all artifacts into a structured, queryable data lakehouse.


## Prerequisites

- **Docker** and **Docker Compose** (for MinIO, Kafka, Zookeeper, Airflow)
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

This starts: **Zookeeper**, **Kafka**, **MinIO** (console at http://localhost:9001), **PostgreSQL** (Airflow metadata DB), **Airflow webserver** (http://localhost:8080), and **Airflow scheduler**.

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
| 5 | Trusted zone | Enrich all landing-zone audio (features + cymatics) |
| 6 | Sync Delta Lake | Parquet → Delta Lake table |

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

**Trusted zone** (processes all unprocessed landing-zone records):

```bash
python trusted_zone/trusted_zone_processing.py
```

**Sync Delta Lake** (syncs Parquet metadata to Delta Lake table):

```bash
python exploitation_zone/sync_delta.py
```

#### Option C: Airflow (automated scheduling)

Open http://localhost:8080, log in with `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD` from `.env`, and unpause the `cold_freesound_ingestion` DAG. It runs weekly, ingesting up to 250 new sounds.

## Environment Variables

See `env.example` for all options:

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO API endpoint |
| `MINIO_ACCESS_KEY` | `admin` | MinIO access key |
| `MINIO_SECRET_KEY` | `password` | MinIO secret key |
| `MINIO_BUCKET` | `landing-zone` | Default bucket |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `FREESOUND_API_KEY` | — | Freesound API key ([get one](https://freesound.org/apiv2/apply/)) |
| `ESC50_BASE_PATH` | — | Local ESC-50 path (auto-downloads if missing) |
| `AIRFLOW_USERNAME` | `admin` | Airflow web UI username |
| `AIRFLOW_PASSWORD` | `admin` | Airflow web UI password |

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
    ├── observations_delta/          # Delta Lake table (after sync)
    └── freesound_last_ingestion.txt # Checkpoint for incremental ingestion

trusted-zone/
├── audio/<peak_freq>/<uuid>-<peak_freq>.wav
├── images/<peak_freq>/<uuid>-<peak_freq>.png
├── videos/<peak_freq>/<uuid>-<peak_freq>.mp4
└── metadata/
    ├── observations.csv
    └── observations.parquet
```

## Authors

- Arman Bazarchi
- Brisa Fernanda Cisneros Cervantes
