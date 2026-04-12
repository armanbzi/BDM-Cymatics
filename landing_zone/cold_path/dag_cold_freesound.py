"""
Airflow DAG — Cold-path Freesound ingestion.

Runs weekly and ingests up to 250 new sounds from Freesound into the
MinIO landing-zone.  Uses a MinIO-persisted checkpoint
(metadata/freesound_last_ingestion.txt) so each run only fetches sounds
created after the previous ingestion.

Retries, error handling, and timeout are configured on the DAG and task
level.

Future extensions (see task dependency comments at bottom):
  - validate_metadata: schema + quality checks on newly ingested rows
  - store_to_trusted_zone: trigger trusted-zone enrichment pipeline
  - trigger_ml_pipeline: kick off downstream ML training / inference
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

BATCH_SIZE = 250

default_args = {
    "owner": "cymatics",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
}


# ── task callables ──────────────────────────────────────────────────────────

def _ingest_freesound(**context):
    """Import and run the cold-path Freesound ingestion script."""
    import os
    import sys
    import time as _time

    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "manual"
    execution_date = context.get("execution_date", "N/A")

    log.info("=" * 60)
    log.info("Cold-path Freesound ingestion — START")
    log.info("  DAG run id       : %s", run_id)
    log.info("  Execution date   : %s", execution_date)
    log.info("  Batch size       : %d", BATCH_SIZE)
    log.info("  Retry policy     : %d retries, exponential backoff", default_args["retries"])
    log.info("=" * 60)

    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    )
    cold_path_dir = os.path.join(project_root, "landing_zone", "cold_path")
    if cold_path_dir not in sys.path:
        sys.path.insert(0, cold_path_dir)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from dotenv import load_dotenv
    dotenv_path = os.path.join(project_root, ".env")
    if os.path.isfile(dotenv_path):
        load_dotenv(dotenv_path, override=False)
        log.info("  Loaded .env from %s (override=False, docker-compose env takes priority)", dotenv_path)

    log.info("  MINIO_ENDPOINT   : %s", os.environ.get("MINIO_ENDPOINT", "(not set)"))
    log.info("  FREESOUND_API_KEY: %s", "(set)" if os.environ.get("FREESOUND_API_KEY") else "(not set)")

    import cold_freesound

    t0 = _time.time()
    cold_freesound.run(
        batch_size=BATCH_SIZE,
        created_after=None,
        update_checkpoint=True,
    )
    elapsed = _time.time() - t0

    log.info("=" * 60)
    log.info("Cold-path Freesound ingestion — DONE  (%.1f s)", elapsed)
    log.info("=" * 60)


# ── DAG definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id="cold_freesound_ingestion",
    default_args=default_args,
    description="Weekly ingestion of up to 250 new Freesound audio files into MinIO landing-zone",
    schedule_interval="@weekly",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cold-path", "freesound", "landing-zone"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_freesound_batch",
        python_callable=_ingest_freesound,
        provide_context=True,
    )

    # ── future extensions ────────────
    # validate_metadata 
    # store_to_trusted_zone 
    # ...
    