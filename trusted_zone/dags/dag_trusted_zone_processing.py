"""
Airflow DAG — Trusted-zone processing (Spark batch + cymatics enrichment).

Runs every two weeks and executes the full trusted-zone pipeline:

  1. Spark (Docker): landing metadata QA → pending workset
     (trim, dedupe on uuid, anti-join UUIDs already in trusted-zone metadata).
  2. Python: cymatics image + video per pending row → trusted-zone bucket.
  3. Append trusted CSV + Parquet and sync Delta Lake.

Deduplication: Spark excludes rows whose uuid already exists in trusted-zone
metadata; Python only processes the pending workset. Re-runs are safe — no
duplicate trusted records when landing data is unchanged.

Requires Docker socket access from the Airflow worker (trusted-spark-batch).
"""

import logging
import os
import sys
import time as _time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "cymatics",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(hours=8),
    "email_on_failure": False,
}


def _run_trusted_zone_processing(**context):
    """Import and run trusted_zone_processing (Spark + Python)."""
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "manual"
    execution_date = context.get("execution_date", "N/A")

    log.info("=" * 60)
    log.info("Trusted-zone processing — START")
    log.info("  DAG run id       : %s", run_id)
    log.info("  Execution date   : %s", execution_date)
    log.info(
        "  Deduplication    : Spark anti-join on trusted UUIDs; "
        "Python processes pending workset only"
    )
    log.info("=" * 60)

    project_root = os.environ.get(
        "CYMATICS_PROJECT_ROOT",
        os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)),
    )
    trusted_dir = os.path.join(project_root, "trusted_zone")

    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    if trusted_dir not in sys.path:
        sys.path.insert(1, trusted_dir)

    from dotenv import load_dotenv

    dotenv_path = os.path.join(project_root, ".env")
    if os.path.isfile(dotenv_path):
        load_dotenv(dotenv_path, override=False)
        log.info(
            "  Loaded .env from %s (override=False, docker-compose env takes priority)",
            dotenv_path,
        )

    log.info("  Project root     : %s", project_root)
    log.info("  MINIO_ENDPOINT   : %s", os.environ.get("MINIO_ENDPOINT", "(not set)"))
    log.info("  TRUSTED_ZONE_BUCKET: %s", os.environ.get("TRUSTED_ZONE_BUCKET", "trusted-zone"))
    log.info("  LANDING_ZONE_BUCKET: %s", os.environ.get("LANDING_ZONE_BUCKET", "landing-zone"))

    import trusted_zone_processing

    t0 = _time.time()
    trusted_zone_processing.run()
    elapsed = _time.time() - t0

    log.info("=" * 60)
    log.info("Trusted-zone processing — DONE  (%.1f s)", elapsed)
    log.info("=" * 60)


with DAG(
    dag_id="trusted_zone_processing",
    default_args=default_args,
    description=(
        "Biweekly trusted-zone pipeline: Spark metadata QA (dedupe + anti-join) "
        "then cymatics enrichment into trusted-zone"
    ),
    schedule_interval=timedelta(weeks=2),
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["trusted-zone", "spark", "cymatics"],
) as dag:

    process_trusted_zone = PythonOperator(
        task_id="run_trusted_zone_processing",
        python_callable=_run_trusted_zone_processing,
        provide_context=True,
    )


