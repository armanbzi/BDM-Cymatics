#!/usr/bin/env python3
"""
Spark batch pipeline — Landing metadata → Trusted (metadata QA only).

Reads landing-zone and trusted-zone observations CSV directly from MinIO via S3A
(no Python download / re-upload). Applies reversible / information-preserving steps:

  - Trim string fields
  - Drop rows missing uuid or audio_path
  - Deduplicate on uuid (keep latest row ordered by ingest time when present)
  - Exclude rows whose uuid already exists in trusted-zone metadata
  - Left-join hot-path Kafka JSON archived in landing-zone (device, timestamp → time_recorded/added)

Per-record cymatics enrichment still runs sequentially in Python
(`trusted_zone_processing.process_record`). Spark writes the pending workset to MinIO;
the host orchestrator loads it after `docker compose run --rm trusted-spark-batch`.

Optional Spark event logs: `s3a://<TRUSTED_ZONE_BUCKET>/spark-logs/` when enabled.

Docker batch: `docker compose run --rm trusted-spark-batch` (or via
`run_spark_trusted_zone_subprocess()` from `trusted_zone_processing`).
Jupyter testing: set `SPARK_ALLOW_HOST_DRIVER=1` and S3A to reachable MinIO (see env.example).
"""

from __future__ import annotations

import glob
import json
import os
import re
import socket
import subprocess
import sys
from io import BytesIO
from pathlib import Path

from shared.minio_helpers import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    ensure_bucket,
    is_unreachable_minio,
)


def _minio_object_exists(client, bucket: str, key: str) -> bool:
    """HEAD object in MinIO (no body download)."""
    from minio.error import S3Error

    try:
        client.stat_object(bucket, key)
        return True
    except S3Error as e:
        code = getattr(e, "code", "") or ""
        if code in ("NoSuchKey", "NoSuchObject"):
            return False
        if code == "NoSuchBucket":
            raise RuntimeError(
                f"Bucket {bucket!r} does not exist (object {key!r}). "
                "Create it or fix LANDING_ZONE_BUCKET / TRUSTED_ZONE_BUCKET."
            ) from e
        msg = getattr(e, "message", str(e))
        raise RuntimeError(
            f"MinIO error checking {bucket!r}/{key!r} (code={code}): {msg}"
        ) from e
    except Exception as e:
        if is_unreachable_minio(e):
            raise RuntimeError(
                f"Cannot reach MinIO at {MINIO_ENDPOINT!r} while checking {bucket}/{key}."
            ) from e
        raise


def _java_major_version(java_bin: str) -> int | None:
    try:
        proc = subprocess.run(
            [java_bin, "-version"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        text = (proc.stderr or "") + (proc.stdout or "")
        m = re.search(r'version "(?:1\.)?(\d+)', text)
        return int(m.group(1)) if m else None
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def _discover_driver_java_homes() -> list[tuple[int, str]]:
    """Find Java 11/17 installs; return (preference_rank, JAVA_HOME) with 11 preferred over 17."""
    found: list[tuple[int, str]] = []
    seen: set[str] = set()

    def consider(home: str) -> None:
        home = home.strip()
        if not home or home in seen:
            return
        java_bin = os.path.join(home, "bin", "java")
        if not os.path.isfile(java_bin):
            return
        major = _java_major_version(java_bin)
        if major is None or major > SPARK_DRIVER_JAVA_MAX_MAJOR:
            return
        seen.add(home)
        rank = 0 if major == 11 else 1 if major == 17 else 2
        found.append((rank, home))

    if sys.platform == "darwin":
        for spec in ("11", "17"):
            try:
                proc = subprocess.run(
                    ["/usr/libexec/java_home", "-v", spec],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=True,
                )
                consider(proc.stdout.strip())
            except (OSError, subprocess.SubprocessError):
                pass
        jvm_root = Path("/Library/Java/JavaVirtualMachines")
        if jvm_root.is_dir():
            for jdk in sorted(jvm_root.glob("*.jdk")):
                consider(str(jdk / "Contents" / "Home"))

    if sys.platform.startswith("linux"):
        for pattern in (
            "/usr/lib/jvm/java-11-openjdk*",
            "/usr/lib/jvm/java-11-amazon-corretto*",
            "/usr/lib/jvm/java-17-openjdk*",
            "/usr/lib/jvm/java-17-amazon-corretto*",
        ):
            for path in sorted(glob.glob(pattern)):
                consider(path)

    consider(os.environ.get("JAVA_HOME", ""))

    found.sort(key=lambda item: (item[0], item[1]))
    return found


def _configure_driver_jvm() -> str:
    """Set JAVA_HOME for the local PySpark driver (auto Java 11 or 17). Workers use Docker image Java."""
    discovered = _discover_driver_java_homes()
    if discovered:
        home = discovered[0][1]
        os.environ["JAVA_HOME"] = home
        return home

    path_major = _java_major_version("java")
    if path_major is not None and path_major > SPARK_DRIVER_JAVA_MAX_MAJOR:
        raise RuntimeError(
            f"PySpark driver found Java {path_major} on PATH, but Spark 3.5 needs Java 11 or 17 "
            f"for the host driver JVM (not Java {path_major}). "
            "Install Java 11 or 17 (e.g. Amazon Corretto 11 / openjdk@11 on macOS). "
            "Docker Spark workers are unaffected."
        )
    raise RuntimeError(
        "No Java 11 or 17 install found for the PySpark driver. "
        "Install one (e.g. Amazon Corretto 11 or OpenJDK 17); discovery checks "
        "/usr/libexec/java_home on macOS and /usr/lib/jvm on Linux."
    )


def _driver_java_install_message(*, path_java_major: int | None = None) -> str:
    lines = [
        "PySpark driver requires Java 11 or 17 (not Java 21+).",
        "No suitable JDK found. Install one, then restart the notebook kernel:",
        "  macOS: Amazon Corretto 11, or: brew install openjdk@11 / openjdk@17",
        "  Linux: sudo apt install openjdk-11-jdk   # or openjdk-17-jdk",
    ]
    if path_java_major is not None and path_java_major > SPARK_DRIVER_JAVA_MAX_MAJOR:
        lines.append(
            f"\nDefault `java` on PATH is Java {path_java_major} — that version is not supported."
        )
    return "\n".join(lines)


def ensure_driver_java() -> dict:
    """Configure JAVA_HOME for the PySpark driver; return summary. Raises if Java 11/17 missing."""
    discovered = _discover_driver_java_homes()
    available: list[dict[str, str | int]] = []
    for _rank, home in discovered:
        java_bin = os.path.join(home, "bin", "java")
        major = _java_major_version(java_bin)
        if major is not None:
            available.append(
                {"java_home": home, "java_major": major, "java_bin": java_bin}
            )

    if not available:
        raise RuntimeError(
            _driver_java_install_message(path_java_major=_java_major_version("java"))
        )

    home = _configure_driver_jvm()
    java_bin = os.path.join(home, "bin", "java")
    major = _java_major_version(java_bin)
    if major is None:
        raise RuntimeError(f"Could not read Java version from {java_bin!r}")

    return {
        "java_home": home,
        "java_major": major,
        "java_bin": java_bin,
        "available": available,
    }


def check_driver_java() -> dict:
    """Notebook entrypoint: select Java 11/17 for the PySpark driver or raise with install steps."""
    return ensure_driver_java()


def _stop_active_spark_session() -> None:
    """Avoid SPARK-2243 when re-running notebook cells after a failed start."""
    try:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()
    except Exception:
        pass


def _raise_spark_session_error(exc: BaseException) -> None:
    if isinstance(exc, RuntimeError) and (
        "PySpark driver" in str(exc) or "Java 11" in str(exc)
    ):
        raise exc
    master = _spark_master_url()
    low = str(exc).lower()
    parts = [
        f"Failed to start Spark (SPARK_MASTER={master!r}).",
        str(exc).strip() or type(exc).__name__,
    ]
    if "host.docker.internal" in low or "unknownhost" in low:
        parts.append(
            "MinIO S3A must use http://minio:9000 inside Docker (not host.docker.internal on a Mac host JVM). "
            "Run: docker compose run --rm trusted-spark-batch"
        )
    if any(x in low for x in ("connection refused", "could not connect", "refused")):
        parts.append(
            "Start the stack: docker compose up -d (minio, spark-master, spark-worker-*), then "
            "docker compose run --rm trusted-spark-batch"
        )
    if "spark connect" in low or "spark.remote" in low:
        parts.append(
            "Use SparkSession.builder.getOrCreate() for standalone cluster mode (not .create(), which is Spark Connect only)."
        )
    if "s3afilesystem" in low.replace(".", "") or (
        "classnotfoundexception" in low and "s3a" in low
    ):
        ivy = " (Ivy via spark.jars.packages)" if _use_ivy_s3_packages() else ""
        parts.append(
            "Hadoop S3A classes are missing on the Spark classpath"
            f"{ivy}. Docker images bake hadoop-aws into /opt/spark/jars — rebuild cymatics-spark:3.5.2. "
            "On the host, set SPARK_S3_PACKAGES or SPARK_S3_USE_IVY=1, or set SPARK_EVENT_LOG_ENABLED=false "
            "to skip s3a:// event logs temporarily."
        )
    elif "getsubject" in low or (
        "unsupportedoperationexception" in low and "subject" in low
    ):
        parts.append(
            "Host PySpark is using Java 21+ (Hadoop UGI breaks). "
            "Install Java 11 or 17; the driver auto-selects it over newer default Java."
        )
    elif "java_home" in low or "no jvm" in low:
        parts.append(
            "Install Java 11 or 17 for the PySpark driver; it is auto-detected (not Java 21+ on PATH)."
        )
    if "ivy" in low and ("package" in low or "resolve" in low or ".ivy2" in low):
        parts.append(
            "Ivy failed while resolving SPARK_S3_PACKAGES. In Docker, S3A JARs are pre-installed — "
            "do not set SPARK_S3_USE_IVY=1. On the host, ensure ~/.ivy2 is writable or use a rebuilt "
            "Spark image with hadoop-aws in /opt/spark/jars."
        )
    elif "eventlog" in low or (
        "event" in low and "log" in low and "s3a" in low and "classnotfound" not in low
    ):
        parts.append(
            "Spark event logs use s3a://$TRUSTED_ZONE_BUCKET/$SPARK_EVENT_LOG_PREFIX; "
            "set SPARK_EVENT_LOG_ENABLED=false to disable if that path is not writable yet."
        )
    raise RuntimeError(" ".join(parts)) from exc


def _raise_spark_s3a_read_error(exc: BaseException, *, uri: str) -> None:
    low = str(exc).lower()
    endpoint = _http_endpoint_for_s3a()
    parts = [
        f"Spark could not read metadata CSV via S3A ({uri}).",
        str(exc).strip() or type(exc).__name__,
    ]
    parts.append(f"Configured S3A endpoint: {endpoint!r} (override with SPARK_S3A_ENDPOINT).")
    if any(x in low for x in ("accessdenied", "403", "forbidden", "invalidaccesskey")):
        parts.append(
            "Check MINIO_ACCESS_KEY / MINIO_SECRET_KEY or SPARK_S3A_ACCESS_KEY / SPARK_S3A_SECRET_KEY."
        )
    elif any(x in low for x in ("nosuchbucket", "404", "not found")):
        parts.append(f"Ensure bucket {_trusted_zone_bucket()!r} (TRUSTED_ZONE_BUCKET) exists and is reachable from Spark.")
    elif is_unreachable_minio(exc) or "endpoint" in low or "hostname" in low:
        parts.append(
            "Spark JVMs must reach MinIO: from Docker workers use host.docker.internal or the minio service name, "
            "not localhost, unless the driver is also on that network."
        )
    elif any(
        x in low
        for x in (
            "classnotfound",
            "nosuchmethod",
            "s3a",
            "amazonaws",
            "hadoop-aws",
            "illegalargumentexception",
        )
    ):
        parts.append(
            "Set SPARK_S3_PACKAGES in the environment (see env.example: hadoop-aws + aws-java-sdk-bundle) "
            "if Hadoop S3A classes are missing."
        )
    raise RuntimeError(" ".join(parts)) from exc


def _raise_trusted_layout_error(exc: BaseException) -> None:
    bucket = _trusted_zone_bucket()
    if is_unreachable_minio(exc):
        raise RuntimeError(
            f"Cannot reach MinIO at {MINIO_ENDPOINT!r} while preparing trusted bucket {bucket!r} "
            "(Spark event-log prefix). Ensure MinIO is running and credentials are valid."
        ) from exc
    from minio.error import S3Error

    if isinstance(exc, S3Error):
        code = getattr(exc, "code", "") or ""
        raise RuntimeError(
            f"Could not create or access trusted bucket {bucket!r} for Spark layout (MinIO code={code}). "
            "Check TRUSTED_ZONE_BUCKET and MINIO_ACCESS_KEY / MINIO_SECRET_KEY."
        ) from exc
    raise RuntimeError(
        f"Could not prepare trusted bucket {bucket!r} for Spark event logs: {exc}"
    ) from exc


def _trusted_zone_bucket() -> str:
    """Same object-store bucket as trusted-zone outputs."""
    return (
        os.environ.get("TRUSTED_ZONE_BUCKET", "trusted-zone").strip()
        or "trusted-zone"
    )


# Spark event logs under TRUSTED_ZONE_BUCKET (optional).
SPARK_LOGS_PREFIX_DEFAULT = "spark-logs/"

# Pending rows for trusted-zone enrichment (written by Spark batch, read by host Python).
PENDING_WORKSET_KEY_DEFAULT = "metadata/pending_workset.json"

# Hot-path Kafka payloads archived by landing_zone_hot_producer (semi-structured merge in Spark).
KAFKA_EVENTS_HOT_PREFIX_DEFAULT = "metadata/kafka-events/hot-path"
TIME_RECORDED_COL = "time_recorded/added"


def _pending_workset_key() -> str:
    return (
        os.environ.get("SPARK_PENDING_WORKSET_KEY", PENDING_WORKSET_KEY_DEFAULT).strip()
        or PENDING_WORKSET_KEY_DEFAULT
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def write_pending_workset(
    client,
    pending: list[dict],
    *,
    bucket: str | None = None,
    key: str | None = None,
) -> str:
    """Persist pending landing rows for host-side cymatics enrichment."""
    bucket = bucket or _trusted_zone_bucket()
    key = key or _pending_workset_key()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    payload = json.dumps(pending, ensure_ascii=False, default=str).encode("utf-8")
    client.put_object(
        bucket,
        key,
        BytesIO(payload),
        len(payload),
        content_type="application/json",
    )
    return key


def load_pending_workset(
    client,
    *,
    bucket: str | None = None,
    key: str | None = None,
) -> list[dict]:
    """Load pending rows written by the Docker Spark metadata batch."""
    bucket = bucket or _trusted_zone_bucket()
    key = key or _pending_workset_key()
    if not _minio_object_exists(client, bucket, key):
        raise RuntimeError(
            f"Pending workset missing at {bucket!r}/{key!r}. "
            "Run the Spark batch first: docker compose run --rm trusted-spark-batch"
        )
    resp = client.get_object(bucket, key)
    try:
        data = json.loads(resp.read().decode("utf-8"))
    finally:
        resp.close()
        resp.release_conn()
    if not isinstance(data, list):
        raise RuntimeError(
            f"Pending workset at {bucket!r}/{key!r} is not a JSON array."
        )
    return data


def run_spark_trusted_zone_subprocess(
    *,
    project_root: Path | str | None = None,
    compose_service: str | None = None,
) -> None:
    """Run trusted-spark-batch on the compose network (driver + executors in Docker)."""
    root = Path(project_root) if project_root else _repo_root()
    service = (
        compose_service
        or os.environ.get("SPARK_BATCH_COMPOSE_SERVICE", "trusted-spark-batch").strip()
        or "trusted-spark-batch"
    )
    cmd = ["docker", "compose", "run", "--rm", service]
    proc = subprocess.run(
        cmd,
        cwd=str(root),
        capture_output=True,
        text=True,
    )
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Spark metadata batch container failed (exit {proc.returncode}). "
            f"From {root!s}: docker compose up -d && docker compose run --rm {service}"
        )


# Docker Spark cluster (compose network).
SPARK_MASTER_DEFAULT_DOCKER = "spark://spark-master:7077"
SPARK_MASTER_DEFAULT_HOST = "spark://localhost:7077"
SPARK_S3A_ENDPOINT_DEFAULT_DOCKER = "http://minio:9000"
SPARK_DRIVER_JAVA_MAX_MAJOR = 17
# MinIO via S3A (metadata CSV reads + optional event logs).
SPARK_S3_PACKAGES_DEFAULT = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.367"
)


def _running_in_docker() -> bool:
    return os.environ.get("CYMATICS_IN_DOCKER", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ) or Path("/.dockerenv").is_file()


def _ensure_spark_runs_in_docker() -> None:
    if os.environ.get("SPARK_ALLOW_HOST_DRIVER", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return
    if not _running_in_docker():
        raise RuntimeError(
            "Spark metadata batch must run in Docker on the compose network "
            "(driver + executors + MinIO S3A). From the repo root:\n"
            "  docker compose up -d\n"
            "  docker compose run --rm trusted-spark-batch\n"
            "Set SPARK_ALLOW_HOST_DRIVER=1 for Jupyter on the host (with SPARK_S3A_ENDPOINT=http://127.0.0.1:9000)."
        )


def _spark_master_url() -> str:
    """Spark standalone master in Docker — local[*] and other modes are not supported."""
    if _running_in_docker():
        default = SPARK_MASTER_DEFAULT_DOCKER
    elif os.environ.get("SPARK_ALLOW_HOST_DRIVER", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        default = SPARK_MASTER_DEFAULT_HOST
    else:
        default = SPARK_MASTER_DEFAULT_DOCKER
    raw = (os.environ.get("SPARK_MASTER") or default).strip()
    if raw.startswith("local"):
        raise RuntimeError(
            f"SPARK_MASTER={raw!r} is not supported. "
            f"Use the Docker cluster: {SPARK_MASTER_DEFAULT_DOCKER!r} "
            "(docker compose run --rm trusted-spark-batch)."
        )
    if not raw.startswith("spark://"):
        raise RuntimeError(
            f"SPARK_MASTER must be a spark:// URL for the Docker cluster (got {raw!r}). "
            f"Example: {SPARK_MASTER_DEFAULT_DOCKER!r}"
        )
    return raw


def _event_log_prefix() -> str:
    raw = (os.environ.get("SPARK_EVENT_LOG_PREFIX") or SPARK_LOGS_PREFIX_DEFAULT).strip()
    raw = raw.replace("\\", "/")
    if raw and not raw.endswith("/"):
        raw = raw + "/"
    if not raw:
        raw = SPARK_LOGS_PREFIX_DEFAULT
    norm = raw.strip("/")
    if norm != SPARK_LOGS_PREFIX_DEFAULT.strip("/"):
        raise RuntimeError(
            f"SPARK_EVENT_LOG_PREFIX must be {SPARK_LOGS_PREFIX_DEFAULT!r} (got {raw!r})."
        )
    return f"{norm}/"


def _ensure_trusted_spark_layout(client) -> str:
    """Ensure trusted-zone bucket exists with Spark event-log prefix marker."""
    bucket = _trusted_zone_bucket()
    pe = _event_log_prefix().rstrip("/")
    ensure_bucket(client, bucket, [f"{pe}/.keep"])
    return bucket


def _http_endpoint_for_s3a() -> str:
    """Hadoop fs.s3a.endpoint — Docker driver + workers use the MinIO service name."""
    explicit = (os.environ.get("SPARK_S3A_ENDPOINT") or "").strip()
    if explicit:
        endpoint = explicit.rstrip("/")
        if not _running_in_docker() and "host.docker.internal" in endpoint:
            raise RuntimeError(
                f"SPARK_S3A_ENDPOINT={endpoint!r} is for Docker containers only. "
                "Run the batch with: docker compose run --rm trusted-spark-batch "
                f"(uses {SPARK_S3A_ENDPOINT_DEFAULT_DOCKER!r} on the compose network)."
            )
        return endpoint

    if _running_in_docker():
        return SPARK_S3A_ENDPOINT_DEFAULT_DOCKER

    scheme = "https" if MINIO_SECURE else "http"
    ep = MINIO_ENDPOINT.strip()
    if ep.startswith(("http://", "https://")):
        endpoint = ep.rstrip("/")
    else:
        endpoint = f"{scheme}://{ep}"
    if "host.docker.internal" in endpoint:
        return "http://127.0.0.1:9000"
    return endpoint


def _s3a_uri(bucket: str, key: str) -> str:
    key = key.lstrip("/")
    return f"s3a://{bucket}/{key}"


def _kafka_hot_events_prefix() -> str:
    raw = (
        os.environ.get("KAFKA_EVENTS_HOT_PREFIX", KAFKA_EVENTS_HOT_PREFIX_DEFAULT).strip()
        or KAFKA_EVENTS_HOT_PREFIX_DEFAULT
    )
    return raw.strip("/")


def _minio_prefix_has_objects(client, bucket: str, prefix: str) -> bool:
    norm = prefix.strip("/") + "/"
    for obj in client.list_objects(bucket, prefix=norm, recursive=True):
        if obj.object_name and not obj.object_name.endswith("/.keep"):
            return True
    return False


def _kafka_hot_events_schema():
    """Explicit schema so inference on the first file cannot drop device/timestamp."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("audio_path", StringType(), True),
            StructField("bucket", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("sample_rate", IntegerType(), True),
            StructField("duration", IntegerType(), True),
            StructField("device", StringType(), True),
        ]
    )


def _uuid_from_audio_path_col(audio_path_col):
    """Capture id from .../raw/<uuid>.wav (regex uses a single backslash before .wav)."""
    from pyspark.sql import functions as F

    return F.coalesce(
        F.nullif(
            F.regexp_extract(audio_path_col, r"/([^/]+)\.wav$", 1),
            F.lit(""),
        ),
        F.nullif(
            F.regexp_replace(
                F.element_at(F.split(audio_path_col, "/"), -1),
                r"\.wav$",
                "",
            ),
            F.lit(""),
        ),
    )


def _non_empty_string(col):
    from pyspark.sql import functions as F

    return F.nullif(F.trim(col.cast("string")), F.lit(""))


def _read_kafka_hot_archive(spark, uri: str):
    """Read per-capture JSON files archived under metadata/kafka-events/hot-path/."""
    return (
        spark.read.schema(_kafka_hot_events_schema())
        .option("mode", "PERMISSIVE")
        .json(uri)
    )


def _merge_kafka_hot_into_landing(spark, landing, client, landing_bucket: str):
    """Join archived hot-path Kafka JSON (device, timestamp) into landing metadata."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    prefix = _kafka_hot_events_prefix()
    if not _minio_prefix_has_objects(client, landing_bucket, prefix):
        print(
            f"  No Kafka event archive under {landing_bucket}/{prefix}/ "
            "— device/time_recorded unchanged for this batch."
        )
        if "device" not in landing.columns:
            landing = landing.withColumn("device", F.lit("-"))
        return landing

    uri = _s3a_uri(landing_bucket, f"{prefix}/")
    try:
        kafka = _read_kafka_hot_archive(spark, uri)
    except Exception as e:
        print(f"  Warning: could not read Kafka archive at {uri}: {e}")
        if "device" not in landing.columns:
            landing = landing.withColumn("device", F.lit("-"))
        return landing

    if len(kafka.columns) == 0:
        return landing

    print(f"  Kafka archive columns: {sorted(kafka.columns)}")

    kafka = _trim_string_columns(kafka)
    if "audio_path" not in kafka.columns:
        print(f"  Kafka archive at {uri} has no audio_path column — skipping merge.")
        if "device" not in landing.columns:
            landing = landing.withColumn("device", F.lit("-"))
        return landing

    # uuid from audio_path and/or archive object name (<uuid>.json from producer).
    kafka = kafka.withColumn("_archive_file", F.input_file_name())
    kafka = kafka.withColumn(
        "uuid",
        F.coalesce(
            _uuid_from_audio_path_col(F.col("audio_path")),
            F.nullif(
                F.regexp_extract(F.col("_archive_file"), r"/([^/]+)\.json$", 1),
                F.lit(""),
            ),
        ),
    )
    before_filter = kafka.count()
    kafka = kafka.filter(F.length(F.col("uuid")) > 0)
    after_filter = kafka.count()
    if after_filter < before_filter:
        print(
            f"  Kafka archive: dropped {before_filter - after_filter} row(s) "
            "with unparseable uuid (check audio_path / archive filename)."
        )
    if after_filter == 0:
        print("  Kafka archive: no rows with a valid uuid — skipping merge.")
        if "device" not in landing.columns:
            landing = landing.withColumn("device", F.lit("-"))
        return landing

    device_col = (
        _non_empty_string(F.col("device"))
        if "device" in kafka.columns
        else F.lit(None).cast("string")
    )
    time_col = (
        _non_empty_string(F.col("timestamp"))
        if "timestamp" in kafka.columns
        else F.lit(None).cast("string")
    )
    kafka_sel = kafka.select(
        F.col("uuid"),
        device_col.alias("_kafka_device"),
        time_col.alias("_kafka_time"),
    )
    window = Window.partitionBy("uuid").orderBy(F.desc_nulls_last(F.col("_kafka_time")))
    kafka_sel = (
        kafka_sel.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("_kafka_hit", F.lit(True))
    )

    event_count = kafka_sel.count()
    if "device" not in landing.columns:
        landing = landing.withColumn("device", F.lit(None).cast("string"))
    if TIME_RECORDED_COL not in landing.columns:
        landing = landing.withColumn(TIME_RECORDED_COL, F.lit(None).cast("string"))

    merged = landing.join(kafka_sel, on="uuid", how="left")
    kafka_matched = merged.filter(F.col("_kafka_hit").isNotNull()).count()
    merged = (
        merged.withColumn(
            "device",
            F.coalesce(
                _non_empty_string(F.col("_kafka_device")),
                _non_empty_string(F.col("device")),
                F.lit("-"),
            ),
        )
        .withColumn(
            TIME_RECORDED_COL,
            F.coalesce(
                _non_empty_string(F.col("_kafka_time")),
                _non_empty_string(F.col(TIME_RECORDED_COL)),
                F.lit(""),
            ),
        )
        .drop("_kafka_device", "_kafka_time", "_kafka_hit")
    )

    hot_pending = 0
    if "source" in merged.columns:
        hot_pending = merged.filter(
            F.lower(F.trim(F.col("source"))) == F.lit("hot-path")
        ).count()
    print(
        f"  Merged Kafka device + {TIME_RECORDED_COL!r}: "
        f"{event_count} archived event(s), {kafka_matched} landing row(s) matched by uuid."
    )
    if hot_pending and kafka_matched < hot_pending:
        print(
            f"  Warning: {hot_pending - kafka_matched} hot-path row(s) had no Kafka archive match "
            "(uuid mismatch or missing metadata/kafka-events/hot-path/<uuid>.json)."
        )
    return merged


def _read_metadata_csv(spark, uri: str):
    """Read a metadata CSV from MinIO via S3A."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", "\"")
        .csv(uri)
    )


def _event_log_s3a_dir() -> str:
    """spark.eventLog.dir — derived only from trusted bucket + SPARK_EVENT_LOG_PREFIX."""
    bucket = _trusted_zone_bucket()
    pfx = _event_log_prefix().rstrip("/").strip() or "spark-logs"
    return f"s3a://{bucket}/{pfx}/"


def _resolve_event_log_dir() -> str | None:
    flag = os.environ.get("SPARK_EVENT_LOG_ENABLED", "").strip().lower()
    if flag not in ("1", "true", "yes"):
        return None
    return _event_log_s3a_dir()


def _resolve_s3_packages() -> str:
    """Ivy coordinates for hadoop-aws + AWS SDK (host driver without baked JARs)."""
    return (os.environ.get("SPARK_S3_PACKAGES") or "").strip() or SPARK_S3_PACKAGES_DEFAULT


def _spark_jars_dir() -> Path:
    return Path(os.environ.get("SPARK_HOME", "/opt/spark")) / "jars"


def _s3_jars_baked_in_image() -> bool:
    """hadoop-aws + aws-java-sdk-bundle copied into the Spark image (Docker/spark_Dockerfile)."""
    jar_dir = _spark_jars_dir()
    if not jar_dir.is_dir():
        return False
    return bool(list(jar_dir.glob("hadoop-aws-*.jar"))) and bool(
        list(jar_dir.glob("aws-java-sdk-bundle-*.jar"))
    )


def _use_ivy_s3_packages() -> bool:
    """Only resolve S3A JARs via Ivy when they are not already on the Spark classpath."""
    flag = os.environ.get("SPARK_S3_USE_IVY", "").strip().lower()
    if flag in ("0", "false", "no"):
        return False
    if flag in ("1", "true", "yes"):
        return True
    if _running_in_docker():
        return False
    if _s3_jars_baked_in_image():
        return False
    return True


def _apply_driver_network_config(builder):
    """Executors must reach the driver; host vs Docker use different advertised hosts."""
    if _running_in_docker():
        # docker compose run forwards host shell env by default — do not use host.docker.internal.
        os.environ.pop("SPARK_DRIVER_HOST", None)
        hostname = socket.gethostname()
        return (
            builder.config("spark.driver.host", hostname)
            .config("spark.driver.bindAddress", "0.0.0.0")
        )

    driver_host = (os.environ.get("SPARK_DRIVER_HOST") or "").strip()
    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)
    bind = (os.environ.get("SPARK_DRIVER_BIND_ADDRESS") or "").strip()
    if bind:
        builder = builder.config("spark.driver.bindAddress", bind)
    return builder


def _apply_s3a_to_builder(builder):
    """Wire Spark to MinIO via Hadoop s3a (path-style bucket)."""
    endpoint = _http_endpoint_for_s3a()
    access = (os.environ.get("SPARK_S3A_ACCESS_KEY") or MINIO_ACCESS_KEY).strip()
    secret = (os.environ.get("SPARK_S3A_SECRET_KEY") or MINIO_SECRET_KEY).strip()

    if _use_ivy_s3_packages():
        builder = builder.config("spark.jars.packages", _resolve_s3_packages())

    return (
        builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(MINIO_SECURE).lower(),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
    )


def _build_spark_session():
    """Connect to the Docker Spark standalone cluster (SPARK_MASTER=spark://…:7077)."""
    from pyspark.sql import SparkSession

    _configure_driver_jvm()
    _stop_active_spark_session()

    master = _spark_master_url()
    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    app_name = os.environ.get("SPARK_APP_NAME", "trusted-zone-processing")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_mem)
        .config("spark.ui.showConsoleProgress", "false")
        .config(
            "spark.sql.shuffle.partitions",
            os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "8"),
        )
    )
    builder = _apply_s3a_to_builder(builder)
    builder = _apply_driver_network_config(builder)

    blockmgr = (os.environ.get("SPARK_DRIVER_BLOCKMANAGER_PORT") or "").strip()
    if blockmgr.isdigit():
        builder = builder.config("spark.driver.blockManager.port", blockmgr)

    log_uri = _resolve_event_log_dir()
    if log_uri:
        builder = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", log_uri)
            .config("spark.eventLog.compress", "true")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def _trim_string_columns(df):
    from pyspark.sql import functions as F

    casts = []
    for c in df.columns:
        casts.append(F.trim(F.col(c).cast("string")).alias(c))
    return df.select(*casts)


def pending_landing_records_spark(
    client,
    *,
    landing_bucket: str,
    landing_meta_key: str,
    trusted_bucket: str,
    trusted_meta_key: str,
):
    """Landing rows eligible for Trusted enrichment after Spark QA on metadata.

    Adds distributed deduplication / validation before the cymatics workload.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    _ensure_spark_runs_in_docker()

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise RuntimeError(
            "MinIO credentials are required for Spark S3A access."
        )
    if not str(MINIO_ENDPOINT).strip():
        raise RuntimeError(
            "MINIO_ENDPOINT is empty. Set it to your MinIO host and port (e.g. localhost:9000)."
        )

    if not _minio_object_exists(client, landing_bucket, landing_meta_key):
        return []

    try:
        _ensure_trusted_spark_layout(client)
    except Exception as e:
        _raise_trusted_layout_error(e)

    landing_uri = _s3a_uri(landing_bucket, landing_meta_key)
    trusted_uri = None
    if _minio_object_exists(client, trusted_bucket, trusted_meta_key):
        trusted_uri = _s3a_uri(trusted_bucket, trusted_meta_key)

    spark = None
    try:
        try:
            spark = _build_spark_session()
        except Exception as e:
            _raise_spark_session_error(e)

        try:
            landing = _read_metadata_csv(spark, landing_uri)
        except Exception as e:
            _raise_spark_s3a_read_error(e, uri=landing_uri)

        if len(landing.columns) == 0:
            return []
        if "uuid" not in landing.columns or "audio_path" not in landing.columns:
            return []

        landing = _trim_string_columns(landing)

        uuid_len = (
            F.length(F.col("uuid")) if "uuid" in landing.columns else F.lit(0)
        )
        path_len = (
            F.length(F.col("audio_path"))
            if "audio_path" in landing.columns
            else F.lit(0)
        )
        landing = landing.filter((uuid_len > 0) & (path_len > 0))

        order_exprs = []
        for col_name in ("time_recorded/added", "source_id"):
            if col_name in landing.columns:
                order_exprs.append(F.desc_nulls_last(F.col(col_name)))
        if not order_exprs:
            order_exprs.append(F.monotonically_increasing_id())

        window = Window.partitionBy("uuid").orderBy(*order_exprs)
        landing = (
            landing.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        landing = _merge_kafka_hot_into_landing(spark, landing, client, landing_bucket)

        if trusted_uri:
            try:
                trusted_uuids = _read_metadata_csv(spark, trusted_uri)
            except Exception as e:
                _raise_spark_s3a_read_error(e, uri=trusted_uri)
            if "uuid" in trusted_uuids.columns:
                trusted_uuids = _trim_string_columns(trusted_uuids.select("uuid"))
                trusted_uuids = trusted_uuids.filter(F.length(F.col("uuid")) > 0).distinct()
                pending = landing.join(trusted_uuids, on="uuid", how="left_anti")
            else:
                pending = landing
        else:
            pending = landing

        try:
            rows = [row.asDict() for row in pending.collect()]
            print(f"  Spark metadata QA complete ({len(rows)} pending rows).")
            return rows
        except Exception as e:
            master = _spark_master_url()
            low = str(e).lower()
            extra = ""
            if is_unreachable_minio(e) or "fetch failed" in low or "executor" in low:
                extra = (
                    " Executors must reach the driver and MinIO; check SPARK_DRIVER_HOST / firewall / "
                    "SPARK_S3A_ENDPOINT for your runtime (host vs Docker network)."
                )
            raise RuntimeError(
                f"Spark failed while running the metadata batch or collecting results (master={master!r}). {e}{extra}"
            ) from e
    finally:
        if spark:
            # Driver calls stop() → master sends SIGTERM to executors → worker logs show
            # "KILLED exitStatus 143". That is expected cleanup, not a failed job.
            print(
                "  Stopping Spark session (worker logs may show executor KILLED/143 — normal)."
            )
            spark.stop()


def run_spark_trusted_zone(client) -> list[dict]:
    """CLI / notebook — Spark QA, then write pending workset to MinIO."""
    from shared.minio_helpers import LANDING_ZONE_BUCKET, METADATA_KEY

    trusted_bucket = _trusted_zone_bucket()
    pending = pending_landing_records_spark(
        client,
        landing_bucket=LANDING_ZONE_BUCKET,
        landing_meta_key=METADATA_KEY,
        trusted_bucket=trusted_bucket,
        trusted_meta_key=METADATA_KEY,
    )
    key = write_pending_workset(client, pending, bucket=trusted_bucket)
    print(f"Wrote pending workset ({len(pending)} rows) → {trusted_bucket}/{key}")
    return pending


def _print_pending_summary(pending: list[dict]) -> None:
    print(f"Pending records: {len(pending)}")
    if pending:
        print("Example keys:", sorted(pending[0].keys())[:12], "…")
        row0 = pending[0]
        for key in ("uuid", "source", "audio_path"):
            if key in row0:
                val = str(row0[key])
                print(f"  {key}:", val[:120] + ("…" if len(val) > 120 else ""))


def main() -> int:
    """CLI entrypoint for docker compose run trusted-spark-batch."""
    from shared.minio_helpers import create_minio_client

    client = create_minio_client()
    pending = run_spark_trusted_zone(client)
    _print_pending_summary(pending)
    print("Spark metadata batch finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
