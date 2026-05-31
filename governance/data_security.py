#!/usr/bin/env python3
"""
Data Governance — MinIO bucket policies & role-based access control.

Creates four IAM users with role-based permissions across pipeline zones:

  ┌──────────────────┬───────────┬───────────┬──────────────┬─────────────┐
  │ Role             │ Landing   │ Trusted   │ Exploitation │ Governance  │
  ├──────────────────┼───────────┼───────────┼──────────────┼─────────────┤
  │ pipeline_admin   │ RW        │ RW        │ RW           │ RW          │
  │ data_engineer    │ RW        │ RW        │ R            │ R           │
  │ data_scientist   │ —         │ R         │ RW           │ R           │
  │ analyst          │ R         │ R         │ R            │ R           │
  └──────────────────┴───────────┴───────────┴──────────────┴─────────────┘

  R = read-only, RW = read-write, — = no access.

Implementation uses ``mc admin`` (MinIO Client) via Docker to manage users
and attach IAM policies, since the Python SDK only supports bucket-level
policies (not user management).

Orchestrator: option 11 → Data governance → Data security.

Run directly:
    python governance/data_security.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from io import BytesIO
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_PROJECT_ROOT / ".env")
except ImportError:
    pass

from shared.minio_helpers import create_minio_client

# ── Constants ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_SECRET_KEY", "password")

LANDING_BUCKET = os.environ.get("LANDING_ZONE_BUCKET", "landing-zone")
TRUSTED_BUCKET = os.environ.get("TRUSTED_ZONE_BUCKET", "trusted-zone")
EXPLOITATION_BUCKET = os.environ.get("EXPLOITATION_ZONE_BUCKET", "exploitation-zone")

SECURITY_REPORT_KEY = "governance/security_report.json"

# ── Role definitions ─────────────────────────────────────────────────────

ROLES = {
    "pipeline_admin": {
        "password": "admin-cymatics-2026",
        "description": "Full access to all zones (pipeline admin & testing)",
        "buckets": {
            LANDING_BUCKET: "readwrite",
            TRUSTED_BUCKET: "readwrite",
            EXPLOITATION_BUCKET: "readwrite",
        },
    },
    "data_engineer": {
        "password": "engineer-cymatics-2026",
        "description": "Manages ingestion and trusted-zone processing",
        "buckets": {
            LANDING_BUCKET: "readwrite",
            TRUSTED_BUCKET: "readwrite",
            EXPLOITATION_BUCKET: "readonly",
        },
    },
    "data_scientist": {
        "password": "scientist-cymatics-2026",
        "description": "Builds models and embeddings from exploitation data",
        "buckets": {
            TRUSTED_BUCKET: "readonly",
            EXPLOITATION_BUCKET: "readwrite",
        },
    },
    "analyst": {
        "password": "analyst-cymatics-2026",
        "description": "Consumes dashboards and KPIs (read-only across all zones)",
        "buckets": {
            LANDING_BUCKET: "readonly",
            TRUSTED_BUCKET: "readonly",
            EXPLOITATION_BUCKET: "readonly",
        },
    },
}


# ── Policy builders ──────────────────────────────────────────────────────


def _build_policy(role_name: str, bucket_permissions: dict[str, str]) -> dict:
    """Build an IAM policy document for a role.

    Each bucket gets either read-only or read-write statements.
    """
    statements = []

    for bucket, access in bucket_permissions.items():
        if access == "readonly":
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket}",
                        f"arn:aws:s3:::{bucket}/*",
                    ],
                }
            )
        elif access == "readwrite":
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket}",
                        f"arn:aws:s3:::{bucket}/*",
                    ],
                }
            )

    return {
        "Version": "2012-10-17",
        "Statement": statements,
    }


# ── mc admin helpers ─────────────────────────────────────────────────────


MINIO_CONTAINER = os.environ.get("MINIO_CONTAINER", "cymatics-minio")


def _run_mc(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run an ``mc`` command via ``docker exec`` inside the MinIO container."""
    cmd = [
        "docker", "exec", MINIO_CONTAINER,
        "mc",
    ] + args
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        check=check,
    )


def _mc_alias_set() -> None:
    """Register the local MinIO instance as an mc alias (inside the container)."""
    _run_mc([
        "alias", "set", "local",
        "http://localhost:9000",
        MINIO_ROOT_USER,
        MINIO_ROOT_PASSWORD,
    ])


def _mc_user_exists(username: str) -> bool:
    """Check if an IAM user already exists."""
    result = _run_mc(["admin", "user", "list", "local"], check=False)
    return username in result.stdout


def _mc_create_user(username: str, password: str) -> None:
    """Create an IAM user (idempotent — skips if exists)."""
    _run_mc(["admin", "user", "add", "local", username, password], check=False)


def _mc_create_policy(policy_name: str, policy_doc: dict) -> None:
    """Create a named IAM policy from a policy document."""
    policy_json = json.dumps(policy_doc)
    cmd = [
        "docker", "exec", "-i", MINIO_CONTAINER,
        "mc", "admin", "policy", "create", "local", policy_name, "/dev/stdin",
    ]
    subprocess.run(
        cmd,
        input=policy_json,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def _mc_attach_policy(policy_name: str, username: str) -> None:
    """Attach a named policy to a user."""
    _run_mc([
        "admin", "policy", "attach", "local", policy_name,
        "--user", username,
    ], check=False)


def _mc_user_info(username: str) -> str:
    """Get user info including attached policies."""
    result = _run_mc(["admin", "user", "info", "local", username], check=False)
    return result.stdout


# ── Apply policies ───────────────────────────────────────────────────────


def apply_security_policies() -> list[dict]:
    """Create users, build policies, and attach them.

    Returns a list of result dicts for display and reporting.
    """
    print("  Registering MinIO alias...")
    _mc_alias_set()

    results: list[dict] = []

    for role_name, role_config in ROLES.items():
        password = role_config["password"]
        description = role_config["description"]
        bucket_permissions = role_config["buckets"]
        policy_name = f"cymatics-{role_name}"

        print(f"\n  ── Role: {role_name} ──")
        print(f"     {description}")

        # Create user.
        existed = _mc_user_exists(role_name)
        _mc_create_user(role_name, password)
        status = "exists" if existed else "created"
        print(f"     User '{role_name}': {status}")

        # Build and create policy.
        policy_doc = _build_policy(role_name, bucket_permissions)
        _mc_create_policy(policy_name, policy_doc)
        print(f"     Policy '{policy_name}': created")

        # Attach policy to user.
        _mc_attach_policy(policy_name, role_name)
        print(f"     Policy attached to user")

        # Show access matrix.
        for bucket, access in bucket_permissions.items():
            icon = "RW" if access == "readwrite" else "R "
            print(f"       [{icon}]  {bucket}")

        results.append(
            {
                "role": role_name,
                "user_status": status,
                "policy": policy_name,
                "description": description,
                "permissions": bucket_permissions,
            }
        )

    return results


# ── Verify policies ──────────────────────────────────────────────────────


def verify_access(role_name: str, role_config: dict) -> list[dict]:
    """Test actual access for a role by attempting read/write operations."""
    from minio import Minio
    from minio.error import S3Error

    client = Minio(
        MINIO_ENDPOINT,
        access_key=role_name,
        secret_key=role_config["password"],
        secure=False,
    )

    all_buckets = [LANDING_BUCKET, TRUSTED_BUCKET, EXPLOITATION_BUCKET]
    checks: list[dict] = []

    for bucket in all_buckets:
        expected = role_config["buckets"].get(bucket)

        # Test read (list objects).
        can_read = False
        try:
            objs = client.list_objects(bucket, prefix="", recursive=False)
            # Consume at most one result to confirm access.
            for _ in objs:
                break
            can_read = True
        except S3Error:
            pass

        # Test write (put + delete a tiny test object).
        can_write = False
        test_key = f"governance/.access_test_{role_name}"
        try:
            data = b"access_test"
            client.put_object(bucket, test_key, BytesIO(data), len(data))
            client.remove_object(bucket, test_key)
            can_write = True
        except S3Error:
            pass

        # Determine actual vs expected.
        if expected == "readwrite":
            read_ok = can_read
            write_ok = can_write
        elif expected == "readonly":
            read_ok = can_read
            write_ok = not can_write  # Should NOT be able to write.
        else:
            # No access expected.
            read_ok = not can_read
            write_ok = not can_write

        checks.append(
            {
                "bucket": bucket,
                "expected": expected or "none",
                "can_read": can_read,
                "can_write": can_write,
                "read_correct": read_ok,
                "write_correct": write_ok,
                "passed": read_ok and write_ok,
            }
        )

    return checks


# ── Save report ──────────────────────────────────────────────────────────


def save_security_report(minio_client, results: list[dict], verification: dict) -> str:
    """Save security policy report to exploitation-zone governance folder."""
    from datetime import datetime, timezone

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "roles": results,
        "verification": verification,
    }

    payload = json.dumps(report, indent=2, default=str).encode("utf-8")
    minio_client.put_object(
        EXPLOITATION_BUCKET,
        SECURITY_REPORT_KEY,
        BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    path = f"{EXPLOITATION_BUCKET}/{SECURITY_REPORT_KEY}"
    print(f"\n  Security report saved: {path} ({len(payload) / 1024:.1f} KB)")
    return path


# ── Display ──────────────────────────────────────────────────────────────


def display_access_matrix(results: list[dict]) -> None:
    """Print the access control matrix."""
    width = 62
    print(f"\n{'═' * width}")
    print("  Access Control Matrix")
    print(f"{'─' * width}")

    header = f"  {'Role':<18} {'Landing':<12} {'Trusted':<12} {'Exploitation':<12}"
    print(header)
    print(f"  {'─' * 54}")

    all_buckets = [LANDING_BUCKET, TRUSTED_BUCKET, EXPLOITATION_BUCKET]

    for r in results:
        perms = r["permissions"]
        cols = []
        for b in all_buckets:
            access = perms.get(b, "—")
            if access == "readwrite":
                cols.append("RW")
            elif access == "readonly":
                cols.append("R")
            else:
                cols.append("—")
        print(f"  {r['role']:<18} {cols[0]:<12} {cols[1]:<12} {cols[2]:<12}")

    print(f"{'═' * width}\n")


def display_verification(verification: dict) -> None:
    """Print the access verification results."""
    width = 62
    print(f"{'─' * width}")
    print("  Access Verification")
    print(f"{'─' * width}")

    all_passed = True
    for role_name, checks in verification.items():
        role_ok = all(c["passed"] for c in checks)
        icon = "✓" if role_ok else "✗"
        print(f"\n  {icon} {role_name}")

        for c in checks:
            bucket = c["bucket"]
            expected = c["expected"]
            status = "PASS" if c["passed"] else "FAIL"

            actual_parts = []
            if c["can_read"]:
                actual_parts.append("R")
            if c["can_write"]:
                actual_parts.append("W")
            actual = "".join(actual_parts) or "—"

            exp_label = {"readwrite": "RW", "readonly": "R", "none": "—"}.get(
                expected, expected,
            )

            check_icon = "✓" if c["passed"] else "✗"
            print(
                f"    {check_icon} {bucket:<22} "
                f"expected={exp_label:<4} actual={actual:<4} [{status}]"
            )

            if not c["passed"]:
                all_passed = False

    print(f"\n{'─' * width}")
    if all_passed:
        print("  ALL ACCESS CHECKS PASSED")
    else:
        print("  SOME ACCESS CHECKS FAILED")
    print(f"{'═' * width}\n")


# ── Interactive CLI ──────────────────────────────────────────────────────


def _print_menu() -> None:
    width = 62
    print(f"\n{'─' * width}")
    print("  Data security options:")
    print(f"{'─' * width}")
    print("   [1]  Apply security policies (create users & policies)")
    print("   [2]  Verify access controls (test each role)")
    print("   [3]  Show access control matrix")
    print("   [b]  Back")
    print()


def run_interactive(*, from_orchestrator: bool = False) -> None:
    """Main loop: apply policies → verify → display → repeat."""
    width = 62
    print(f"\n{'─' * width}")
    print("  Data Governance — Data Security (MinIO Policies)")
    print(f"{'─' * width}")
    print("  Role-based access control for pipeline zones:")
    print("    pipeline_admin → full access (all zones)")
    print("    data_engineer  → RW landing + trusted, R exploitation")
    print("    data_scientist → R trusted, RW exploitation")
    print("    analyst        → R all zones (read-only)")
    print(f"{'─' * width}")
    print()
    print("  Requirements:")
    print("    - MinIO running  (docker compose up -d minio)")
    print("    - Docker available (for mc admin commands)")
    print()

    results: list[dict] | None = None
    verification: dict | None = None

    while True:
        _print_menu()
        try:
            choice = input("  Select option [1-3, b]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Leaving data security.")
            break

        if choice in ("b", "q", "quit", "exit", ""):
            break

        if choice == "1":
            try:
                results = apply_security_policies()
            except Exception as e:
                print(f"\n  Failed to apply policies: {e}")
                continue

            display_access_matrix(results)

            try:
                minio_client = create_minio_client()
                save_security_report(minio_client, results, {})
            except Exception as e:
                print(f"  Failed to save report: {e}")

        elif choice == "2":
            if results is None:
                print("\n  Applying policies first...")
                try:
                    results = apply_security_policies()
                except Exception as e:
                    print(f"\n  Failed to apply policies: {e}")
                    continue

            print("\n  Verifying access controls...")
            verification = {}
            for role_name, role_config in ROLES.items():
                checks = verify_access(role_name, role_config)
                verification[role_name] = checks

            display_verification(verification)

            try:
                minio_client = create_minio_client()
                save_security_report(minio_client, results, verification)
            except Exception as e:
                print(f"  Failed to save report: {e}")

        elif choice == "3":
            if results is None:
                print("\n  No policies applied yet — showing role definitions.")
                results = [
                    {
                        "role": name,
                        "permissions": cfg["buckets"],
                        "description": cfg["description"],
                    }
                    for name, cfg in ROLES.items()
                ]
            display_access_matrix(results)

        else:
            print("  Invalid choice.")


def main() -> None:
    run_interactive(from_orchestrator=False)


if __name__ == "__main__":
    main()
