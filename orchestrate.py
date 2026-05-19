#!/usr/bin/env python3
"""
Manual Orchestrator — BDM Cymatics Pipeline.

Interactive CLI that lets you choose which flow to run:

  1. Warm path          (microphone → approve → store audio )
  2. Hot path           (producer + consumer in parallel)
  3. Cold path: Freesound  (batch Freesound ingestion)
  4. Cold path: ESC-50     (batch ESC-50 ingestion)
  5. Trusted zone       → trusted_zone/trusted_zone_processing.py
  6. Exploitation zone  → exploitation_zone/exploitation_zone_processing.py
  7. Sync Delta Lake    → shared/sync_delta.py (all zones)
  8. SonarQube analysis (code quality scan + results)
  9. Data consumption   (KPI queries on exploitation Delta)

While a flow is running, live CPU / RAM / disk metrics are displayed
every 2 seconds so you can monitor resource usage in real time.
"""

import os
import sys
import time
import signal
import subprocess
import threading
import tempfile
import json
import urllib.request
import urllib.error
import shutil

try:
    import psutil
except ImportError:
    print("psutil is required for resource monitoring.")
    print("Install it with:  pip install psutil")
    sys.exit(1)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PAUSE_MONITOR_FILE = os.path.join(tempfile.gettempdir(), ".cymatics_pause_monitor")

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
except ImportError:
    pass

FLOWS = {
    "1": {
        "name": "Warm path",
        "scripts": [
            os.path.join(PROJECT_ROOT, "landing_zone", "warm_path", "landing_zone_warm.py"),
        ],
    },
    "2": {
        "name": "Hot path (producer + consumer)",
        "scripts": [
            os.path.join(PROJECT_ROOT, "landing_zone", "hot_path", "landing_zone_hot_producer.py"),
            os.path.join(PROJECT_ROOT, "landing_zone", "hot_path", "landing_zone_hot_consumer.py"),
        ],
        "parallel": True,
    },
    "3": {
        "name": "Cold path — Freesound",
        "scripts": [
            os.path.join(PROJECT_ROOT, "landing_zone", "cold_path", "cold_freesound.py"),
        ],
    },
    "4": {
        "name": "Cold path — ESC-50",
        "scripts": [
            os.path.join(PROJECT_ROOT, "landing_zone", "cold_path", "cold_esc50.py"),
        ],
    },
    "5": {
        "name": "Trusted zone processing",
        "scripts": [
            os.path.join(PROJECT_ROOT, "trusted_zone", "trusted_zone_processing.py"),
        ],
    },
    "6": {
        "name": "Exploitation zone processing",
        "scripts": [
            os.path.join(
                PROJECT_ROOT,
                "exploitation_zone",
                "exploitation_zone_processing.py",
            ),
        ],
    },
    "7": {
        "name": "Sync all zones → Delta Lake",
        "scripts": [
            os.path.join(PROJECT_ROOT, "shared", "sync_delta.py"),
        ],
    },
}

# ── resource monitor ────────────────────────────────────────────────────────

_stop_monitor = threading.Event()


def _format_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _monitor_resources(pids, interval=2.0):
    """Print CPU / RAM / disk metrics for *pids* every *interval* seconds."""
    while not _stop_monitor.is_set():
        if os.path.exists(PAUSE_MONITOR_FILE):
            _stop_monitor.wait(interval)
            continue

        lines = []

        cpu_pct = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("/")

        lines.append(
            f"  System  │  CPU: {cpu_pct:5.1f}%  │  "
            f"RAM: {_format_bytes(mem.used)}/{_format_bytes(mem.total)} ({mem.percent:.1f}%)  │  "
            f"Disk: {_format_bytes(disk.used)}/{_format_bytes(disk.total)} ({disk.percent:.1f}%)"
        )

        for pid in list(pids):
            try:
                p = psutil.Process(pid)
                with p.oneshot():
                    proc_cpu = p.cpu_percent(interval=0)
                    proc_mem = p.memory_info()
                    proc_name = " ".join(p.cmdline()[-1:]) or p.name()
                    proc_name = os.path.basename(proc_name)
                lines.append(
                    f"  PID {pid:<6}│  CPU: {proc_cpu:5.1f}%  │  "
                    f"RSS: {_format_bytes(proc_mem.rss)}  │  {proc_name}"
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        ts = time.strftime("%H:%M:%S")
        print(f"\n  ┌─ metrics @ {ts} " + "─" * 48)
        for line in lines:
            print(f"  │{line}")
        print("  └" + "─" * 62)

        _stop_monitor.wait(interval)


# ── subprocess helpers ──────────────────────────────────────────────────────

_ALLOWED_SCRIPTS = frozenset(
    script
    for flow in FLOWS.values()
    for script in flow["scripts"]
)


def _run_script(script_path, extra_args=None):
    """Launch a Python script as a subprocess, return the Popen object."""
    resolved = os.path.abspath(script_path)
    if resolved not in _ALLOWED_SCRIPTS:
        raise ValueError(f"Refusing to run untrusted script: {script_path}")
    safe_args = [str(a) for a in (extra_args or [])]
    cmd = [sys.executable, resolved] + safe_args
    return subprocess.Popen(
        cmd,
        cwd=PROJECT_ROOT,
        stdin=sys.stdin,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )


def _wait_all(procs, parallel=False):
    """Wait for all processes; return list of (script, returncode).

    In parallel mode, once any process exits the remaining ones are
    terminated so the orchestrator can return to the menu.
    """
    if parallel and len(procs) > 1:
        while True:
            for script, proc in procs:
                ret = proc.poll()
                if ret is not None:
                    first_done = script
                    break
            else:
                time.sleep(0.3)
                continue
            break

        print(f"\n  {first_done} exited — stopping remaining processes...")
        for script, proc in procs:
            if proc.poll() is None:
                try:
                    proc.send_signal(signal.SIGTERM)
                except OSError:
                    pass
        for _, proc in procs:
            proc.wait()
        return [(script, proc.returncode) for script, proc in procs]

    results = []
    for script, proc in procs:
        proc.wait()
        results.append((script, proc.returncode))
    return results


# ── SonarQube analysis ─────────────────────────────────────────────────────

SONAR_URL = "http://localhost:9090"
SONAR_PROJECT_KEY = "bdm-cymatics"


def _sonar_credentials():
    """Return (mode, value) for API/scanner auth: ('token', str) or ('basic', (user, pass)) or (None, None)."""
    token = (os.environ.get("SONAR_TOKEN") or "").strip()
    if token:
        return "token", token
    user = (os.environ.get("SONAR_USERNAME") or os.environ.get("SONAR_LOGIN") or "").strip()
    password = (os.environ.get("SONAR_PASSWORD") or "").strip()
    if user and password:
        return "basic", (user, password)
    return None, None


def _sonar_auth_header():
    import base64
    mode, val = _sonar_credentials()
    if mode == "token":
        cred = base64.b64encode(f"{val}:".encode()).decode()
        return f"Basic {cred}"
    if mode == "basic":
        u, p = val
        cred = base64.b64encode(f"{u}:{p}".encode()).decode()
        return f"Basic {cred}"
    return None


def _sonar_api(path):
    """GET a SonarQube API endpoint, return parsed JSON."""
    url = f"{SONAR_URL}{path}"
    req = urllib.request.Request(url)
    auth = _sonar_auth_header()
    if auth:
        req.add_header("Authorization", auth)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def _check_sonar_ready():
    """Return True if SonarQube server is up and ready."""
    try:
        data = _sonar_api("/api/system/status")
        return data.get("status") == "UP"
    except Exception:
        return False


def _find_sonar_scanner():
    """Locate sonar-scanner binary. Return path or None."""
    scanner = shutil.which("sonar-scanner")
    if scanner:
        return scanner
    local = os.path.join(PROJECT_ROOT, "sonar-scanner", "bin", "sonar-scanner")
    if os.path.isfile(local):
        return local
    return None


def run_sonarqube():
    """Run SonarQube analysis and display results."""
    print(f"\n{'─' * 62}")
    print("  SonarQube Code Quality Analysis")
    print(f"{'─' * 62}\n")

    print("  Checking SonarQube server...")
    if not _check_sonar_ready():
        print("  SonarQube is not running or not ready.")
        print("  Start it with:  docker compose up -d sonarqube")
        print("  First startup takes ~90 seconds. UI: http://localhost:9090")
        print(f"{'─' * 62}\n")
        return

    print("  SonarQube server is UP.\n")

    scanner = _find_sonar_scanner()
    if not scanner:
        print("  sonar-scanner not found on PATH.")
        print("  Install it:  brew install sonar-scanner")
        print("         or:   https://docs.sonarqube.org/latest/analyzing-source-code/scanners/sonarscanner/")
        print(f"\n{'─' * 62}\n")
        return

    print(f"  Scanner: {scanner}")
    mode, val = _sonar_credentials()
    if mode == "token":
        print("  Auth: user token (from SONAR_TOKEN)")
    elif mode == "basic":
        u, _pw = val
        print(f"  Auth: password login (SONAR_USERNAME={u!r})")
    else:
        print("  Auth: none — set SONAR_TOKEN or SONAR_USERNAME + SONAR_PASSWORD in .env")
    print("  Running analysis...\n")

    cmd = [scanner]
    if mode == "token":
        cmd.append(f"-Dsonar.token={val}")
    elif mode == "basic":
        u, p = val
        cmd.append(f"-Dsonar.login={u}")
        cmd.append(f"-Dsonar.password={p}")
    else:
        print("  Hint: set SONAR_TOKEN or SONAR_USERNAME + SONAR_PASSWORD in .env")
        print("        if analysis fails with “not authorized”.\n")

    proc = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if any(k in stripped.upper() for k in ["ERROR", "WARN", "INFO"]):
            print(f"    {stripped}")

    if proc.returncode != 0:
        print(f"\n  Scanner exited with code {proc.returncode}.")
        print(f"{'─' * 62}\n")
        return

    print("\n  Waiting for analysis report to be processed...")
    for _ in range(30):
        time.sleep(2)
        try:
            task_data = _sonar_api(
                f"/api/ce/component?component={SONAR_PROJECT_KEY}"
            )
            queue = task_data.get("queue", [])
            if not queue:
                break
        except Exception:
            pass

    print("  Fetching results...\n")

    try:
        measures = _sonar_api(
            f"/api/measures/component?component={SONAR_PROJECT_KEY}"
            "&metricKeys=bugs,vulnerabilities,code_smells,coverage,"
            "duplicated_lines_density,ncloc,sqale_rating,reliability_rating,"
            "security_rating,sqale_index"
        )
    except urllib.error.HTTPError as e:
        print(f"  Could not fetch results: {e}")
        print(f"  Check the dashboard at {SONAR_URL}/dashboard?id={SONAR_PROJECT_KEY}")
        print(f"{'─' * 62}\n")
        return

    metrics = {}
    for m in measures.get("component", {}).get("measures", []):
        metrics[m["metric"]] = m["value"]

    rating_map = {"1.0": "A", "2.0": "B", "3.0": "C", "4.0": "D", "5.0": "E"}

    print("  ┌─ SonarQube Results " + "─" * 41)
    print(f"  │  Project:        {SONAR_PROJECT_KEY}")
    print(f"  │  Lines of Code:  {metrics.get('ncloc', '—')}")
    print("  │")
    print(f"  │  Bugs:                {metrics.get('bugs', '—')}")
    print(f"  │  Vulnerabilities:     {metrics.get('vulnerabilities', '—')}")
    print(f"  │  Code Smells:         {metrics.get('code_smells', '—')}")
    debt_min = int(float(metrics.get("sqale_index", 0)))
    if debt_min >= 60:
        debt_str = f"{debt_min // 60}h {debt_min % 60}min"
    else:
        debt_str = f"{debt_min}min"
    print(f"  │  Technical Debt:      {debt_str}")
    print("  │")
    dup = metrics.get("duplicated_lines_density", "—")
    if dup != "—":
        dup = f"{float(dup):.1f}%"
    print(f"  │  Duplication:         {dup}")
    cov = metrics.get("coverage", "—")
    if cov != "—":
        cov = f"{float(cov):.1f}%"
    print(f"  │  Coverage:            {cov}")
    print("  │")
    print(f"  │  Maintainability:     {rating_map.get(metrics.get('sqale_rating', ''), '—')}")
    print(f"  │  Reliability:         {rating_map.get(metrics.get('reliability_rating', ''), '—')}")
    print(f"  │  Security:            {rating_map.get(metrics.get('security_rating', ''), '—')}")
    print("  └" + "─" * 62)
    print(f"\n  Full dashboard: {SONAR_URL}/dashboard?id={SONAR_PROJECT_KEY}")
    print(f"{'─' * 62}\n")


# ── data consumption ────────────────────────────────────────────────────────

DISCOVER_KPIS_SCRIPT = os.path.join(
    PROJECT_ROOT, "data_consumption", "tasks", "discover_kpis.py"
)

DATA_CONSUMPTION_TASKS = {
    "1": {
        "name": "Discover defined queries (KPIs)",
        "script": DISCOVER_KPIS_SCRIPT,
        "callable": "run_interactive",
    },
}


def print_data_consumption_banner():
    print(f"\n{'─' * 62}")
    print("  Data consumption tasks")
    print(f"{'─' * 62}")
    for key in sorted(DATA_CONSUMPTION_TASKS.keys()):
        print(f"   [{key}]  {DATA_CONSUMPTION_TASKS[key]['name']}")
    print("   [b]  Back to main menu")
    print()


def run_data_consumption_menu():
    """Sub-menu for exploitation-zone analytics and KPI discovery."""
    while True:
        print_data_consumption_banner()
        try:
            choice = input("  Select task [1, b]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Back to main menu.")
            break

        if choice in ("b", "q", ""):
            break
        if choice not in DATA_CONSUMPTION_TASKS:
            print("  Invalid choice. Try again.")
            continue

        task = DATA_CONSUMPTION_TASKS[choice]
        print(f"\n{'─' * 62}")
        print(f"  {task['name']}")
        print(f"{'─' * 62}\n")

        script = task["script"]
        if not os.path.isfile(script):
            print(f"  Task script not found: {script}")
            continue

        if PROJECT_ROOT not in sys.path:
            sys.path.insert(0, PROJECT_ROOT)

        try:
            import importlib.util

            spec = importlib.util.spec_from_file_location("discover_kpis", script)
            if spec is None or spec.loader is None:
                raise RuntimeError(f"Cannot load task module from {script}")
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            fn = getattr(mod, task["callable"])
            fn(from_orchestrator=True)
        except Exception as e:
            print(f"  Task failed: {e}")


# ── main menu ───────────────────────────────────────────────────────────────

def print_banner():
    print("\n" + "=" * 62)
    print("   BDM Cymatics — Manual Orchestrator")
    print("=" * 62)
    print()
    for key in sorted(FLOWS.keys()):
        flow = FLOWS[key]
        tag = " (parallel)" if flow.get("parallel") else ""
        print(f"   [{key}]  {flow['name']}{tag}")
    print("   [8]  SonarQube code analysis")
    print("   [9]  Data consumption tasks")
    print()
    print("   [q]  Quit")
    print()


def run_flow(choice):
    flow = FLOWS[choice]
    scripts = flow["scripts"]
    parallel = flow.get("parallel", False)

    print(f"\n{'─' * 62}")
    print(f"  Starting: {flow['name']}")
    if parallel:
        print(f"  Mode:     parallel ({len(scripts)} processes)")
    else:
        print(f"  Mode:     sequential ({len(scripts)} script(s))")
    for s in scripts:
        print(f"  Script:   {os.path.relpath(s, PROJECT_ROOT)}")
    print(f"{'─' * 62}\n")

    extra_args = []
    if choice in ("3", "4"):
        try:
            batch = input("  Enter batch size: ").strip()
            if batch:
                extra_args = [batch]
        except (EOFError, KeyboardInterrupt):
            print("\n  Cancelled.")
            return

    procs = []
    pids = []

    if parallel:
        for script in scripts:
            p = _run_script(script, extra_args)
            procs.append((os.path.basename(script), p))
            pids.append(p.pid)
            print(f"  Launched PID {p.pid}: {os.path.basename(script)}")
    else:
        p = _run_script(scripts[0], extra_args)
        procs.append((os.path.basename(scripts[0]), p))
        pids.append(p.pid)
        print(f"  Launched PID {p.pid}: {os.path.basename(scripts[0])}")

    _stop_monitor.clear()
    monitor_thread = threading.Thread(
        target=_monitor_resources, args=(pids,), daemon=True,
    )
    monitor_thread.start()

    try:
        results = _wait_all(procs, parallel=parallel)
    except KeyboardInterrupt:
        print("\n\n  Interrupt received — sending SIGTERM to child processes...")
        for _, proc in procs:
            try:
                proc.send_signal(signal.SIGTERM)
            except OSError:
                pass
        for _, proc in procs:
            proc.wait()
        results = [(name, proc.returncode) for name, proc in procs]
    finally:
        _stop_monitor.set()
        monitor_thread.join(timeout=3)
        try:
            os.remove(PAUSE_MONITOR_FILE)
        except FileNotFoundError:
            pass

    print(f"\n{'─' * 62}")
    print("  Results:")
    for name, rc in results:
        status = "OK" if rc == 0 else f"EXIT {rc}"
        print(f"    {name:.<45} {status}")
    print(f"{'─' * 62}\n")


def main():
    while True:
        print_banner()
        try:
            choice = input("  Select flow [1-9, q]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye!")
            break

        if choice == "q":
            print("  Bye!")
            break
        if choice == "8":
            run_sonarqube()
            continue
        if choice == "9":
            run_data_consumption_menu()
            continue
        if choice not in FLOWS:
            print("  Invalid choice. Try again.")
            continue

        run_flow(choice)


if __name__ == "__main__":
    main()
