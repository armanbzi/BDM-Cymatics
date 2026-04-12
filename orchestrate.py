#!/usr/bin/env python3
"""
Manual Orchestrator — BDM Cymatics Pipeline.

Interactive CLI that lets you choose which flow to run:

  1. Warm path          (microphone → approve → store audio )
  2. Hot path           (producer + consumer in parallel)
  3. Cold path: Freesound  (batch Freesound ingestion)
  4. Cold path: ESC-50     (batch ESC-50 ingestion)
  5. Trusted zone       (enrich all landing-zone audios)
  6. Sync Delta Lake    (Parquet → Delta Lake)

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

try:
    import psutil
except ImportError:
    print("psutil is required for resource monitoring.")
    print("Install it with:  pip install psutil")
    sys.exit(1)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PAUSE_MONITOR_FILE = os.path.join(tempfile.gettempdir(), ".cymatics_pause_monitor")

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
        "name": "Sync Delta Lake",
        "scripts": [
            os.path.join(PROJECT_ROOT, "exploitation_zone", "sync_delta.py"),
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

def _run_script(script_path, extra_args=None):
    """Launch a Python script as a subprocess, return the Popen object."""
    cmd = [sys.executable, script_path] + (extra_args or [])
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
            choice = input("  Select flow [1-6, q]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye!")
            break

        if choice == "q":
            print("  Bye!")
            break
        if choice not in FLOWS:
            print("  Invalid choice. Try again.")
            continue

        run_flow(choice)


if __name__ == "__main__":
    main()
