#!/usr/bin/env python3
"""Run all three scenarios (A: caget() bug, B: auto_monitor=False,
C: + clear_channel()) back-to-back, each against its own freshly-started
mock IOC, and merge their results into one comparison.

This is the orchestration that produced the numbers/charts in README.md --
previously done by hand (start a server, run a scenario, kill it, repeat,
then a one-off script to merge the CSVs). Running each scenario against a
*fresh* server, one at a time, matters: epics.pv._PVcache_/epics.ca state
is process-global, so scenarios must run as separate processes, and a
server a prior scenario has already grown to gigabytes would contaminate
the next scenario's memory-growth measurement.

Usage:
    python3 run_comparison.py                      # 150s default per scenario
    python3 run_comparison.py --n-buffers 25 --pvs-per-buffer 20 \
        --round-interval-s 1.0 --max-seconds 400    # reproduces README.md's documented numbers

Writes one CSV per scenario (from each scenario script's own CacheWatcher),
a merged results.json (same shape the published comparison artifact's
chart data uses: {"A": [...], "B": [...], "C": [...]}), and prints a
summary table -- by default under <repo_root>/docs/camonitor-cliff/run_comparison/,
regardless of the working directory this is run from, so results land in
one place meant to be committed as reference output (see .gitignore's
docs/ exemption). Pass --out-dir to write elsewhere instead.

Occasionally (rare, not caused by --out-dir/output location) a scenario's
client fails with a ctypes.ArgumentError out of epics.ca.pend_io -- the
same general EPICS CA fragility this whole test exists to probe, not a bug
in this script. Re-running the same command has always succeeded so far;
there's no retry loop here because a silent auto-retry would hide exactly
the kind of intermittent CA flakiness this repo is meant to surface.
"""

import argparse
import csv
import json
import os
import socket
import subprocess
import sys
import time

import common

SCENARIOS = [
    ("A", "run_caget_bug.py", "caget() -- the bug"),
    ("B", "run_fixed.py", "auto_monitor=False"),
    ("C", "run_clear_channel.py", "auto_monitor=False + clear_channel()"),
]

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
DEFAULT_OUT_DIR = os.path.join(REPO_ROOT, "docs", "camonitor-cliff", "run_comparison")


def wait_for_server(host="127.0.0.1", port=5064, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def stop_process(proc, name):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        print(f"[run_comparison] {name} did not stop cleanly, killing")
        proc.kill()
        proc.wait(timeout=5.0)


def run_one_scenario(label, script, args, out_dir):
    csv_path = os.path.join(out_dir, f"results_{label}.csv")
    server_log = open(os.path.join(out_dir, f"server_{label}.log"), "w")
    client_log_path = os.path.join(out_dir, f"client_{label}.log")

    print(f"\n=== Scenario {label}: {script} ===")
    server = subprocess.Popen(
        [sys.executable, "-u", "ioc_server.py",
         "--n-buffers", str(args.n_buffers),
         "--pvs-per-buffer", str(args.pvs_per_buffer)],
        cwd=HERE, stdout=server_log, stderr=subprocess.STDOUT,
    )
    print(f"  server pid={server.pid}, waiting for it to accept connections...")
    if not wait_for_server():
        stop_process(server, "server")
        raise RuntimeError(f"server for scenario {label} never started listening")

    client_args = [
        sys.executable, "-u", script,
        "--n-buffers", str(args.n_buffers),
        "--pvs-per-buffer", str(args.pvs_per_buffer),
        "--round-interval-s", str(args.round_interval_s),
        "--n-rounds", str(args.n_rounds),
        "--server-pids", str(server.pid),
        "--csv", csv_path,
    ]
    if args.max_seconds is not None:
        client_args += ["--max-seconds", str(args.max_seconds)]

    client_timeout = (args.max_seconds or args.n_rounds * args.round_interval_s) + 60
    print(f"  running client (timeout {client_timeout:.0f}s)...")
    with open(client_log_path, "w") as client_log:
        result = subprocess.run(
            client_args, cwd=HERE, stdout=client_log, stderr=subprocess.STDOUT,
            timeout=client_timeout,
        )

    stop_process(server, "server")
    server_log.close()

    if result.returncode != 0:
        raise RuntimeError(
            f"scenario {label} client exited {result.returncode} -- see {client_log_path}"
        )
    print(f"  done -- {csv_path}")
    return csv_path


def load_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def summarize(label, desc, rows):
    first, last = rows[0], rows[-1]
    lat = [float(r["latency_p50_ms"]) for r in rows if r["latency_p50_ms"] not in ("", None)]
    print(f"\n--- {label} ({desc}) ---")
    print(f"  elapsed:            {first['elapsed_s']}s -> {last['elapsed_s']}s")
    print(f"  monitored_count:    {first['monitored_count']} -> {last['monitored_count']}")
    print(
        f"  server_mem_rss_mb:  {first['server_mem_rss_mb']} -> {last['server_mem_rss_mb']} "
        f"(delta={float(last['server_mem_rss_mb']) - float(first['server_mem_rss_mb']):.1f})"
    )
    print(
        f"  client_mem_rss_mb:  {first['client_mem_rss_mb']} -> {last['client_mem_rss_mb']} "
        f"(delta={float(last['client_mem_rss_mb']) - float(first['client_mem_rss_mb']):.1f})"
    )
    if lat:
        print(f"  latency p50 (ms):   min={min(lat):.1f} max={max(lat):.1f} last={lat[-1]:.1f}")


def to_chart_series(rows):
    series = []
    for r in rows:
        e = {
            "t": round(float(r["elapsed_s"]), 1),
            "m": int(r["monitored_count"]),
            "s": round(float(r["server_mem_rss_mb"]), 1),
            "c": round(float(r["client_mem_rss_mb"]), 2),
        }
        if r["latency_p50_ms"] not in ("", None):
            e["p50"] = round(float(r["latency_p50_ms"]), 1)
            e["p95"] = round(float(r["latency_p95_ms"]), 1)
            e["p99"] = round(float(r["latency_p99_ms"]), 1)
        series.append(e)
    return series


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-buffers", type=int, default=common.N_BUFFERS)
    parser.add_argument("--pvs-per-buffer", type=int, default=common.PVS_PER_BUFFER)
    parser.add_argument("--round-interval-s", type=float, default=1.0)
    parser.add_argument("--n-rounds", type=int, default=400)
    parser.add_argument(
        "--max-seconds", type=float, default=150.0,
        help="wall-clock budget per scenario (default 150s; pass a negative value for none)",
    )
    parser.add_argument(
        "--out-dir", default=DEFAULT_OUT_DIR,
        help=f"default: {DEFAULT_OUT_DIR} (repo_root/docs/camonitor-cliff/run_comparison, regardless of cwd)",
    )
    args = parser.parse_args()
    if args.max_seconds is not None and args.max_seconds < 0:
        args.max_seconds = None

    os.makedirs(args.out_dir, exist_ok=True)

    results = {}
    for label, script, desc in SCENARIOS:
        csv_path = run_one_scenario(label, script, args, args.out_dir)
        results[label] = load_csv(csv_path)

    print("\n" + "=" * 60)
    print("SUMMARY")
    for label, script, desc in SCENARIOS:
        summarize(label, desc, results[label])

    chart_data = {label: to_chart_series(rows) for label, rows in results.items()}
    json_path = os.path.join(args.out_dir, "results.json")
    with open(json_path, "w") as f:
        json.dump(chart_data, f, separators=(",", ":"))
    print(f"\nMerged chart data written to {json_path}")


if __name__ == "__main__":
    main()
