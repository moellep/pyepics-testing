#!/usr/bin/env python3
"""Measure the server-side memory cost (and, via round_dur_s, the
per-round performance cost) of a leftover CA channel, comparing any two
scenarios from SCENARIO_REGISTRY at real scale -- spread across several
mock IOCs instead of one.

Scenario pairs run so far:
- B vs C (auto_monitor=False, left open) vs (same + clear_channel()
  every round) -- the memory question: does explicitly clearing the
  channel too, not just the monitor, meaningfully reduce cost?
- B vs D (auto_monitor=False, left open) vs (plain caget(), monitor left
  on, but the scan's own channels cleared right after) -- the
  performance question: does leaving the monitor on buy back any of the
  round-latency cost of periodic clearing?

Why several servers: a single caproto mock IOC process degrades badly well
before reaching realistic channel counts -- at 800 total defined PVs (all
scanned at 13 Hz on one asyncio event loop), a single fresh connect+get
already took ~0.5s; at 1500 it was ~1.5s; at 5000 the server never
serviced a single round in 800+ seconds. Splitting the same total PV count
across N server processes, each on its own port and its own
non-overlapping slice of buffer numbers, keeps every individual server
inside the healthy range (~500 PVs) while the client's total *open
channel* count -- which is what this measurement is actually about -- can
still reach into the thousands. It's also more representative of the real
facility, which serves its ~56 wires' PVs from many separate IOCs, not one.

Usage:
    python3 run_channel_cost_test.py                          # 10 servers x 500 PVs = 5000 channels, B vs C
    python3 run_channel_cost_test.py --scenarios B,D --out-dir ../docs/camonitor-cliff/channel_cost_bd
    python3 run_channel_cost_test.py --n-servers 20 --pvs-per-server 500

Runs each requested scenario's script (from SCENARIO_REGISTRY, each
invoked with --no-watcher) against its own fresh set of servers, with a
client EPICS_CA_ADDR_LIST spanning every server for that scenario. The
client runs fully single-threaded -- no CacheWatcher, no second thread
touching CA state at all. (Originally a workaround for a multi-server
stall that turned out to be an unrelated bug -- common.py was clobbering
the multi-server EPICS_CA_ADDR_LIST, fixed there. It's kept anyway for a
real, separately-verified reason: CacheWatcher's own use_initial_context()
call, unless CA_LOCK-protected -- now fixed at the source too -- can race
with the main thread's first CA call to corrupt libca's ctypes state.
Since this measurement doesn't need the watcher for anything -- server
RSS is sampled by THIS process, which never touches epics/CA at all --
staying single-threaded here removes that whole class of risk for free
either way.)

Writes, per requested scenario label: results_<label>.csv (columns:
elapsed_s, channels_touched_so_far, server_mem_rss_mb -- the same
per-scenario shape ../results.ipynb reads for A/B/C) and
round_times_<label>.csv (columns: round_num, elapsed_s, round_dur_s --
one row per round, the data the performance/round-duration question
needs), plus each scenario's raw server/client logs under <out-dir>/
<label>/. channels_touched_so_far is reconstructed analytically from the
client's own round-completion log lines, not assumed from
--round-interval-s (per-round time grows substantially with total channel
count already open -- see analyze()).
"""

import argparse
import csv
import os
import re
import socket
import subprocess
import sys
import threading
import time

import common  # noqa: F401  (this process launches subprocesses rather than making CA
                              # calls itself, but sets EPICS_CA_ADDR_LIST/AUTO_ADDR_LIST
                              # for consistency; each client subprocess gets its own env below)

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
DEFAULT_OUT_DIR = os.path.join(REPO_ROOT, "docs", "camonitor-cliff", "channel_cost")
BASE_PORT = 5064

SCENARIO_REGISTRY = {
    "B": ("run_fixed.py", "auto_monitor=False"),
    "C": ("run_clear_channel.py", "auto_monitor=False + clear_channel()"),
    "D": ("run_periodic_monitor.py", "caget() default (monitor on) + periodic scoped clear"),
}

_ROUND_DONE_RE = re.compile(
    r"-- round (\d+) done .* elapsed=([\d.]+)s(?: round_dur_s=([\d.]+))? --"
)


def read_rss_mb(pid: int) -> float:
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return -1.0


def wait_for_server(port, host="127.0.0.1", timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def stop_process(proc):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)


def run_one_scenario(label, script, n_servers, n_buffers_per_server, pvs_per_buffer,
                      round_interval_s, out_dir):
    """Launch n_servers fresh mock IOCs, run `script` against all of them, and
    return (samples, client_log_path) where samples is a list of
    (elapsed_s, server_rss_mb) polled by THIS process throughout the run.
    """
    total_buffers = n_buffers_per_server * n_servers
    scenario_dir = os.path.join(out_dir, label)
    os.makedirs(scenario_dir, exist_ok=True)

    servers = []
    addr_list_parts = []
    server_logs = []
    samples = []
    try:
        for i in range(n_servers):
            port = BASE_PORT + i
            buffer_start = i * n_buffers_per_server
            log = open(os.path.join(scenario_dir, f"server_{i}.log"), "w")
            server_logs.append(log)
            proc = subprocess.Popen(
                [sys.executable, "-u", "ioc_server.py",
                 "--n-buffers", str(n_buffers_per_server),
                 "--pvs-per-buffer", str(pvs_per_buffer),
                 "--buffer-start", str(buffer_start),
                 "--port", str(port)]
                + ([] if i == 0 else ["--no-canary"]),
                cwd=HERE, stdout=log, stderr=subprocess.STDOUT,
            )
            servers.append(proc)
            addr_list_parts.append(f"127.0.0.1:{port}")
            if not wait_for_server(port):
                raise RuntimeError(f"[{label}] server {i} (port {port}) never started listening")
        print(f"[{label}] all {n_servers} servers up. PIDs: {[p.pid for p in servers]}")

        client_log_path = os.path.join(scenario_dir, "client.log")
        client_env = dict(os.environ)
        client_env["EPICS_CA_ADDR_LIST"] = " ".join(addr_list_parts)
        client_env["EPICS_CA_AUTO_ADDR_LIST"] = "NO"

        client_args = [
            sys.executable, "-u", script,
            "--n-buffers", str(total_buffers),
            "--pvs-per-buffer", str(pvs_per_buffer),
            "--round-interval-s", str(round_interval_s),
            "--n-rounds", str(total_buffers + 10),
            "--no-watcher",
            "--csv", os.path.join(scenario_dir, "client_unused.csv"),
        ]
        print(f"[{label}] running client against all {n_servers} servers "
              f"({total_buffers} rounds to touch every buffer once), "
              f"sampling server RSS from this process...")

        rss_stop = threading.Event()

        def poll_rss():
            t_start = time.monotonic()
            while not rss_stop.is_set():
                elapsed = time.monotonic() - t_start
                total_rss = sum(read_rss_mb(p.pid) for p in servers)
                samples.append((round(elapsed, 1), round(total_rss, 2)))
                rss_stop.wait(1.0)

        rss_thread = threading.Thread(target=poll_rss, daemon=True)
        rss_thread.start()
        try:
            with open(client_log_path, "w") as client_log:
                # Per-round time grows with total channel count already open
                # (observed: ~3s/round at a few hundred open channels, vs.
                # near-instant at a few dozen) -- well above round_interval_s
                # itself, which only bounds the *sleep* between rounds. 15s/
                # round is a generous ceiling, not the expected rate.
                result = subprocess.run(
                    client_args, cwd=HERE, stdout=client_log, stderr=subprocess.STDOUT,
                    env=client_env, timeout=total_buffers * 15 + 120,
                )
        finally:
            rss_stop.set()
            rss_thread.join(timeout=5.0)

        if result.returncode != 0:
            raise RuntimeError(f"[{label}] client exited {result.returncode} -- see {client_log_path}")
        print(f"[{label}] client done ({len(samples)} RSS samples)")
    finally:
        for proc in servers:
            stop_process(proc)
        for log in server_logs:
            log.close()

    return samples, client_log_path


def parse_round_times(client_log_path):
    """[(round_num, elapsed_s, round_dur_s), ...] sorted by round_num, from
    the client's own "-- round N done ... elapsed=X.Xs round_dur_s=Y.YYY
    --" lines. round_dur_s is None for a log line that predates that field
    (kept optional in the regex for that reason)."""
    rounds = []
    with open(client_log_path) as f:
        for line in f:
            m = _ROUND_DONE_RE.search(line)
            if m:
                dur = float(m.group(3)) if m.group(3) is not None else None
                rounds.append((int(m.group(1)), float(m.group(2)), dur))
    rounds.sort()
    return rounds


def channels_touched_at(elapsed_s, round_times, total_buffers, pvs_per_buffer):
    """How many distinct PVs had been read by elapsed_s, reconstructed from
    round-completion timestamps rather than assumed from configured timing
    (see module docstring)."""
    completed = sum(1 for _, t, _ in round_times if t <= elapsed_s)
    return min(completed, total_buffers) * pvs_per_buffer


def analyze(label, desc, samples, round_times, total_channels, total_buffers, pvs_per_buffer):
    """Settled-baseline vs real-plateau delta -> KB/channel. Returns a dict
    summary for cross-scenario comparison; also prints its own block.

    Two things this does NOT assume, both observed to matter at real scale:
    - That round timing matches --round-interval-s (it doesn't, once enough
      channels are already open -- per-round time grows with total channel
      count). Uses the real round-completion timestamps instead.
    - That server RSS at t=0 is a clean "zero channels" baseline. Server
      RSS jumps during the first ~1-2s of asyncio/scan-callback warm-up,
      independent of any client channels. Uses the first sample at t >= 2.0s
      instead.
    """
    if len(samples) < 2:
        print(f"[{label}] not enough samples to analyze.")
        return None

    baseline_t, baseline_rss = next((pt for pt in samples if pt[0] >= 2.0), samples[0])

    plateau_t = next((t for r, t, _ in round_times if r == total_buffers - 1), None)
    if plateau_t is None:
        print(f"[{label}] (warning: couldn't find round {total_buffers - 1}'s completion "
              f"line; the plateau point below may be inaccurate)")
        plateau_t = samples[-1][0]

    plateau_sample = next((pt for pt in samples if pt[0] >= plateau_t), samples[-1])
    final_t, final_rss = samples[-1]

    delta_at_plateau = plateau_sample[1] - baseline_rss
    per_channel_kb = (delta_at_plateau / total_channels) * 1024 if total_channels else 0

    print(f"\n--- {label} ({desc}) ---")
    print(f"  settled baseline (t={baseline_t:.1f}s, before any channels): {baseline_rss:.1f} MB")
    print(f"  real plateau (round {total_buffers - 1} done, all {total_channels} channels "
          f"touched): t={plateau_t:.1f}s, RSS={plateau_sample[1]:.1f} MB")
    print(f"  final sample (t={final_t:.1f}s): RSS={final_rss:.1f} MB "
          f"(post-plateau growth: {final_rss - plateau_sample[1]:+.1f} MB)")
    print(f"  delta at plateau: {delta_at_plateau:.1f} MB / {total_channels} channels "
          f"= {per_channel_kb:.2f} KB/channel")

    return {
        "label": label,
        "baseline_rss": baseline_rss,
        "plateau_rss": plateau_sample[1],
        "delta_mb": delta_at_plateau,
        "per_channel_kb": per_channel_kb,
    }


def analyze_round_durations(label, round_times):
    """Mean/median/p95 round_dur_s, plus a first-10-vs-last-10-rounds trend
    -- the direct answer to whether a scenario's per-round work (not
    server memory) is getting cheaper, pricier, or staying flat as it
    repeatedly touches/clears the same small set of names. Returns a dict
    summary; also prints its own block. None if no round carried timing
    (an old-format log line, or too few rounds)."""
    durs = [d for _, _, d in round_times if d is not None]
    if len(durs) < 2:
        print(f"[{label}] no round_dur_s data to analyze.")
        return None

    durs_sorted = sorted(durs)
    n = len(durs_sorted)
    mean_s = sum(durs_sorted) / n
    median_s = durs_sorted[n // 2]
    p95_s = durs_sorted[int(n * 0.95)] if n >= 20 else durs_sorted[-1]
    first10 = sum(durs[:10]) / min(10, n)
    last10 = sum(durs[-10:]) / min(10, n)

    print(f"\n--- {label} round duration (performance) ---")
    print(f"  mean={mean_s:.3f}s  median={median_s:.3f}s  p95={p95_s:.3f}s  "
          f"(n={n} rounds)")
    print(f"  first 10 rounds avg={first10:.3f}s  last 10 rounds avg={last10:.3f}s  "
          f"({'growing' if last10 > first10 * 1.1 else 'shrinking' if last10 < first10 * 0.9 else 'flat'})")

    return {
        "label": label,
        "mean_s": mean_s,
        "median_s": median_s,
        "p95_s": p95_s,
        "first10_s": first10,
        "last10_s": last10,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-servers", type=int, default=10)
    parser.add_argument(
        "--pvs-per-server", type=int, default=500,
        help="total PVs each server instance defines (n_buffers_per_server * pvs_per_buffer)",
    )
    parser.add_argument("--pvs-per-buffer", type=int, default=20)
    parser.add_argument("--round-interval-s", type=float, default=0.3)
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--scenarios", default="B,C",
        help=f"comma-separated scenario labels to compare, from {sorted(SCENARIO_REGISTRY)} "
             "(default B,C -- the memory comparison; pass B,D for the round-duration/"
             "performance comparison)",
    )
    args = parser.parse_args()

    labels = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    unknown = [l for l in labels if l not in SCENARIO_REGISTRY]
    if unknown:
        raise SystemExit(f"unknown scenario label(s) {unknown}, choose from {sorted(SCENARIO_REGISTRY)}")
    scenarios = [(label, *SCENARIO_REGISTRY[label]) for label in labels]

    n_buffers_per_server = args.pvs_per_server // args.pvs_per_buffer
    total_buffers = n_buffers_per_server * args.n_servers
    total_channels = total_buffers * args.pvs_per_buffer
    os.makedirs(args.out_dir, exist_ok=True)

    print(f"Total: {args.n_servers} servers x {args.pvs_per_server} PVs = "
          f"{total_channels} channels, per scenario ({', '.join(labels)})")

    summaries = []
    duration_summaries = []
    for label, script, desc in scenarios:
        print(f"\n=== Scenario {label}: {script} ===")
        samples, client_log_path = run_one_scenario(
            label, script, args.n_servers, n_buffers_per_server, args.pvs_per_buffer,
            args.round_interval_s, args.out_dir,
        )
        round_times = parse_round_times(client_log_path)

        results_csv = os.path.join(args.out_dir, f"results_{label}.csv")
        with open(results_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["elapsed_s", "channels_touched_so_far", "server_mem_rss_mb"])
            for elapsed_s, rss in samples:
                touched = channels_touched_at(elapsed_s, round_times, total_buffers, args.pvs_per_buffer)
                w.writerow([elapsed_s, touched, rss])
        print(f"[{label}] wrote {results_csv}")

        round_times_csv = os.path.join(args.out_dir, f"round_times_{label}.csv")
        with open(round_times_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["round_num", "elapsed_s", "round_dur_s"])
            for round_num, elapsed_s, dur in round_times:
                w.writerow([round_num, elapsed_s, dur if dur is not None else ""])
        print(f"[{label}] wrote {round_times_csv}")

        summary = analyze(
            label, desc, samples, round_times, total_channels, total_buffers, args.pvs_per_buffer,
        )
        if summary:
            summaries.append(summary)

        duration_summary = analyze_round_durations(label, round_times)
        if duration_summary:
            duration_summaries.append(duration_summary)

    if len(summaries) == len(scenarios):
        print("\n" + "=" * 60)
        print("COMPARISON -- memory")
        for s in summaries:
            print(f"  {s['label']}: {s['per_channel_kb']:.2f} KB/channel "
                  f"(delta {s['delta_mb']:.1f} MB over {total_channels} channels)")

    if len(duration_summaries) == len(scenarios):
        print("\nCOMPARISON -- round duration (performance)")
        for s in duration_summaries:
            print(f"  {s['label']}: mean={s['mean_s']:.3f}s median={s['median_s']:.3f}s "
                  f"p95={s['p95_s']:.3f}s last10avg={s['last10_s']:.3f}s")


if __name__ == "__main__":
    main()
