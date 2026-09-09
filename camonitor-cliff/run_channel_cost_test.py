#!/usr/bin/env python3
"""Measure the server-side memory cost (and, via round_dur_s, the
per-round performance cost) of a leftover CA channel, comparing any two
or more scenarios from SCENARIO_REGISTRY at real scale -- spread across
several mock IOCs instead of one.

Scenario comparisons run so far:
- B vs C (auto_monitor=False, left open) vs (same + clear_channel()
  every round) -- the memory question: does explicitly clearing the
  channel too, not just the monitor, meaningfully reduce cost?
- B vs D (auto_monitor=False, left open) vs (plain caget(), monitor left
  on, but the scan's own channels cleared right after) -- the
  performance question: does leaving the monitor on buy back any of the
  round-latency cost of periodic clearing?
- B vs D vs E (B, plus clearing only the client-side cached *value* after
  each read, channel left intact) -- B leaves epics.ca._cache[ctx]
  [pvname].get_results holding the full last-read value forever
  (confirmed directly: ~24.5 KB/channel client-side at 5,000 waveform
  channels, matching one array almost exactly, since disconnect() never
  touches get_results). E tests whether clearing just that cached value
  -- not the whole channel, so none of C's shared-chid clear_channel()
  risk -- keeps B's small server-side profile while avoiding this
  client-side growth.

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
    python3 run_channel_cost_test.py --scenarios B,D,E --out-dir ../docs/camonitor-cliff/channel_cost_bd
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
elapsed_s, channels_touched_so_far, server_mem_rss_mb, client_mem_rss_mb
-- server RSS summed across all servers, client RSS from the one client
subprocess, polled by this orchestrator process itself rather than by an
in-client CacheWatcher, since the client runs with --no-watcher) and
round_times_<label>.csv (columns: round_num, elapsed_s, round_dur_s,
total_buffers -- one row per round, the data the performance/round-duration
question needs; total_buffers is the same value repeated every row, the
round_num at which buffer numbers start repeating -- see --extra-rounds
below), plus each scenario's raw server/client logs under <out-dir>/
<label>/. channels_touched_so_far is reconstructed analytically from the
client's own round-completion log lines, not assumed from
--round-interval-s (per-round time grows substantially with total channel
count already open -- see analyze()).

--extra-rounds (default 50) controls how far each client runs past the
buffer-pool rollover (round_num == total_buffers, where round_num %
n_buffers starts repeating already-touched buffer numbers) -- e.g. Scenario
B's round_dur_s drops sharply right at that round, because the channels it
opened for those buffer numbers earlier in the run are still open (never
cleared), so create_channel() hits its cache instead of paying for a fresh
CA connect. The default gives a large enough post-rollover sample for that
effect (or its absence, for a scenario that clears every round) to show up
clearly rather than as just the original 10-row tail.
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
    "E": ("run_clear_value_cache.py", "auto_monitor=False + clear client value cache"),
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
                      round_interval_s, out_dir, extra_rounds=10):
    """Launch n_servers fresh mock IOCs, run `script` against all of them, and
    return (samples, client_log_path) where samples is a list of
    (elapsed_s, server_rss_mb, client_rss_mb) polled by THIS process
    throughout the run -- server_rss_mb summed across all n_servers,
    client_rss_mb from the one client subprocess itself.
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

        n_rounds = total_buffers + extra_rounds
        client_args = [
            sys.executable, "-u", script,
            "--n-buffers", str(total_buffers),
            "--pvs-per-buffer", str(pvs_per_buffer),
            "--round-interval-s", str(round_interval_s),
            "--n-rounds", str(n_rounds),
            "--no-watcher",
            "--csv", os.path.join(scenario_dir, "client_unused.csv"),
        ]
        print(f"[{label}] running client against all {n_servers} servers "
              f"({total_buffers} rounds to touch every buffer once, "
              f"{extra_rounds} more past the buffer-pool rollover), "
              f"sampling server+client RSS from this process...")

        rss_stop = threading.Event()
        client_proc_holder = {}  # populated once Popen returns, below

        def poll_rss():
            t_start = time.monotonic()
            while not rss_stop.is_set():
                elapsed = time.monotonic() - t_start
                total_server_rss = sum(read_rss_mb(p.pid) for p in servers)
                client_proc = client_proc_holder.get("proc")
                client_rss = read_rss_mb(client_proc.pid) if client_proc is not None else 0.0
                samples.append((round(elapsed, 1), round(total_server_rss, 2), round(client_rss, 2)))
                rss_stop.wait(1.0)

        rss_thread = threading.Thread(target=poll_rss, daemon=True)
        rss_thread.start()
        try:
            # Popen, not run(): need the client's own PID immediately (to
            # poll its RSS above), not just its exit code after it's done.
            with open(client_log_path, "w") as client_log:
                client_proc = subprocess.Popen(
                    client_args, cwd=HERE, stdout=client_log, stderr=subprocess.STDOUT,
                    env=client_env,
                )
            client_proc_holder["proc"] = client_proc
            try:
                # Per-round time grows with total channel count already open
                # (observed: ~3s/round at a few hundred open channels, vs.
                # near-instant at a few dozen) -- well above round_interval_s
                # itself, which only bounds the *sleep* between rounds. 15s/
                # round is a generous ceiling, not the expected rate (rounds
                # past the buffer-pool rollover run much faster than this,
                # so extra_rounds doesn't need its own, smaller budget).
                client_proc.wait(timeout=n_rounds * 15 + 120)
            except subprocess.TimeoutExpired:
                client_proc.kill()
                client_proc.wait(timeout=5.0)
                raise
        finally:
            rss_stop.set()
            rss_thread.join(timeout=5.0)

        if client_proc.returncode != 0:
            raise RuntimeError(f"[{label}] client exited {client_proc.returncode} -- see {client_log_path}")
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

    baseline_t, baseline_server, baseline_client = next(
        (pt for pt in samples if pt[0] >= 2.0), samples[0]
    )

    plateau_t = next((t for r, t, _ in round_times if r == total_buffers - 1), None)
    if plateau_t is None:
        print(f"[{label}] (warning: couldn't find round {total_buffers - 1}'s completion "
              f"line; the plateau point below may be inaccurate)")
        plateau_t = samples[-1][0]

    plateau_sample = next((pt for pt in samples if pt[0] >= plateau_t), samples[-1])
    _, plateau_server, plateau_client = plateau_sample
    final_t, final_server, final_client = samples[-1]

    delta_server = plateau_server - baseline_server
    delta_client = plateau_client - baseline_client
    per_channel_kb_server = (delta_server / total_channels) * 1024 if total_channels else 0
    per_channel_kb_client = (delta_client / total_channels) * 1024 if total_channels else 0

    print(f"\n--- {label} ({desc}) ---")
    print(f"  settled baseline (t={baseline_t:.1f}s, before any channels): "
          f"server={baseline_server:.1f} MB, client={baseline_client:.1f} MB")
    print(f"  real plateau (round {total_buffers - 1} done, all {total_channels} channels "
          f"touched): t={plateau_t:.1f}s, server={plateau_server:.1f} MB, "
          f"client={plateau_client:.1f} MB")
    print(f"  final sample (t={final_t:.1f}s): server={final_server:.1f} MB "
          f"(post-plateau growth: {final_server - plateau_server:+.1f} MB), "
          f"client={final_client:.1f} MB (post-plateau growth: "
          f"{final_client - plateau_client:+.1f} MB)")
    print(f"  delta at plateau over {total_channels} channels: "
          f"server={delta_server:.1f} MB ({per_channel_kb_server:.2f} KB/channel), "
          f"client={delta_client:.1f} MB ({per_channel_kb_client:.2f} KB/channel)")

    return {
        "label": label,
        "baseline_rss": baseline_server,
        "plateau_rss": plateau_server,
        "delta_mb": delta_server,
        "per_channel_kb": per_channel_kb_server,
        "client_baseline_rss": baseline_client,
        "client_plateau_rss": plateau_client,
        "client_delta_mb": delta_client,
        "client_per_channel_kb": per_channel_kb_client,
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
    parser.add_argument(
        "--extra-rounds", type=int, default=50,
        help="rounds to run past the buffer-pool rollover (round_num == total_buffers, "
             "where buffer numbers start repeating) -- default 50 gives a clear "
             "post-rollover signal for the round-duration comparison",
    )
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--scenarios", default="B,C",
        help=f"comma-separated scenario labels to compare, from {sorted(SCENARIO_REGISTRY)} "
             "(default B,C -- the memory comparison; pass B,D for the round-duration/"
             "performance comparison; pass B,D,E to also test clearing just the "
             "client-side value cache)",
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
            args.round_interval_s, args.out_dir, extra_rounds=args.extra_rounds,
        )
        round_times = parse_round_times(client_log_path)

        results_csv = os.path.join(args.out_dir, f"results_{label}.csv")
        with open(results_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["elapsed_s", "channels_touched_so_far", "server_mem_rss_mb", "client_mem_rss_mb"])
            for elapsed_s, server_rss, client_rss in samples:
                touched = channels_touched_at(elapsed_s, round_times, total_buffers, args.pvs_per_buffer)
                w.writerow([elapsed_s, touched, server_rss, client_rss])
        print(f"[{label}] wrote {results_csv}")

        round_times_csv = os.path.join(args.out_dir, f"round_times_{label}.csv")
        with open(round_times_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["round_num", "elapsed_s", "round_dur_s", "total_buffers"])
            for round_num, elapsed_s, dur in round_times:
                w.writerow([round_num, elapsed_s, dur if dur is not None else "", total_buffers])
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
        print("COMPARISON -- server memory")
        for s in summaries:
            print(f"  {s['label']}: {s['per_channel_kb']:.2f} KB/channel "
                  f"(delta {s['delta_mb']:.1f} MB over {total_channels} channels)")
        print("\nCOMPARISON -- client memory")
        for s in summaries:
            print(f"  {s['label']}: {s['client_per_channel_kb']:.2f} KB/channel "
                  f"(delta {s['client_delta_mb']:.1f} MB over {total_channels} channels)")

    if len(duration_summaries) == len(scenarios):
        print("\nCOMPARISON -- round duration (performance)")
        for s in duration_summaries:
            print(f"  {s['label']}: mean={s['mean_s']:.3f}s median={s['median_s']:.3f}s "
                  f"p95={s['p95_s']:.3f}s last10avg={s['last10_s']:.3f}s")


if __name__ == "__main__":
    main()
