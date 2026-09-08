#!/usr/bin/env python3
"""Scenario C: the fully-clean pattern -- auto_monitor=False + clear_channel().

Extends Scenario B (run_fixed.py) with the one thing it was shown to still
leave behind: pv.disconnect() detaches the PV wrapper/monitor but -- per its
own docstring -- "keeps corresponding Epics CA connection intact." This adds
an explicit epics.ca.clear_channel(pv.chid) right after disconnect(), so no
channel accumulates in epics.ca._cache at all, not just no monitor.

Per claude/lazy-pv-clear-channel.md's danger analysis: calling
clear_channel() unconditionally is NOT safe in general (pyepics shares one
channel per (pvname, context) across every caller in a process -- clearing
it out from under some other unrelated PV/LazyPV/get_pv() reference to the
same name is what plausibly caused the "dropping the pv... may cause
crashes" symptom in the 2026-09-03 meeting notes). It's only safe here
because each HST<n>-suffixed name is exclusive to this one buffer's read --
nothing else in this test process ever touches the same name concurrently.

Run the mock IOC first: python3 ioc_server.py
"""

import argparse
import time

import common  # noqa: F401  (sets CA env vars before epics import)

import epics
import epics.ca
from monitor_watcher import CacheWatcher


def _main_thread_channel_count() -> int:
    ctx = epics.ca.current_context()
    return len(epics.ca._cache.get(ctx, {})) if ctx else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-buffers", type=int, default=common.N_BUFFERS)
    parser.add_argument("--pvs-per-buffer", type=int, default=common.PVS_PER_BUFFER)
    parser.add_argument("--n-rounds", type=int, default=20)
    parser.add_argument("--round-interval-s", type=float, default=2.0)
    parser.add_argument(
        "--max-seconds", type=float, default=None,
        help="stop after this many wall-clock seconds, even if --n-rounds hasn't been reached",
    )
    parser.add_argument("--csv", default="results_clear_channel.csv")
    parser.add_argument(
        "--server-pids", type=int, nargs="+", default=None,
        help="PID(s) of ioc_server.py instance(s), to also track their summed RSS "
             "(each prints its PID in its startup message) -- pass several when spreading "
             "channels across multiple servers, see run_channel_cost_test.py",
    )
    parser.add_argument(
        "--no-latency-probe", action="store_true",
        help="skip the canary-latency probe -- required when the canary PV isn't reachable "
             "from every server address (see CacheWatcher's probe_latency docstring)",
    )
    parser.add_argument(
        "--no-watcher", action="store_true",
        help="don't run CacheWatcher at all (no background thread in this process) -- see "
             "run_fixed.py's --no-watcher docstring for why; same reasoning applies here.",
    )
    args = parser.parse_args()

    print(
        f"Scenario C (clear_channel): {args.n_rounds} rounds (max {args.max_seconds}s), "
        f"{args.n_buffers} buffers x {args.pvs_per_buffer} signals "
        f"(expected monitored_count AND channel_count to stay near 0), "
        f"{args.round_interval_s}s between rounds -> {args.csv}"
    )

    watcher = None
    if not args.no_watcher:
        watcher = CacheWatcher(
            args.csv, server_pids=args.server_pids, probe_latency=not args.no_latency_probe
        )
        watcher.start()
    start = time.monotonic()

    try:
        for round_num in range(args.n_rounds):
            buf = round_num % args.n_buffers
            for sig_idx in range(args.pvs_per_buffer):
                with common.CA_LOCK:
                    pv = epics.PV(common.pv_name(sig_idx, buf), auto_monitor=False)
                    pv.get(use_monitor=False, timeout=5.0)
                    chid = pv.chid
                    pv.disconnect()
                    if chid is not None:
                        epics.ca.clear_channel(chid)
            with common.CA_LOCK:
                n = _main_thread_channel_count()
            print(f"-- round {round_num} done (buffer {buf}) elapsed={time.monotonic()-start:.1f}s "
                  f"-- main-thread channels={n} --")
            if args.max_seconds is not None and time.monotonic() - start >= args.max_seconds:
                print(f"-- max_seconds ({args.max_seconds}) reached, stopping early --")
                break
            time.sleep(args.round_interval_s)
    finally:
        if watcher is not None:
            watcher.stop()

    with common.CA_LOCK:
        n = _main_thread_channel_count()
    print(
        f"Finished. main-thread epics.ca._cache: {n} channel entries remain "
        f"after {args.n_rounds} rounds across "
        f"{args.n_buffers * args.pvs_per_buffer} distinct PVs "
        f"(Scenario B left {args.n_buffers * args.pvs_per_buffer} behind under "
        f"the same conditions) -- see {args.csv}."
    )


if __name__ == "__main__":
    main()
