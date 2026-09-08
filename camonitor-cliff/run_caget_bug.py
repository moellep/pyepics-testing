#!/usr/bin/env python3
"""Scenario A: reproduce the caget() monitor-accumulation bug.

Simulates repeated wire scans reading a rotating pool of buffer-numbered
history PVs via epics.caget() -- exactly what
slac_timing.buffer.Buffer._fetch_single() does today (see
claude/lazy-pv-clear-channel.md). Each caget() silently creates a
persistent camonitor (pyepics' auto_monitor default), which is never
released.

Run the mock IOC first: python3 ioc_server.py
"""

import argparse
import time

import common  # noqa: F401  (sets CA env vars before epics import)

import epics
from monitor_watcher import CacheWatcher


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
    parser.add_argument("--csv", default="results_caget_bug.csv")
    parser.add_argument(
        "--server-pids", type=int, nargs="+", default=None,
        help="PID(s) of ioc_server.py instance(s), to also track their summed RSS "
             "(each prints its PID in its startup message)",
    )
    args = parser.parse_args()

    expected_plateau = args.n_buffers * args.pvs_per_buffer
    print(
        f"Scenario A (caget bug): {args.n_rounds} rounds (max {args.max_seconds}s), "
        f"{args.n_buffers} buffers x {args.pvs_per_buffer} signals "
        f"(expected monitored_count plateau = {expected_plateau}), "
        f"{args.round_interval_s}s between rounds -> {args.csv}"
    )

    watcher = CacheWatcher(args.csv, server_pids=args.server_pids)
    watcher.start()
    start = time.monotonic()

    try:
        for round_num in range(args.n_rounds):
            buf = round_num % args.n_buffers
            for sig_idx in range(args.pvs_per_buffer):
                with common.CA_LOCK:
                    epics.caget(common.pv_name(sig_idx, buf))
            print(f"-- round {round_num} done (buffer {buf}) --")
            if args.max_seconds is not None and time.monotonic() - start >= args.max_seconds:
                print(f"-- max_seconds ({args.max_seconds}) reached, stopping early --")
                break
            time.sleep(args.round_interval_s)
    finally:
        watcher.stop()

    print(
        f"Finished. Expected monitored_count to have plateaued at "
        f"{expected_plateau} and never decreased -- see {args.csv}."
    )


if __name__ == "__main__":
    main()
