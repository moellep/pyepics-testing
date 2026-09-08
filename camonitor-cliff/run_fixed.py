#!/usr/bin/env python3
"""Scenario B: the fix -- explicit epics.PV(..., auto_monitor=False).

Same round structure as run_caget_bug.py, but reads each PV via a plain
epics.PV(pvname, auto_monitor=False) that is explicitly disconnected right
after the read -- the pattern proposed in claude/lazy-pv-clear-channel.md.
Since a bare PV(...) construction never registers in epics.pv._PVcache_
(only get_pv()/caget() do), and auto_monitor=False means pyepics never
subscribes, monitored_count and implied_bytes_per_sec should stay at zero
throughout, regardless of how many rounds run.

Run the mock IOC first: python3 ioc_server.py
"""

import argparse
import time

import common  # noqa: F401  (sets CA env vars before epics import)

import epics
import epics.ca
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
    parser.add_argument("--csv", default="results_fixed.csv")
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
        help="don't run CacheWatcher at all (no background thread in this process). "
             "A second thread touching CA state -- even properly CA_LOCK-synchronized --  "
             "was observed to cause multi-second stalls on the main thread's ordinary "
             "connects when reaching a new server for the first time in a multi-server "
             "EPICS_CA_ADDR_LIST setup (see run_channel_cost_test.py, which uses this flag "
             "and samples server RSS from the orchestrator process instead).",
    )
    args = parser.parse_args()

    print(
        f"Scenario B (fixed): {args.n_rounds} rounds (max {args.max_seconds}s), "
        f"{args.n_buffers} buffers x {args.pvs_per_buffer} signals "
        f"(expected monitored_count to stay at 0), "
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
            round_start = time.monotonic()
            for sig_idx in range(args.pvs_per_buffer):
                with common.CA_LOCK:
                    pv = epics.PV(common.pv_name(sig_idx, buf), auto_monitor=False)
                    chid_before = pv.chid
                    pv.get(use_monitor=False, timeout=5.0)
                    pv.disconnect()
                    if round_num == 0 and sig_idx == 0:
                        print(
                            f"[diagnostic] {pv.pvname}: chid before disconnect() "
                            f"= {chid_before}, chid after disconnect() = {pv.chid} "
                            f"(non-None/non-zero here means the channel is still "
                            f"clearable via epics.ca.clear_channel())"
                        )
            round_dur = time.monotonic() - round_start
            print(f"-- round {round_num} done (buffer {buf}) elapsed={time.monotonic()-start:.1f}s "
                  f"round_dur_s={round_dur:.3f} --")
            if args.max_seconds is not None and time.monotonic() - start >= args.max_seconds:
                print(f"-- max_seconds ({args.max_seconds}) reached, stopping early --")
                break
            time.sleep(args.round_interval_s)
    finally:
        if watcher is not None:
            watcher.stop()

    print(
        f"Finished. Expected monitored_count and implied_bytes_per_sec to "
        f"have stayed at 0 throughout -- see {args.csv}."
    )

    # Read epics.ca._cache from the SAME thread (main) that actually created
    # the channels via PV(...) above -- current_context() is thread-scoped,
    # so reading this from a different thread (e.g. the watcher) would show
    # a different, unrelated view.
    with common.CA_LOCK:
        ctx = epics.ca.current_context()
        cache = dict(epics.ca._cache.get(ctx, {}))
        print(
            f"[diagnostic] main-thread epics.ca._cache: {len(cache)} channel "
            f"entries remain for {args.n_buffers * args.pvs_per_buffer} distinct "
            f"PVs touched, despite calling pv.disconnect() after every read"
        )
        if cache:
            name, entry = next(iter(cache.items()))
            print(
                f"[diagnostic] clearing one via epics.ca.clear_channel(): "
                f"{name}, chid={entry.chid}"
            )
            epics.ca.clear_channel(entry.chid)
            after = len(epics.ca._cache.get(ctx, {}))
            print(
                f"[diagnostic] after clear_channel(): {after} entries remain "
                f"(was {len(cache)}) -- confirms the channel was real and clearable"
            )


if __name__ == "__main__":
    main()
