#!/usr/bin/env python3
"""Scenario E: B, plus clearing the client-side cached *value* after each
read -- without touching the channel itself.

B (run_fixed.py) leaves epics.ca._cache[ctx][pvname].get_results holding
the full last-read value (metadata + data array) forever, since
disconnect() never touches it -- confirmed directly this session by
inspecting a live _CacheItem after a B-style read. For a waveform PV this
is real, array-sized memory per distinct channel ever read (measured:
~24.5 KB/channel client-side at 5,000 channels in the B-vs-D comparison,
matching one 2800-element float64 array almost exactly).

Scenario C (run_clear_channel.py) avoids this by calling
epics.ca.clear_channel() -- but that tears down the whole low-level CA
channel, which is only safe because pyepics shares one channel per
(pvname, context) across every caller in a process (see
claude/lazy-pv-clear-channel.md's danger analysis): clearing it out from
under some other, unrelated reference to the same PV name is what
plausibly caused the "dropping the pv... may cause crashes" symptom in
the 2026-09-03 meeting notes.

E takes a narrower approach: clear only entry.get_results (the cached
*value*), leaving the channel/connection (chid) fully intact. Any other
caller sharing this channel loses nothing but a cached value they'd have
to re-fetch on their own next .get() anyway -- not the connection itself.
This should keep B's server-side profile (small, bounded channel cost,
same as B) while avoiding B's client-side value-cache growth, without
C's shared-channel risk.

Run the mock IOC first: python3 ioc_server.py
"""

import argparse
import time

import common  # noqa: F401  (sets CA env vars before epics import)

import epics
import epics.ca
from monitor_watcher import CacheWatcher


def _clear_value_cache(pvname: str) -> None:
    ctx = epics.ca.current_context()
    if ctx is None:
        return
    context_cache = epics.ca._cache.get(ctx)
    if context_cache is None:
        return
    entry = context_cache.get(pvname)
    if entry is not None:
        entry.get_results.clear()


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
    parser.add_argument("--csv", default="results_clear_value_cache.csv")
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
        f"Scenario E (auto_monitor=False + clear value cache): {args.n_rounds} rounds "
        f"(max {args.max_seconds}s), {args.n_buffers} buffers x {args.pvs_per_buffer} "
        f"signals (expected monitored_count to stay at 0, channel count to grow like B, "
        f"client value-cache to stay flat like C), "
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
                    pvname = common.pv_name(sig_idx, buf)
                    pv = epics.PV(pvname, auto_monitor=False)
                    pv.get(use_monitor=False, timeout=5.0)
                    pv.disconnect()
                    _clear_value_cache(pvname)
            with common.CA_LOCK:
                n = _main_thread_channel_count()
            round_dur = time.monotonic() - round_start
            print(f"-- round {round_num} done (buffer {buf}) elapsed={time.monotonic()-start:.1f}s "
                  f"round_dur_s={round_dur:.3f} -- main-thread channels={n} --")
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
        f"{args.n_buffers * args.pvs_per_buffer} distinct PVs (same as B -- channel "
        f"itself is never cleared) -- see {args.csv}."
    )


if __name__ == "__main__":
    main()
