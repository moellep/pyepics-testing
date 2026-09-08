#!/usr/bin/env python3
"""Scenario D: leave the monitor on (plain epics.caget()), clear the scan's
own channels right after -- instead of disabling the monitor per-read
(Scenario B) or disabling it AND clear_channel()ing per-read (Scenario C).

Same read mechanism as run_caget_bug.py (Scenario A) -- a plain
epics.caget(pvname) with no auto_monitor=False, so pyepics' default
monitor subscription is established exactly like today's real, pre-fix
slac_timing.buffer.Buffer._fetch_single(). The difference from A: after
finishing a "scan" (this round's pvs_per_buffer signals for the current
buffer), immediately clear just that buffer's own cached channels --
the same *logic* as buffer.py's existing _clear_ca_cache(), scoped by the
same f"HST{buffer_num}" suffix convention common.pv_name() already uses.

Ported without _clear_ca_cache()'s two _PVcache_/context_cache .pop()
calls: confirmed by reading pyepics' own source this session that
PV.disconnect() (epics/pv.py) and epics.ca.clear_channel() (epics/ca.py)
already remove their own cache entries internally, so those pops are
no-ops by the time they run -- this is the same cleanup, not a
simplified/different one.

The question this scenario exists to answer: does leaving the monitor on
(so a value may already be pushed/cached before a read) buy back any of
the round-latency cost of establishing a fresh subscription every time
the cache is cleared -- i.e. does periodic clearing offer a performance
advantage over Scenario B's never-clear/never-monitor approach, not just
a possible memory one? See run_channel_cost_test.py's round_dur_s
tracking, added alongside this scenario for exactly that comparison.

Run the mock IOC first: python3 ioc_server.py
"""

import argparse
import time

import common  # noqa: F401  (sets CA env vars before epics import)

import epics
import epics.ca
from epics.pv import _PVcache_
from monitor_watcher import CacheWatcher


def _main_thread_channel_count() -> int:
    ctx = epics.ca.current_context()
    return len(epics.ca._cache.get(ctx, {})) if ctx else 0


def _clear_scan_cache(buf: int) -> None:
    """Clear every cached PV object / channel belonging to this buffer's
    own HST{buf}-suffixed names -- same logic as buffer.py's
    _clear_ca_cache(), without its two redundant .pop() calls (see module
    docstring)."""
    suffix = f"HST{buf}"

    for pvid in [k for k in list(_PVcache_) if k[0].endswith(suffix)]:
        pv_obj = _PVcache_.get(pvid)
        if pv_obj is not None:
            pv_obj.disconnect()

    ctx = epics.ca.current_context()
    context_cache = epics.ca._cache.get(ctx, {})
    for name in [n for n in list(context_cache) if n.endswith(suffix)]:
        entry = context_cache.get(name)
        if entry is not None and getattr(entry, "chid", None) is not None:
            epics.ca.clear_channel(entry.chid)


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
    parser.add_argument("--csv", default="results_periodic_monitor.csv")
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
        f"Scenario D (monitor on + periodic clear): {args.n_rounds} rounds "
        f"(max {args.max_seconds}s), {args.n_buffers} buffers x "
        f"{args.pvs_per_buffer} signals (expected monitored_count AND "
        f"channel_count to stay near 0, same as C, but via caget()'s "
        f"default monitor + scoped clear rather than auto_monitor=False), "
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
                    epics.caget(common.pv_name(sig_idx, buf))
            with common.CA_LOCK:
                _clear_scan_cache(buf)
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
        f"{args.n_buffers * args.pvs_per_buffer} distinct PVs -- see {args.csv}."
    )


if __name__ == "__main__":
    main()
