#!/usr/bin/env python3
"""Scenario C: verify epics.caget_many() does NOT create persistent monitors.

Same round structure as run_caget_bug.py/run_fixed.py, but reads each
round's PVs via a single batched epics.caget_many() call -- what
slac_timing.buffer.Buffer._fetch_many() does today. caget_many()'s own
docstring says "this does not cache PV objects" -- it uses the raw
ca.create_channel(auto_cb=False, ...) / ca.get(wait=False) / ca.get_complete()
primitives directly, never epics.PV or a CA subscription (auto_cb=False
means it doesn't even use libca's default connection-callback, let alone
ca_create_subscription -- see claude/lazy-pv-clear-channel.md).

Verified two ways here, both empirically, not just by reading the source:
1. Server RSS: our proven, sensitive indicator of live monitors (Scenario A
   grew a fresh 73 MB server to 4.5 GB in ~130s; Scenario B, with no
   monitors, stayed flat at 73 MB). If caget_many() also stays flat, that's
   strong evidence it creates no monitors either.
2. Main-thread epics.ca._cache, checked the same way run_fixed.py's
   diagnostic does -- channels may still accumulate (create_channel() is
   used internally either way), but with no monitor/subscription on them.

Run the mock IOC first: python3 ioc_server.py
"""

import argparse
import time

import common  # noqa: F401  (sets CA env vars before epics import)

import epics
import epics.ca
from epics.pv import _PVcache_
from monitor_watcher import CacheWatcher


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-buffers", type=int, default=common.N_BUFFERS)
    parser.add_argument("--pvs-per-buffer", type=int, default=common.PVS_PER_BUFFER)
    parser.add_argument("--n-rounds", type=int, default=20)
    parser.add_argument("--round-interval-s", type=float, default=2.0)
    parser.add_argument("--csv", default="results_caget_many.csv")
    parser.add_argument(
        "--server-pids", type=int, nargs="+", default=None,
        help="PID(s) of ioc_server.py instance(s), to also track their summed RSS "
             "(each prints its PID in its startup message)",
    )
    args = parser.parse_args()

    print(
        f"Scenario C (caget_many): {args.n_rounds} rounds, "
        f"{args.n_buffers} buffers x {args.pvs_per_buffer} signals "
        f"(expected monitored_count and server RSS to stay flat), "
        f"{args.round_interval_s}s between rounds -> {args.csv}"
    )

    watcher = CacheWatcher(args.csv, server_pids=args.server_pids)
    watcher.start()

    try:
        for round_num in range(args.n_rounds):
            buf = round_num % args.n_buffers
            names = [common.pv_name(sig_idx, buf) for sig_idx in range(args.pvs_per_buffer)]
            with common.CA_LOCK:
                epics.caget_many(names)
            print(f"-- round {round_num} done (buffer {buf}) --")
            time.sleep(args.round_interval_s)
    finally:
        watcher.stop()

    print(
        f"Finished. Expected monitored_count/server RSS to have stayed flat "
        f"throughout -- see {args.csv}."
    )

    with common.CA_LOCK:
        pvcache_count = len(_PVcache_)
        ctx = epics.ca.current_context()
        cache = dict(epics.ca._cache.get(ctx, {}))
        print(
            f"[diagnostic] main-thread epics.pv._PVcache_: {pvcache_count} entries "
            f"(caget_many's own docstring: 'this does not cache PV objects')"
        )
        print(
            f"[diagnostic] main-thread epics.ca._cache: {len(cache)} channel "
            f"entries for {args.n_buffers * args.pvs_per_buffer} distinct PVs touched"
        )
        if cache:
            name, entry = next(iter(cache.items()))
            print(
                f"[diagnostic] one entry: {name}, chid={entry.chid}, "
                f"callbacks={getattr(entry, 'callbacks', 'N/A')} "
                f"(empty/no callbacks here means no subscription was ever attached)"
            )


if __name__ == "__main__":
    main()
