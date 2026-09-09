#!/usr/bin/env python3
"""Demo + verification for pyepics_watcher.tracker.PVTracker.

Starts the mock IOC (ioc_server.py), then creates the same PV three
different ways real code does, and confirms snapshot() tells them apart
correctly:

1. epics.caget() -- monitor, found via _PVcache_.
2. epics.PV(auto_monitor=False).get() -- channel-only, no monitor.
3. A bare epics.PV(auto_monitor=True), never passed to get_pv()/caget() --
   monitor, invisible to _PVcache_, found only via the
   entry.callbacks[i].__self__ check in tracker.snapshot().

Then: disconnects PV 3 and confirms it drops out of the monitored set
(tracker tracks channel/monitor lifetime, not Python object lifetime);
checks cache_bytes against the known array size; and checks update_hz
against the known scan rate.

Run: pip install -e ".[demo]" (from the repo root), then pyepics-watcher-demo
"""

import subprocess
import sys
import time

from pyepics_watcher import common  # sets CA env vars before epics import
from pyepics_watcher import report
from pyepics_watcher import tracker

import epics


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    sys.exit(1)


def main() -> None:
    server = subprocess.Popen([sys.executable, "-m", "pyepics_watcher.ioc_server"])
    try:
        time.sleep(1.5)  # let caproto come up

        # 1. epics.caget() -- creates & caches a PV wrapper with a monitor.
        val = epics.caget(common.CAGET_PV, timeout=5.0)
        if val is None:
            _fail(f"couldn't read {common.CAGET_PV} via caget()")
        pv_caget = epics.get_pv(common.CAGET_PV)

        # 2. epics.PV(auto_monitor=False) -- channel-only, no monitor.
        pv_nomon = epics.PV(common.NOMON_PV, auto_monitor=False)
        data = pv_nomon.get(use_monitor=False, timeout=5.0)
        if data is None:
            _fail(f"couldn't read {common.NOMON_PV}")

        # 3. A bare, still-connected auto_monitor=True PV that never goes
        # through get_pv()/caget() -- the case _PVcache_ alone would miss.
        pv_baremon = epics.PV(common.BAREMON_PV, auto_monitor=True)
        if not pv_baremon.wait_for_connection(timeout=5.0):
            _fail(f"couldn't connect {common.BAREMON_PV}")
        time.sleep(0.5)  # let the first monitor push land

        t = tracker.PVTracker()
        t.watch(pv_caget)
        t.watch(pv_baremon)

        print("--- snapshot right after setup ---")
        snap = t.snapshot()
        report.print_summary(snap)
        report.print_snapshot(snap)

        # Step 1: monitor detection for all three creation styles.
        if snap.pvs[common.CAGET_PV].monitored is not True:
            _fail("caget() PV not reported monitored=True")
        if snap.pvs[common.NOMON_PV].monitored is not False:
            _fail("auto_monitor=False PV not reported monitored=False")
        if snap.pvs[common.BAREMON_PV].monitored is not True:
            _fail(
                "bare auto_monitor=True PV not reported monitored=True -- "
                "entry.callbacks[i].__self__ check failed to find it"
            )
        print("PASS: monitor status correct for all three creation styles")

        # Step 3: cache memory sizing against the known array size. Checked
        # on BAREMON_PV (monitor only, never explicitly .get()'d) -- this is
        # exactly the case that used to read ~0: a monitor push never
        # touches _CacheItem.get_results (only .get() does), so that alone
        # would show only a tiny connection-time-probe leftover, not the
        # real (possibly large) monitored value. CAGET_PV isn't used for
        # this check: caget() both .get()s AND auto-enables a monitor, so it
        # legitimately holds two distinct real copies (~2x this size), not
        # a bug -- see the module docstring.
        expected_bytes = common.ARRAY_LEN * 8  # float64
        actual_bytes = snap.pvs[common.BAREMON_PV].cache_bytes
        if abs(actual_bytes - expected_bytes) > 64:
            _fail(f"cache_bytes={actual_bytes}, expected close to {expected_bytes}")
        print(f"PASS: cache_bytes close to known array size ({actual_bytes} bytes)")

        # Step 4: update frequency against the known scan rate.
        print("watching for ~3s to measure update_hz...")
        time.sleep(3.0)
        snap = t.snapshot()
        hz = snap.pvs[common.CAGET_PV].update_hz
        print(f"measured update_hz={hz:.2f} (server scans at {common.UPDATE_HZ} Hz)")
        if hz is None or abs(hz - common.UPDATE_HZ) > 0.3 * common.UPDATE_HZ:
            _fail(f"update_hz={hz}, expected close to {common.UPDATE_HZ}")
        print("PASS: measured update rate close to known scan rate")

        # Step 2: channel/monitor lifetime, not Python object lifetime.
        pv_baremon.disconnect()
        snap = t.snapshot()
        status = snap.pvs.get(common.BAREMON_PV)
        if status is not None and status.monitored is True:
            _fail("bare PV still reported monitored=True after disconnect()")
        print(
            "PASS: after disconnect(), bare PV drops out of the monitored set "
            f"(status: {status})"
        )

        print("\nALL CHECKS PASSED")
    finally:
        server.terminate()
        server.wait(timeout=5.0)


if __name__ == "__main__":
    main()
