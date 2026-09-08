"""Background sampler of pyepics' global PV cache, plus a canary latency probe.

Adapts the introspection technique already proven in
~/src/slaclab/slacwire/benchmarking/ca_degradation_benchmark.py:
- epics.pv._PVcache_ walked defensively via list(...) before iterating
- "active" (still receiving monitor pushes) detected via .timestamp changes,
  the same proxy as that script's _install_monitor_counter()/_reset_monitor_count()
- open file descriptor count via /proc/<pid>/fd, matching count_open_fds()
- explicit-get latency percentiles via repeated timed .get(use_monitor=False)
  calls, matching measure_caget_latency()
"""

import csv
import os
import platform
import threading
import time

import common  # noqa: F401  (sets CA env vars before epics import)

import epics
import epics.ca
import numpy as np
from epics.pv import _PVcache_

LATENCY_SAMPLES = 10


def count_open_fds() -> int:
    if platform.system() == "Darwin":
        try:
            return len(os.listdir("/dev/fd"))
        except OSError:
            return -1
    try:
        return len(os.listdir(f"/proc/{os.getpid()}/fd"))
    except OSError:
        return -1


def read_rss_mb(pid: int | None = None) -> float:
    """Current resident set size (RSS) of a process, in MB. Defaults to self.

    Reads /proc/<pid>/status VmRSS -- Linux only (matches count_open_fds()'s
    existing /proc dependency in this harness).
    """
    pid = pid or os.getpid()
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return -1.0


def measure_canary_latency_ms(pvname: str, n_samples: int = LATENCY_SAMPLES) -> dict:
    with common.CA_LOCK:
        pv = epics.PV(pvname, auto_monitor=False)
        if not pv.wait_for_connection(timeout=5.0):
            return {"p50": float("inf"), "p95": float("inf"), "p99": float("inf")}
        latencies = []
        for _ in range(n_samples):
            t0 = time.perf_counter()
            val = pv.get(use_monitor=False, timeout=5.0)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if val is not None:
                latencies.append(elapsed_ms)
        pv.disconnect()
    arr = np.array(latencies) if latencies else np.array([float("inf")])
    return {
        "p50": float(np.percentile(arr, 50)),
        "p95": float(np.percentile(arr, 95)),
        "p99": float(np.percentile(arr, 99)),
    }


class CacheWatcher:
    """Samples the pyepics global cache + canary latency on a background thread."""

    CSV_COLUMNS = [
        "elapsed_s",
        "cached_pv_count",
        "monitored_count",
        "active_count",
        "channel_cache_count",
        "open_fd_count",
        "implied_bytes_per_sec",
        "client_mem_rss_mb",
        "server_mem_rss_mb",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
    ]

    def __init__(
        self,
        csv_path: str,
        sample_interval_s: float = 1.0,
        latency_every_n_samples: int = 5,
        probe_latency: bool = True,
        canary_pv: str = common.CANARY_NAME,
        array_len: int = common.ARRAY_LEN,
        update_hz: int = common.UPDATE_HZ,
        server_pids: int | list[int] | None = None,
    ):
        """probe_latency=False skips the canary-latency probe entirely --
        needed when the canary PV isn't reachable from every address a
        multi-server client is configured to search (see
        run_channel_cost_test.py): measure_canary_latency_ms() holds
        CA_LOCK for its whole multi-sample duration, and a slow/ambiguous
        search for a name that only one of several addresses actually has
        can block ALL other CA activity (including unrelated PVs on other
        servers) in the same process for that entire time -- confirmed
        directly, this is what caused multi-second stalls on ordinary
        buffer-PV reads in an earlier version of that test.
        """
        self._csv_path = csv_path
        self._sample_interval_s = sample_interval_s
        self._latency_every_n = latency_every_n_samples
        self._probe_latency = probe_latency
        self._canary_pv = canary_pv
        self._array_len = array_len
        self._update_hz = update_hz
        if isinstance(server_pids, int):
            server_pids = [server_pids]
        self._server_pids = server_pids or []
        self._last_timestamps: dict[str, float] = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._t_start = time.monotonic()
        self._tick = 0

    def start(self) -> None:
        with open(self._csv_path, "w", newline="") as f:
            csv.writer(f).writerow(self.CSV_COLUMNS)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5.0)

    def _sample_cache(self) -> dict:
        # _PVcache_ and epics.ca._cache are plain, mutable, unsynchronized
        # Python dicts shared with the main thread's real CA I/O (inside
        # ca_pend_io(), etc.) -- reading them from this thread without
        # holding the same lock the main thread uses for its own CA calls
        # is a genuine race, confirmed directly: it produced both random
        # ctypes.ArgumentError crashes (seen sporadically all session) and,
        # in the multi-server channel-cost test, multi-second stalls on
        # the MAIN thread's unrelated connects. CA_LOCK here closes that
        # race the same way it already protects every other CA call in
        # this harness.
        with common.CA_LOCK:
            cached = 0
            monitored = 0
            active = 0
            for pvid in list(_PVcache_):
                pv_obj = _PVcache_.get(pvid)
                if pv_obj is None:
                    continue
                cached += 1
                if getattr(pv_obj, "auto_monitor", False):
                    monitored += 1
                ts = getattr(pv_obj, "timestamp", None)
                if ts != self._last_timestamps.get(pvid[0]):
                    active += 1
                    self._last_timestamps[pvid[0]] = ts

            # Low-level channel cache (epics.ca._cache) -- distinct from
            # _PVcache_ above, which only tracks PV *wrapper objects*. A raw
            # PV(pvname) (used by run_fixed.py, not going through get_pv()/
            # caget()) never enters _PVcache_, but PV.__init__ always calls
            # ca.create_channel(), which does register here -- so this is the
            # count of live low-level CA channels regardless of whether any
            # wrapper object or monitor exists for them.
            ctx = epics.ca.current_context()
            channel_cache_count = len(epics.ca._cache.get(ctx, {})) if ctx else 0

        return {
            "cached_pv_count": cached,
            "monitored_count": monitored,
            "active_count": active,
            "channel_cache_count": channel_cache_count,
            "open_fd_count": count_open_fds(),
            "implied_bytes_per_sec": active * self._array_len * 8 * self._update_hz,
        }

    def _run(self) -> None:
        # pyepics requires each thread that makes CA calls to explicitly
        # attach to the CA context first (see epics.ca.use_initial_context's
        # own docstring) -- without this, concurrent CA calls from the main
        # thread and this thread can corrupt libca's ctypes-level state.
        # Observed intermittently: libca sometimes auto-attaches a brand-new
        # thread to the initial context before this call runs (a mismatch
        # between pyepics' Python-side current_context() bookkeeping and
        # libca's actual per-thread attachment state), in which case this
        # raises "Thread is already attached to a client context" -- which
        # means the desired state (attached to the right context) already
        # holds, so it's safe to treat as success rather than a real failure.
        #
        # This call must hold CA_LOCK. use_initial_context() is @withCA
        # -decorated, meaning it goes through epics.ca's lazy
        # `if libca is None: initialize_libca()` check -- exactly like every
        # other CA call in this process. Without the lock, this is the
        # *first* CA call this thread makes, so it can race with the main
        # thread's own first CA call (also potentially hitting `libca is
        # None` at the same moment) to both call initialize_libca()
        # concurrently. Confirmed as the actual cause of an intermittent
        # ctypes.ArgumentError ("Don't know how to convert parameter 1")
        # out of libca.ca_pend_io(): caught the real timeout argument at the
        # crash site across dozens of trials and it was always a perfectly
        # valid float (1.0) -- the corruption was in ca_pend_io's ctypes
        # argtypes signature itself (from the concurrent re-init), not in
        # any argument value. Reproduced 3/40 fresh-process trials without
        # this lock, 0/40 with it (same server, same round structure, each
        # trial a genuinely fresh process -- the race window is
        # process-lifetime-once, at first CA library initialization, so
        # reusing one process across many trials never re-exercises it).
        with common.CA_LOCK:
            try:
                epics.ca.use_initial_context()
            except epics.ca.CASeverityException as e:
                if "already attached" not in str(e):
                    raise
        while not self._stop.is_set():
            row = self._sample_cache()
            row["elapsed_s"] = round(time.monotonic() - self._t_start, 1)
            row["client_mem_rss_mb"] = round(read_rss_mb(), 2)
            row["server_mem_rss_mb"] = (
                round(sum(read_rss_mb(pid) for pid in self._server_pids), 2)
                if self._server_pids else ""
            )

            if self._probe_latency and self._tick % self._latency_every_n == 0:
                lat = measure_canary_latency_ms(self._canary_pv)
                row["latency_p50_ms"] = round(lat["p50"], 2)
                row["latency_p95_ms"] = round(lat["p95"], 2)
                row["latency_p99_ms"] = round(lat["p99"], 2)
            else:
                row["latency_p50_ms"] = ""
                row["latency_p95_ms"] = ""
                row["latency_p99_ms"] = ""

            with open(self._csv_path, "a", newline="") as f:
                csv.writer(f).writerow([row[c] for c in self.CSV_COLUMNS])

            print(
                f"[{row['elapsed_s']:>7.1f}s] cached={row['cached_pv_count']:>4} "
                f"monitored={row['monitored_count']:>4} active={row['active_count']:>4} "
                f"channels={row['channel_cache_count']:>4} "
                f"fds={row['open_fd_count']:>4} "
                f"implied={row['implied_bytes_per_sec']/1e6:>7.2f} MB/s "
                f"client_rss={row['client_mem_rss_mb']:>7.2f}MB "
                f"server_rss={row['server_mem_rss_mb']} "
                f"latency_p50={row['latency_p50_ms']}"
            )

            self._tick += 1
            self._stop.wait(self._sample_interval_s)
