"""Snapshot-based introspection of a running pyepics process.

Answers, for any process that has imported ``epics``: which PVs has it
touched, which ones currently have a live CA monitor, how much memory the
pyepics-internal caches are holding, and how fast each monitored PV is
actually updating.

No monkeypatching, no heap scanning (no ``gc.get_objects()``): every fact
below comes from walking objects pyepics itself already keeps around, as a
normal side effect of creating a PV or a channel:

- ``epics.pv._PVcache_`` -- PV wrapper objects, but only ones created via
  ``get_pv()``/``caget()``.
- ``epics.ca._cache[ctx][pvname]`` (a ``_CacheItem``) -- one per channel,
  created for *any* PV, including a bare ``epics.PV()`` that never touches
  ``_PVcache_``. Holds ``.get_results`` (the value cached by an *explicit*
  ``.get()`` call, used here for memory sizing) and ``.callbacks``: the
  connection callbacks each ``PV.__init__`` registers on its own channel via
  ``ca.create_channel(pvname, callback=self.__on_connect)``. A bound
  method's ``.__self__`` is the instance it's bound to, so
  ``entry.callbacks[i].__self__`` recovers the PV object for channels
  ``_PVcache_`` never saw -- confirmed live against pyepics source
  (epics/ca.py's ``_CacheItem.callbacks``, epics/pv.py's ``PV.__init__``).
- ``pv_obj._args['value']`` -- the *other* half of memory sizing, and the
  one that matters for a monitored PV: ``epics.ca._onMonitorEvent()`` never
  touches ``_CacheItem.get_results`` at all (that's ``_onGetEvent()``,
  triggered only by an explicit ``.get()``) -- it hands the unpacked value
  straight to the PV's own callback, which stores it here. A pure-monitor
  PV's ``get_results`` holds at most a tiny leftover from a connection-time
  probe, never the real (possibly large, e.g. a camera image) monitored
  value -- confirmed directly against epics/ca.py's ``_onMonitorEvent()``
  and epics/pv.py's value-storing callback.

Only a pvname with a channel but no PV object found by either means has
genuinely unknowable monitor status -- reported as ``None``, not guessed.
"""

import ctypes
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PVSnapshot:
    pvname: str
    has_pv_object: bool
    # None means "unknown: channel only, no live PV object found".
    monitored: Optional[bool]
    auto_monitor: Optional[bool]
    connected: Optional[bool]
    cache_bytes: int
    update_hz: Optional[float]


@dataclass
class Snapshot:
    pvs: dict
    total_cache_bytes: int
    taken_at: float = field(default_factory=time.monotonic)


def _find_pv_object(epics_module, entry):
    """Recover the PV wrapper for a channel _PVcache_ never registered,
    by walking the connection callbacks pyepics itself put on entry.callbacks.

    If more than one independent PV object shares this channel (two separate
    epics.PV(same_name) calls in the same process), only one is returned as
    a representative -- good enough for reporting per-pvname, not per
    PV-instance, monitor status.
    """
    for cb in getattr(entry, "callbacks", []):
        owner = getattr(cb, "__self__", None)
        if isinstance(owner, epics_module.PV):
            return owner
    return None


def _entry_cache_bytes(entry) -> int:
    """Size, in bytes, of the values epics.ca._CacheItem.get_results is
    holding for this channel.

    Each holder[0] is what epics.ca._onGetEvent() stored: normally a
    [meta_or_None, ctypes_array] pair from dbr.cast_args() (confirmed
    directly against epics/dbr.py) -- ctypes.sizeof() on that array element
    gives the real wire-format byte count. Anything else found there (the
    GET_PENDING sentinel while a get is in flight, or a
    ChannelAccessGetFailure on error) isn't ctypes-sizeable, so it falls
    back to nbytes/sys.getsizeof of whatever's actually there.
    """
    results = getattr(entry, "get_results", None)
    if not results:
        return 0
    total = 0
    for _ftype, holder in results.items():
        value = holder[0] if holder else None
        if value is None:
            continue
        payload = value[1] if isinstance(value, (list, tuple)) and len(value) == 2 else value
        if payload is None:
            continue
        try:
            total += ctypes.sizeof(payload)
        except TypeError:
            nbytes = getattr(payload, "nbytes", None)
            total += nbytes if nbytes is not None else sys.getsizeof(payload)
    return total


def _pv_value_bytes(pv_obj) -> int:
    """Size, in bytes, of the value a live PV object is holding right now
    (pv_obj._args['value']) -- see the module docstring: this is what a
    *monitored* PV actually updates on every push, never
    _CacheItem.get_results. as_numpy defaults to True in pyepics, so for
    array/waveform data this is normally a real numpy array with .nbytes;
    sys.getsizeof() covers anything else (a scalar, a string).
    """
    args = getattr(pv_obj, "_args", None)
    if not args:
        return 0
    value = args.get("value")
    if value is None:
        return 0
    nbytes = getattr(value, "nbytes", None)
    return nbytes if nbytes is not None else sys.getsizeof(value)


class PVTracker:
    """start()/stop() run a background poll loop (matching the
    camonitor-cliff CacheWatcher's 1s-interval pattern); snapshot() can also
    just be called directly, any time, from whichever thread already owns
    CA access in the process under test.

    This tool can't assume the app it's pointed at serializes its own CA
    calls the way camonitor-cliff/common.py's CA_LOCK does -- if it does,
    call snapshot() from a thread already holding that lock rather than via
    start()'s own background thread, to avoid the cross-thread pyepics/libca
    race camonitor-cliff's CacheWatcher documents in detail.

    Every monitored PV snapshot() discovers gets watch()ed automatically, so
    update_hz fills in a couple of polls after a monitor starts, without the
    caller needing to know PV names ahead of time -- e.g. a monitor a user
    starts through some unrelated UI, after this tracker is already running.

    log_path, if given, gets one terse report.print_summary()-style line
    (see report.write_summary()) appended every log_interval_s -- separate
    from csv_path's full per-PV row-per-poll dump, for a lightweight
    "is anything going wrong" tail -f instead of a dataset to load later.
    log_interval_s defaults to sample_interval_s, and can't be finer-grained
    than it: a log line can only be written on a poll tick. The poll loop
    only ever writes to csv_path/log_path -- it never prints to stdout (a
    console script wanting that can call report.print_summary()/
    print_snapshot() itself, e.g. right after snapshot()).

    snapshot_interval_s, if given (requires log_path), additionally appends
    the full per-PV breakdown (report.write_snapshot(), the report.print_
    snapshot() table) to log_path on its own cadence -- independent of and
    usually much coarser than log_interval_s, since a full breakdown is a
    lot more log volume per write than one terse line.
    """

    def __init__(
        self,
        csv_path: str = None,
        sample_interval_s: float = 1.0,
        log_path: str = None,
        log_interval_s: float = None,
        snapshot_interval_s: float = None,
    ):
        self._csv_path = csv_path
        self._sample_interval_s = sample_interval_s
        self._log_path = log_path
        self._log_interval_s = (
            log_interval_s if log_interval_s is not None else sample_interval_s
        )
        self._last_log_at = None
        self._snapshot_interval_s = snapshot_interval_s
        self._last_snapshot_log_at = None
        self._lock = threading.Lock()
        self._counts: dict = {}
        self._last_rate_check: dict = {}
        self._watched: set = set()
        self._stop = threading.Event()
        self._thread = None
        self._t_start = time.monotonic()

    def watch(self, pv) -> None:
        """Attach a counting callback to pv so snapshot() can report its
        update rate precisely (a callback fire per update), rather than
        guessing from polling. snapshot() calls this automatically for
        every monitored PV it discovers (see the loop below) -- call it
        directly only to start counting before that PV's first snapshot()
        (so update_hz isn't 0.0 on the first sample that sees it). Either
        way it's a small, real intrusion: adds a callback to a PV object
        this tracker didn't create, alongside whatever callbacks it
        already has -- a cheap counter increment, not a change to any
        existing callback's behavior.
        """
        pvname = pv.pvname
        with self._lock:
            if pvname in self._watched:
                return
            self._watched.add(pvname)
            self._counts[pvname] = 0
            self._last_rate_check[pvname] = (0, time.monotonic())

        def _count(pvname=pvname, **kwargs):
            with self._lock:
                self._counts[pvname] = self._counts.get(pvname, 0) + 1

        pv.add_callback(_count)

    def _update_hz(self, pvname: str) -> Optional[float]:
        """Rate since the *last* call for this pvname (a windowed rate),
        not a lifetime average -- a lifetime average freezes its numerator
        (the count) once updates stop while its denominator (elapsed since
        watch() started) keeps growing on every later call, so it decays
        toward zero as ~1/t long after the monitor has gone quiet, instead
        of reflecting that. Confirmed directly: this was exactly why
        update_hz was seen decaying slowly rather than dropping once a
        monitor actually stopped.
        """
        if pvname not in self._watched:
            return None
        now = time.monotonic()
        with self._lock:
            count = self._counts.get(pvname, 0)
            prev_count, prev_time = self._last_rate_check[pvname]
            self._last_rate_check[pvname] = (count, now)
        elapsed = now - prev_time
        return (count - prev_count) / elapsed if elapsed > 0 else 0.0

    def snapshot(self) -> Snapshot:
        import epics
        import epics.ca
        from epics.pv import _PVcache_

        pvs = {}

        for pvid in list(_PVcache_):
            pv_obj = _PVcache_.get(pvid)
            if pv_obj is None:
                continue
            monitored = bool(pv_obj.auto_monitor) and pv_obj._monref is not None
            if monitored:
                self.watch(pv_obj)
            pvs[pv_obj.pvname] = PVSnapshot(
                pvname=pv_obj.pvname,
                has_pv_object=True,
                monitored=monitored,
                auto_monitor=bool(pv_obj.auto_monitor),
                connected=bool(pv_obj.connected),
                cache_bytes=_pv_value_bytes(pv_obj),
                update_hz=self._update_hz(pv_obj.pvname),
            )

        ctx = epics.ca.current_context()
        context_cache = epics.ca._cache.get(ctx, {}) if ctx else {}
        for pvname, entry in context_cache.items():
            # get_results bytes (explicit-.get() cache) -- added to, not
            # replacing, whatever _pv_value_bytes() already found for this
            # pvname above: they're two distinct caches (see module
            # docstring) and either, or both, can hold real data.
            get_bytes = _entry_cache_bytes(entry)
            existing = pvs.get(pvname)
            if existing is not None:
                existing.cache_bytes += get_bytes
                continue

            pv_obj = _find_pv_object(epics, entry)
            if pv_obj is not None:
                monitored = bool(pv_obj.auto_monitor) and pv_obj._monref is not None
                if monitored:
                    self.watch(pv_obj)
                pvs[pvname] = PVSnapshot(
                    pvname=pvname,
                    has_pv_object=True,
                    monitored=monitored,
                    auto_monitor=bool(pv_obj.auto_monitor),
                    connected=bool(pv_obj.connected),
                    cache_bytes=get_bytes + _pv_value_bytes(pv_obj),
                    update_hz=self._update_hz(pvname),
                )
            else:
                pvs[pvname] = PVSnapshot(
                    pvname=pvname,
                    has_pv_object=False,
                    monitored=None,
                    auto_monitor=None,
                    connected=bool(getattr(entry, "conn", False)),
                    cache_bytes=get_bytes,
                    update_hz=None,
                )

        total = sum(p.cache_bytes for p in pvs.values())
        return Snapshot(pvs=pvs, total_cache_bytes=total)

    def start(self) -> None:
        if self._csv_path:
            from pyepics_watcher import report

            report.write_header(self._csv_path)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def _run(self) -> None:
        # First CA call this thread makes -- best-effort attach, matching
        # camonitor-cliff's CacheWatcher._run(). No lock here: this tool
        # can't assume it shares one with the app under test (see class
        # docstring), so it can't fully close the same race that comment
        # documents -- only the caller, serializing against its own CA
        # calls, can.
        try:
            import epics.ca

            epics.ca.use_initial_context()
        except Exception as exc:
            if "already attached" not in str(exc):
                raise

        from pyepics_watcher import report

        while not self._stop.is_set():
            snap = self.snapshot()
            elapsed_s = time.monotonic() - self._t_start
            if self._csv_path:
                report.append_rows(self._csv_path, snap, elapsed_s)
            if self._log_path and (
                self._last_log_at is None
                or elapsed_s - self._last_log_at >= self._log_interval_s
            ):
                report.write_summary(self._log_path, snap, elapsed_s)
                self._last_log_at = elapsed_s
            if self._log_path and self._snapshot_interval_s and (
                self._last_snapshot_log_at is None
                or elapsed_s - self._last_snapshot_log_at >= self._snapshot_interval_s
            ):
                report.write_snapshot(self._log_path, snap, elapsed_s)
                self._last_snapshot_log_at = elapsed_s
            self._stop.wait(self._sample_interval_s)
