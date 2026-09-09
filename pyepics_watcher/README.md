# `pyepics_watcher` — general-purpose pyepics process introspection

Answers, for any process that has `import epics`'d: which PVs has it touched, which ones
currently have a live CA monitor, how much memory pyepics' internal caches are holding, and how
fast each monitored PV is actually updating. Everything `camonitor-cliff/` diagnosed by hand, each
time, with a purpose-built script — this turns "diagnose this by hand again" into "run the tool."

Standalone: unlike `camonitor-cliff/`, this doesn't assume anything about how the process under
test set up CA (no shared `CA_LOCK`, no required env vars) — `PVTracker.snapshot()` works against
any running pyepics process, called from whichever thread already owns CA access there.

## How it works

No monkeypatching, no heap scanning (no `gc.get_objects()`) — every fact comes from walking
objects pyepics itself already maintains, as a normal side effect of creating a PV or a channel:

- `epics.pv._PVcache_` — PV wrapper objects, but only ones created via `get_pv()`/`caget()`.
- `epics.ca._cache[ctx][pvname]` (a `_CacheItem`) — one per channel, created for *any* PV,
  including a bare `epics.PV()` that never touches `_PVcache_`. Its `.get_results` gives cache
  memory sizing for whatever an *explicit* `.get()` cached, even for a channel whose PV wrapper is
  long gone; its `.callbacks` list holds each `PV.__init__`'s own bound `__on_connect` connection
  callback — `entry.callbacks[i].__self__` recovers the PV object for channels `_PVcache_` never
  saw (a bare `epics.PV(auto_monitor=True)` call), which is how monitor status is found for that
  case too.
- `pv_obj._args['value']` — the other half of cache-memory sizing, and the half that matters for a
  *monitored* PV: a monitor push (`epics.ca._onMonitorEvent()`) never touches
  `_CacheItem.get_results` at all — only an explicit `.get()` does. It stores straight into the PV
  object's own `_args['value']` instead. A pure-monitor PV (never explicitly `.get()`'d) would read
  as nearly empty from `get_results` alone — confirmed this is exactly what was happening for a
  monitored image PV before this was added; `cache_bytes` now adds both sources.

Only a pvname with a channel but no PV object recoverable by either means has genuinely
unknowable monitor status — reported as `None`/`"unknown"`, not guessed.

See `tracker.py`'s module docstring and `_find_pv_object()`/`_entry_cache_bytes()`/
`_pv_value_bytes()` for the exact mechanics, confirmed directly against this environment's pyepics
source (`epics/ca.py`, `epics/dbr.py`, `epics/pv.py`).

## Files

- `tracker.py` — `PVTracker`: `snapshot()` (call any time, from any thread), `watch(pv)` (attach a
  counting callback for a precise update-rate measurement), `start()`/`stop()` (optional
  background poll loop, matching `camonitor-cliff`'s `CacheWatcher` cadence) with two independent
  optional outputs: `csv_path` (full per-PV row every poll) and `log_path`/`log_interval_s` (one
  terse summary line appended every `log_interval_s`, default same as the poll interval).
- `report.py` — table printing and CSV export: `print_snapshot()` (full per-PV breakdown),
  `print_summary()` (one terse line of aggregate counts), and CSV helpers.
- `common.py` / `ioc_server.py` — a small local mock IOC for the demo below (not a dependency of
  `tracker.py` itself); runnable standalone as `pyepics-watcher-server`.
- `run_demo.py` — starts the mock IOC, creates one PV three different ways (`caget()`,
  `PV(auto_monitor=False)`, and a bare `PV(auto_monitor=True)` that never touches `_PVcache_`),
  and checks `snapshot()` tells all three apart correctly — including cache-memory sizing against
  the known array size, and update rate against the known scan rate.

## Install

From the repo root (this is a real, `pip install -e .`-able package):

```sh
pip install -e .            # just the tracker (only depends on pyepics)
pip install -e ".[demo]"    # + caproto/numpy, needed for the demo below
```

## Running the demo

```sh
pyepics-watcher-demo
```

(or `python -m pyepics_watcher.run_demo`; both are installed by `pip install -e ".[demo]"`)

Prints a snapshot table, then walks through each check, e.g.:

```
PASS: monitor status correct for all three creation styles
PASS: cache_bytes close to known array size (22408 bytes)
PASS: measured update rate close to known scan rate
PASS: after disconnect(), bare PV drops out of the monitored set (status: ...)

ALL CHECKS PASSED
```

## Using it against real code

After `pip install -e .`, import it like any installed package:

```python
from pyepics_watcher import tracker, report

t = tracker.PVTracker()
# ... exercise the code under test ...
snap = t.snapshot()
report.print_summary(snap)   # one terse line of aggregate counts
report.print_snapshot(snap)  # full per-PV breakdown
```

`snapshot()` calls `t.watch(pv)` automatically for every monitored PV it discovers, so `update_hz`
fills in a couple of polls after any monitor starts — including one some other, unrelated part of
the process started, with no way for this tool to have been told about it in advance. Call
`watch(pv)` yourself only to start counting earlier (so the first snapshot() that sees a PV
doesn't read update_hz=0.0). Either way it attaches a real callback to the PV object (a small,
real intrusion: a cheap counter increment, not a change to any existing callback).

`update_hz` is a **windowed** rate: callback count since the *previous* `snapshot()` call for that
pvname, divided by the time since that previous call — not a lifetime average. It reflects
current activity, so it drops to ~0 within a poll or two of a monitor actually going quiet,
instead of decaying slowly (a lifetime-average's frozen numerator over an ever-growing
denominator would keep reporting stale, decaying-toward-zero activity long after a monitor
stopped).

For a long-running process, `start()`/`stop()` run the poll loop in a background thread instead:

```python
t = tracker.PVTracker(
    csv_path="pv_monitor.csv",              # full per-PV row every poll
    log_path="pv_monitor.log",              # terse summary + periodic snapshot, appended
    sample_interval_s=1.0,                  # poll cadence
    log_interval_s=30.0,                    # summary-line cadence (>= sample_interval_s)
    snapshot_interval_s=300.0,              # full per-PV breakdown cadence (optional, into log_path)
)
t.start()
# ... app runs ...
t.stop()
```

`snapshot_interval_s` is independent of `log_interval_s` and usually much coarser — a full
per-PV breakdown is a lot more log volume per write than one terse line. Both write into the
same `log_path`, interleaved by whichever comes due; omit `snapshot_interval_s` to get only the
terse summary lines (the default).
