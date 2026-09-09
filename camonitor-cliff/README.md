# `camonitor-cliff` — CA monitor-accumulation reproduction harness

Reproduces, in isolation, a real EPICS Channel Access bug found in a SLAC wire-scanner client
(`slac_timing.buffer.Buffer._fetch_single()`): `epics.caget()` silently creates a persistent CA
monitor on any PV under 65536 elements, and that monitor is never released. Read repeatedly
against a rotating pool of buffer-numbered history PVs (the real code's actual access pattern),
this leaves an ever-growing set of live monitors running — with real, measured costs: unbounded
server memory growth, and client-observed read latency that climbs past a 100 ms "cliff" and
stays there.

Everything here runs locally over loopback (`EPICS_CA_ADDR_LIST=127.0.0.1`, set in `common.py`
before `epics`/`caproto` are imported) — no real IOCs or network access needed. PV names use a
`CAGET_TEST:` prefix to avoid any collision with real facility PVs. pyepics may auto-spawn a
local `caRepeater` subprocess on first client use — expected and benign.

**For results and the current recommendation, see [`docs/camonitor-cliff/SUMMARY.md`](../docs/camonitor-cliff/SUMMARY.md)**
(short version) or that directory's per-comparison notebooks (full data, plots, and the exact
command that generated each). This file describes what each scenario/script does, not specific
numbers — those move fast enough across reruns that duplicating them here just goes stale.

## The bug, and four candidate fixes

- **`run_caget_bug.py`** (Scenario A) — the bug: `epics.caget()` per PV per round. Silently
  creates a persistent monitor every time, never released.
- **`run_fixed.py`** (Scenario B) — `epics.PV(pvname, auto_monitor=False)`, explicitly
  `.disconnect()`ed after each read. Eliminates the monitor and its server-side cost, but leaves
  the low-level CA channel open (never calls `clear_channel()`) — and, separately, leaves the
  last-read *value* cached client-side forever (`epics.ca._cache[ctx][pvname].get_results`,
  which `disconnect()` never touches).
- **`run_clear_channel.py`** (Scenario C) — B plus an explicit `epics.ca.clear_channel()` after
  `disconnect()`, so no channel is left behind either. Only safe to do unconditionally because
  pyepics shares one channel per `(pvname, context)` across every caller in a process — clearing
  one out from under some other, unrelated reference to the same PV name is a real risk in a
  bigger or longer-lived application.
- **`run_periodic_monitor.py`** (Scenario D) — the opposite trade: keep `caget()`'s default
  monitor on, but periodically clear the cache (once per scan) instead of disabling the monitor
  per-read.
- **`run_clear_value_cache.py`** (Scenario E) — B, plus clearing just the client-side cached
  *value* after each read (`entry.get_results.clear()`), leaving the channel/connection itself
  untouched. Narrower than C: no `clear_channel()` at all, so none of C's shared-channel risk.

## Two ways to run a comparison

**`run_comparison.py`** — small-scale (one mock IOC), A vs B vs C:
```sh
python3 run_comparison.py
```
Runs all three scenarios back-to-back, each against its own freshly-started server (killed and
restarted between scenarios), writes one CSV per scenario plus a merged `results.json`
(`{"A": [...], "B": [...], "C": [...]}`, same shape the published comparison artifact's chart
data uses), and prints a summary. Takes the same `--n-buffers`/`--pvs-per-buffer`/
`--round-interval-s`/`--n-rounds`/`--max-seconds`/`--out-dir` knobs as the individual scripts
(defaults: 150s wall-clock budget per scenario, writing to `docs/camonitor-cliff/run_comparison/`
at the repo root regardless of cwd) — see `--help`.

**`run_channel_cost_test.py`** — real scale (several mock IOCs spread across servers, since one
server alone degrades badly well past a few hundred PVs), any subset of B/C/D/E:
```sh
python3 run_channel_cost_test.py --scenarios B,D,E --out-dir ../docs/camonitor-cliff/channel_cost_bd
```
Defaults to 10 servers × 500 PVs = 5,000 channels per scenario, `--scenarios B,C` if not given.
Tracks server *and* client RSS, and per-round duration (`--extra-rounds` controls how far each
run continues past the buffer-pool rollover, where buffer numbers start repeating and a
never-cleared scenario starts hitting its own channel cache instead of paying for a fresh CA
connect).

Both orchestrators' output directories hold a `results.ipynb` that loads the CSVs and reproduces
the summary tables and plots without needing to regenerate the data — open it directly, or
refresh it against fresh data with `jupyter nbconvert --to notebook --execute --inplace
<dir>/results.ipynb`. Each notebook's first cell documents the exact command that generated its
own data.

To run one scenario by hand instead (e.g. while developing against a server you're watching
directly):
```sh
python3 ioc_server.py &
python3 run_caget_bug.py              # Scenario A: watch monitored_count climb and never drop
python3 run_fixed.py                  # Scenario B: watch it stay at zero
python3 run_clear_channel.py          # Scenario C: watch channels stay at zero too
python3 run_periodic_monitor.py       # Scenario D: monitor on, cleared once per scan
python3 run_clear_value_cache.py      # Scenario E: B + clear the client value cache
kill %1                               # stop the mock IOC
```
Run each scenario as a **separate process**, against a **freshly-restarted server** —
`epics.pv._PVcache_`/`epics.ca` state is process-global, so combining scenarios in one
interpreter (or reusing a server one scenario has already polluted) contaminates the comparison.
This is exactly what the two orchestrator scripts above automate.

Each client script prints a live status line and, when run with a `CacheWatcher` (i.e. without
`--no-watcher`), writes a CSV with columns: `elapsed_s, cached_pv_count, monitored_count,
active_count, channel_cache_count, open_fd_count, implied_bytes_per_sec, client_mem_rss_mb,
server_mem_rss_mb, latency_p50_ms, latency_p95_ms, latency_p99_ms`. Every script also takes
`--n-buffers`, `--pvs-per-buffer`, `--n-rounds`, `--round-interval-s`, and `--max-seconds` (stop
after a wall-clock budget regardless of `--n-rounds`) — see each script's `--help`.

## `ioc_server_pcaspy.py` — untested alternative server, known broken in one environment

A `pcaspy`-based version of the same mock IOC (`pcaspy` wraps the real reference `libcas` C
library, unlike `caproto`'s pure-Python reimplementation — useful for checking whether a
server-side finding is `caproto`-specific or general). **In the environment this was first built
in, it did not work at all**: even a plain `caget`/`camonitor` against it (using EPICS Base's own
command-line tools, not just pyepics) failed with a real server-side error (`"Channel read
request failed"` on get, `"Virtual circuit disconnect"` on monitor) — isolated to a
`pcaspy`/EPICS-Base version or build incompatibility in that sandbox, not a bug in the test
scripts. Untested elsewhere; worth trying in an environment with a clean, matched `pcaspy` +
EPICS Base install before relying on it.

## Attempting to reproduce the actual latency cliff — what worked

The original incident this reproduces measured canary-PV `pv.get()` round-trip latency
(p50/p95/p99) crossing ~100 ms after enough accumulated scans, on a real facility network with
real per-IOC resource limits. This harness measures the same thing (a dedicated
`CAGET_TEST:CANARY` PV, repeated timed `use_monitor=False` gets) — and at sufficient scale (500
PVs, sustained past the monitor-count plateau) it reproduced a real cliff locally: p50 crossed
100 ms well before the monitor count itself finished climbing, then settled into a sustained
plateau several times higher, with p95/p99 spiking well beyond that at times. The likely local
bottleneck is different from the original incident's (client-side CA event-processing load and/or
the mock server's own queue growth, rather than real multi-IOC network/connection exhaustion), but
the practical lesson — leaving monitors running forever eventually degrades read latency badly —
reproduces cleanly. See `docs/camonitor-cliff/run_comparison/results.ipynb` for the actual numbers
from the most recent run.

To reproduce this yourself, scale up and run past the plateau — the 150s default is already
enough to see the cliff clearly; `--max-seconds 400` pushes further past it:
```sh
python3 run_comparison.py --n-buffers 25 --pvs-per-buffer 20 --round-interval-s 1.0   # 150s default
python3 run_comparison.py --n-buffers 25 --pvs-per-buffer 20 --round-interval-s 1.0 --max-seconds 400
```
To run Scenario A alone against a server you're watching directly instead:
```sh
python3 ioc_server.py --n-buffers 25 --pvs-per-buffer 20 &
python3 run_caget_bug.py --n-buffers 25 --pvs-per-buffer 20 --round-interval-s 1.0 --max-seconds 150 --server-pid <pid>
```
