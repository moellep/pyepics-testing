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

## The three scenarios

- **`run_caget_bug.py`** (Scenario A) — the bug: `epics.caget()` per PV per round.
- **`run_fixed.py`** (Scenario B) — the fix: `epics.PV(pvname, auto_monitor=False)`, explicitly
  `.disconnect()`ed after each read. Eliminates the monitor and its costs, but leaves the
  low-level CA channel open (never calls `clear_channel()`).
- **`run_clear_channel.py`** (Scenario C) — B plus an explicit
  `epics.ca.clear_channel()` after `disconnect()`, so no channel is left behind either. Only
  safe to do unconditionally because pyepics shares one channel per `(pvname, context)` across
  every caller in a process — clearing one out from under some other, unrelated reference to the
  same PV name is a real risk in a bigger or longer-lived application. See "Which scenario for a
  real client?" below.

## Quick run

```sh
python3 run_comparison.py
```

Runs all three scenarios back-to-back, each against its own freshly-started server (killed and
restarted between scenarios), writes one CSV per scenario plus a merged `results.json`
(`{"A": [...], "B": [...], "C": [...]}`, same shape the published comparison artifact's chart
data uses), and prints a summary. Takes the same `--n-buffers`/`--pvs-per-buffer`/
`--round-interval-s`/`--n-rounds`/`--max-seconds`/`--out-dir` knobs as the individual scripts
(defaults: 150s wall-clock budget per scenario — the cliff is visible well before 400s, see
below — writing to `docs/camonitor-cliff/run_comparison/` at the repo root regardless of cwd) —
see `--help`.

`docs/camonitor-cliff/run_comparison/results.ipynb` loads that directory's three `results_*.csv`
and reproduces the summary table and comparison plots (monitored-count, server RSS, client RSS,
canary latency) without needing to regenerate the data — open it directly, or re-run and refresh
it against a fresh `run_comparison.py` output with `jupyter nbconvert --to notebook --execute
--inplace docs/camonitor-cliff/run_comparison/results.ipynb`. See also
`docs/camonitor-cliff/channel_cost/results.ipynb` (B vs C server-memory cost at 5,000-channel
scale) and `docs/camonitor-cliff/channel_cost_bd/results.ipynb` (B vs D, whether leaving the
monitor on with periodic clearing offers a performance advantage — it doesn't).

To run one scenario by hand instead (e.g. while developing against a server you're watching
directly):
```sh
python3 ioc_server.py &
python3 run_caget_bug.py        # Scenario A: watch monitored_count climb and never drop
python3 run_fixed.py            # Scenario B: watch it stay at zero
python3 run_clear_channel.py    # Scenario C: watch channels stay at zero too
kill %1                         # stop the mock IOC
```
Run each scenario as a **separate process**, against a **freshly-restarted server** —
`epics.pv._PVcache_`/`epics.ca` state is process-global, so combining scenarios in one
interpreter (or reusing a server one scenario has already polluted) contaminates the comparison.
This is exactly what `run_comparison.py` automates.

Each client script prints a live status line and writes a CSV with columns:
`elapsed_s, cached_pv_count, monitored_count, active_count, channel_cache_count, open_fd_count,
implied_bytes_per_sec, client_mem_rss_mb, server_mem_rss_mb, latency_p50_ms, latency_p95_ms,
latency_p99_ms`. Pass `--server-pid <pid>` (printed in `ioc_server.py`'s startup message) to also
track the server's own memory. Every script also takes `--n-buffers`, `--pvs-per-buffer`,
`--n-rounds`, `--round-interval-s`, and `--max-seconds` (stop after a wall-clock budget
regardless of `--n-rounds`) — see each script's `--help`.

## What was actually measured

At 500 PVs (25 buffer numbers × 20 signals, 2800-element float64 arrays, 13 Hz — matched to the
real HST buffer PVs this reproduces), 400 seconds, fresh server per scenario. (This specific run
predates the 150s default below — the cliff and the memory-growth trend are both already fully
established well before 400s, which is why the default budget was shortened afterward; these
numbers remain accurate, just from a longer run than you'd get out of the box now.)

| | A — `caget()` | B — `auto_monitor=False` | C — B + `clear_channel()` |
|---|---|---|---|
| Server RSS | 113 MB → **8.96 GB** | 113 → 114 MB (flat) | 113 → 114 MB (flat) |
| Client RSS | 38.5 → 64.5 MB | 38.6 → 52.2 MB | 38.6 → 40.5 MB |
| `monitored_count` | 500/500, never released | 0 | 0 |
| Canary p50 latency | 25 → **285 ms** (crosses a 100 ms "cliff") | 27-39 ms (flat) | 23-34 ms (flat) |
| Channels left open | n/a | 500, never cleared | 0, cleared every round |

`monitored_count` climbs as the rotating buffer-number pool gets touched, plateaus once every
buffer number has been used once, and in Scenario A **never decreases** afterward —
`implied_bytes_per_sec` stays pinned at its plateau value indefinitely even though every
simulated "scan" has long since finished. B and C never create a monitor in the first place.

The server-memory growth in A is driven by *elapsed time with monitors active*, not by PV count
— it kept climbing long after `monitored_count` itself plateaued, consistent with an unbounded
per-subscription outgoing queue on the (pure-Python, `caproto`) mock server. B's and C's leftover
resources (channels, in B's case) are bounded instead — by the size of the PV-name space
actually touched, not by runtime — so they plateau early and don't grow further no matter how
long the process keeps running.

## Which scenario for a real client?

For a long-running (multi-hour) production client, **B**, not C: B's residual cost (leftover
open channels) is small, bounded, and doesn't grow with runtime, while C's `clear_channel()`
call is only safe under a precondition (this PV name is touched nowhere else in the process) that
a long-lived, multi-subsystem application is more likely to quietly violate over time — and when
violated, `clear_channel()` doesn't fail gracefully, it can pull a channel out from under
whatever else was using it. C is a legitimate choice only if that exclusivity is formally
documented/enforced as an invariant.

## `ioc_server_pcaspy.py` — untested alternative server, known broken in one environment

A `pcaspy`-based version of the same mock IOC (`pcaspy` wraps the real reference `libcas` C
library, unlike `caproto`'s pure-Python reimplementation — useful for checking whether the
server-memory-growth finding above is `caproto`-specific or general). **In the environment this
was first built in, it did not work at all**: even a plain `caget`/`camonitor` against it (using
EPICS Base's own command-line tools, not just pyepics) failed with a real server-side error
(`"Channel read request failed"` on get, `"Virtual circuit disconnect"` on monitor) — isolated to
a `pcaspy`/EPICS-Base version or build incompatibility in that sandbox, not a bug in the test
scripts. Untested elsewhere; worth trying in an environment with a clean, matched `pcaspy` +
EPICS Base install before relying on it.

## Attempting to reproduce the actual latency cliff — what worked

The original incident this reproduces measured canary-PV `pv.get()` round-trip latency
(p50/p95/p99) crossing ~100 ms after enough accumulated scans, on a real facility network with
real per-IOC resource limits. This harness measures the same thing (a dedicated
`CAGET_TEST:CANARY` PV, repeated timed `use_monitor=False` gets) — and at sufficient scale (500
PVs, sustained past the monitor-count plateau) it reproduced a real cliff locally: p50 crossed
100 ms at just 140/500 monitors and climbed to a sustained ~255-285 ms plateau, with p95/p99
spiking as high as ~700-1000 ms at times. The likely local bottleneck is different from the
original incident's (client-side CA event-processing load and/or the mock server's own queue
growth, rather than real multi-IOC network/connection exhaustion), but the practical lesson —
leaving monitors running forever eventually degrades read latency badly — reproduces cleanly.

To reproduce this yourself, scale up and run past the plateau — the 150s default is already
enough to see the cliff clearly, `--max-seconds 400` reproduces the exact numbers in the table
above:
```sh
python3 run_comparison.py --n-buffers 25 --pvs-per-buffer 20 --round-interval-s 1.0   # 150s default
python3 run_comparison.py --n-buffers 25 --pvs-per-buffer 20 --round-interval-s 1.0 --max-seconds 400   # matches the table above exactly
```
To run Scenario A alone against a server you're watching directly instead:
```sh
python3 ioc_server.py --n-buffers 25 --pvs-per-buffer 20 &
python3 run_caget_bug.py --n-buffers 25 --pvs-per-buffer 20 --round-interval-s 1.0 --max-seconds 150 --server-pid <pid>
```
