# camonitor-cliff — summary of all three test runs

Three rounds of testing, each comparing candidate fixes for the real bug this harness reproduces:
`epics.caget()` silently creates a persistent CA monitor per PV, never released, on a rotating
pool of buffer-numbered history PVs. Detailed data/notebooks for each round live in this
directory's subfolders; this is the short version.

## 1. `run_comparison/` — A vs B vs C (500 PVs, single server, ~150s)

| Scenario | What it does | Result |
|---|---|---|
| **A** | `epics.caget()` (today's bug) | Monitor count never drops; server RSS 113 MB → **3.7 GB**; canary latency climbs past a 100 ms "cliff" |
| **B** | `epics.PV(pvname, auto_monitor=False)` + `.disconnect()` | Monitor count stays 0; server RSS flat; latency flat |
| **C** | B + `epics.ca.clear_channel()` | Same as B, plus the leftover low-level channel is cleared too |

A confirms the bug. B and C both fully fix it — the only difference is whether the now-idle CA
channel itself is also torn down.

## 2. `channel_cost/` — B vs C at real scale (5,000 channels, 10 mock IOCs)

Measures B's leftover-channel cost is actually worth worrying about, at a scale close to the real
`HST<n>` namespace (tens of thousands of names across all buffers/wires).

| | B (never cleared) | C (`clear_channel()` every read) |
|---|---|---|
| Per-channel server memory | 2.09–4.87 KB/channel (noise between runs) | 1.12 KB/channel |

C is cheaper — but only by a small, already-small margin (tens of MB either way at real scale).

## 3. `channel_cost_bd/` — B vs D at real scale (5,000 channels, 10 mock IOCs)

Tests a third option: keep `caget()`'s monitor on, but clear the cache periodically (once per
scan) instead of disabling the monitor per-read.

| | B (monitor off, never cleared) | D (monitor on, cleared every scan) |
|---|---|---|
| Server memory delta | 23.8 MB (4.87 KB/channel) | 100.0 MB (**20.5 KB/channel**) |
| Mean round duration | 2.61s | **3.53s** |
| Round-duration trend | shrinking over the run | **growing** over the run |

D is worse than B on every axis — more memory *and* slower, with per-round cost that keeps rising
rather than settling. Leaving the monitor on buys nothing back.

## Why Scenario B wins

- **Safest**: B never calls `clear_channel()`. pyepics shares one low-level CA channel per
  `(pvname, context)` across every caller in a process — clearing it can pull that channel out
  from under an unrelated PV/`LazyPV`/`get_pv()` reference to the same name elsewhere in a
  long-lived, multi-subsystem process. C and D both carry this risk on every read; B never does.
- **Cleanest**: B needs no extra cleanup step at all. Its leftover CA channels are bounded by the
  fixed `HST<n>` name space actually touched (not by elapsed time or scan count) — the count
  plateaus once every buffer/signal combination in use has been read once, and does not grow
  further. There is nothing to periodically clear, so the production code's existing
  `_clear_ca_cache()` becomes unneeded once `_fetch_single()` moves to `auto_monitor=False`: it
  only ever had anything to find because the old `epics.caget()` populated pyepics' internal PV
  cache, and a raw `epics.PV(...)` never does.
- **Best or tied-best on cost, every time it was actually measured**: C's memory saving over B is
  small at real scale (tens of MB), and D isn't a saving at all — it's strictly worse on both
  memory and per-round speed, with a worsening trend over time.

Net: B removes the real bug completely, and its one remaining cost (idle channels, small and
bounded) is worth far less than the shared-channel crash risk that both alternatives introduce to
buy it back.
