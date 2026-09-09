# camonitor-cliff — summary of all test runs

Four rounds of testing, each comparing candidate fixes for the real bug this harness reproduces:
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

Measures whether B's leftover-channel cost is actually worth worrying about, at a scale close to
the real `HST<n>` namespace (tens of thousands of names across all buffers/wires).

| | B (never cleared) | C (`clear_channel()` every read) |
|---|---|---|
| Per-channel server memory | 2.09–4.87 KB/channel (noise between runs) | 1.12 KB/channel |
| Per-channel **client** memory | **24.5 KB/channel** | 0.03–0.17 KB/channel |

Server-side, C is cheaper but only by a small, already-small margin. Client-side, B's cost is much
bigger than it first looked — see #4 below.

## 3. `channel_cost_bd/` (B vs D) — a third option: keep the monitor, clear periodically

Tests keeping `caget()`'s monitor on but clearing the cache periodically (once per scan) instead
of disabling the monitor per-read.

| | B (monitor off, never cleared) | D (monitor on, cleared every scan) |
|---|---|---|
| Server memory delta | ~10.3 MB (~2.1 KB/channel) | ~97 MB (**~20 KB/channel**) |
| Mean round duration | ~2.4s | **~3.5s** |
| Round-duration trend | shrinking over the run | **growing** over the run |

D is worse than B on every axis — more memory *and* slower, with per-round cost that keeps rising
rather than settling. Leaving the monitor on buys nothing back.

**Root-caused, not just measured.** D's server cost traces to the mock server (`caproto`)'s
per-subscription backlog (`unexpired_updates`, capped at `MAX_SUBSCRIPTION_BACKLOG = 1000`),
which buffers real scan-driven monitor pushes for as long as a subscription stays open — measured
to scale roughly linearly with hold time (12.2 → 65.6 KB/channel from 0 to 3.5s held open), not
with channel count. Two genuine `caproto` bugs were found and fixed along the way
(`_cull_subscriptions()` never deleted an emptied `SubscriptionSpec` entry, in two different
dicts) — reproduction at `~/save/slac-wire/caproto-bug/`, suitable for an upstream report — but
confirmed to change D's real-scale cost by under 5%; they weren't the dominant driver. Since the
backlog behavior is legitimate CA server design, not a caproto defect, it plausibly reproduces on
a real IOC too.

## 4. `channel_cost_bd/` (B vs D vs E) — fixing B's client-side cost

B's small server-side cost hid a much bigger client-side one: `epics.PV(...).get(...)` caches the
full last-read *value* (`epics.ca._cache[ctx][pvname].get_results`) forever — `disconnect()` never
touches it. Confirmed directly by inspecting a live cache entry. For a waveform PV that's real,
array-sized memory per distinct channel: **24.5 KB/channel measured, matching one 2,800-element
`float64` array almost exactly.**

**E** tests fixing this without `clear_channel()`'s risk: same as B, plus one extra line clearing
just the cached value (`entry.get_results.clear()`) right after each read, leaving the channel
itself untouched.

| | B | D | E |
|---|---|---|---|
| Server memory | 2.15 KB/channel | 19.85 KB/channel | 2.13 KB/channel |
| Client memory | **24.52 KB/channel** | 0.09 KB/channel | **1.39 KB/channel** |
| Mean round duration | 2.331s | 3.528s | 2.353s |

E matches B almost exactly on server memory and speed, while cutting client memory by **94%** —
without touching `clear_channel()` at all.

## Why Scenario E wins

- **Safest**: E never calls `clear_channel()`. pyepics shares one low-level CA channel per
  `(pvname, context)` across every caller in a process — clearing it can pull that channel out
  from under an unrelated PV/`LazyPV`/`get_pv()` reference to the same name elsewhere in a
  long-lived, multi-subsystem process. C and D both carry this risk on every read; B and E never
  do. Clearing just the cached *value* (E) carries none of that risk — the worst case for another
  caller is re-fetching a value they'd have needed to re-fetch anyway.
- **Cleanest on the channel itself**: like B, E's leftover CA channels are bounded by the fixed
  `HST<n>` name space actually touched (not by elapsed time or scan count) — nothing to
  periodically clear there, so the production code's existing `_clear_ca_cache()` becomes
  unneeded once `_fetch_single()` moves off `caget()`. E adds exactly one new thing to clean up
  (the cached value), in exchange for removing the much larger client-side cost that plain B left
  behind.
- **Best on cost, every axis, every time it was actually measured**: matches B on server memory
  and speed; matches (and slightly beats) C on client memory; strictly beats D on everything.

Net: E removes the real bug completely, keeps B's small and bounded channel cost, and closes the
one real gap B left open (the client-side value cache) — without ever taking on C's or D's
shared-channel risk to do it.
