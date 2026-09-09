# pyepics-testing

Standalone test harnesses for `pyepics`/Channel Access performance and behavior questions —
things worth reproducing in isolation against a local mock IOC rather than debugging against a
real facility network.

## Tests

- [`camonitor-cliff/`](camonitor-cliff/) — reproduces a monitor-accumulation bug where
  `epics.caget()` silently creates a persistent CA monitor that's never released, and compares it
  against two fixes (`auto_monitor=False`, and that plus `epics.ca.clear_channel()`). Includes a
  `caproto`-based mock IOC and an untested `pcaspy`-based alternative.
- [`pyepics_watcher/`](pyepics_watcher/) — general-purpose introspection for any running pyepics process:
  which PVs it has touched, which have a live CA monitor, cache memory usage, and per-PV update
  rate. Standalone (no shared env/lock assumptions with `camonitor-cliff/`); built from what that
  harness kept diagnosing by hand.

Each test directory is self-contained (its own `common.py`/env setup, its own README) and can be
run independently.

`pyepics_watcher/` is also a real installable package (`pyproject.toml` at this repo root):

```sh
pip install -e ".[demo]"
```

See [`pyepics_watcher/README.md`](pyepics_watcher/README.md) for usage.
