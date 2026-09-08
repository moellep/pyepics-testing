# pyepics-testing

Standalone test harnesses for `pyepics`/Channel Access performance and behavior questions —
things worth reproducing in isolation against a local mock IOC rather than debugging against a
real facility network.

## Tests

- [`camonitor-cliff/`](camonitor-cliff/) — reproduces a monitor-accumulation bug where
  `epics.caget()` silently creates a persistent CA monitor that's never released, and compares it
  against two fixes (`auto_monitor=False`, and that plus `epics.ca.clear_channel()`). Includes a
  `caproto`-based mock IOC and an untested `pcaspy`-based alternative.

Each test directory is self-contained (its own `common.py`/env setup, its own README) and can be
run independently.
