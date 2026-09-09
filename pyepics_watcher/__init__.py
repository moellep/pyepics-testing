""":mod:`pyepics_watcher` package

General-purpose introspection for a running pyepics process: which PVs it has
touched, which have a live CA monitor, cache memory usage, and per-PV update
rate. See ``tracker.py`` and ``README.md`` for how it works.
"""

import importlib.metadata

try:
    # We only have a version once the package is installed.
    __version__ = importlib.metadata.version("pyepics_watcher")
except importlib.metadata.PackageNotFoundError:
    pass
