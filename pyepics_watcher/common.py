"""CA env setup and PV names for the pyepics_watcher demo (ioc_server.py/run_demo.py).

pyepics_watcher itself (tracker.py) has no dependency on this module or on
camonitor-cliff's -- it introspects whatever pyepics process it's pointed at,
however that process configured CA. This file only sets up the demo's own
local mock IOC, and duplicates camonitor-cliff/common.py's import-order
sensitive env vars rather than importing them, to keep that independence real.
"""

import os

if not os.environ.get("EPICS_CA_ADDR_LIST"):
    os.environ["EPICS_CA_ADDR_LIST"] = "127.0.0.1"
if not os.environ.get("EPICS_CA_AUTO_ADDR_LIST"):
    os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"

PREFIX = "PVMON_DEMO:"
ARRAY_LEN = 2800
UPDATE_HZ = 13

CAGET_PV = f"{PREFIX}CAGET"  # read via epics.caget() -- monitor, _PVcache_-visible
NOMON_PV = f"{PREFIX}NOMON"  # read via epics.PV(auto_monitor=False) -- channel-only, no monitor
BAREMON_PV = f"{PREFIX}BAREMON"  # bare epics.PV(auto_monitor=True) -- monitor, _PVcache_-invisible

ALL_PVS = [CAGET_PV, NOMON_PV, BAREMON_PV]
