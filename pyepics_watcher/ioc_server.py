#!/usr/bin/env python3
"""Mock IOC serving the three pyepics_watcher demo PVs (see common.py), all scanned
at common.UPDATE_HZ so run_demo.py can check both cache-memory sizing and
measured update frequency against known values.

Same caproto ``pvproperty(...).scan(period=...)`` pattern as
camonitor-cliff/ioc_server.py.
"""

from pyepics_watcher import common  # sets CA env vars before caproto import

import caproto.server
import numpy as np


async def _periodic_update(group, instance, async_lib):
    await instance.write(value=list(np.random.random(common.ARRAY_LEN) * 1000.0))


def _make_group_class() -> type:
    class _PVGroup(caproto.server.PVGroup):
        def __init__(self, *args, **kwargs):
            for name in common.ALL_PVS:
                p = caproto.server.pvproperty(value=[0.0] * common.ARRAY_LEN)
                p = p.scan(period=1.0 / common.UPDATE_HZ)(_periodic_update)
                self._pvs_[name] = p
                p.__set_name__(self, name)
            super().__init__(*args, **kwargs)

    return _PVGroup


def main() -> None:
    print(
        f"Starting pyepics_watcher demo IOC: {common.ALL_PVS}, "
        f"{common.ARRAY_LEN} elements each, {common.UPDATE_HZ} Hz"
    )
    group_cls = _make_group_class()
    caproto.server.run(
        pvdb=group_cls(prefix="").pvdb,
        interfaces=["127.0.0.1"],
        module_name="caproto.asyncio.server",
        log_pv_names=False,
    )


if __name__ == "__main__":
    main()
