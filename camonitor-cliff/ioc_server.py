#!/usr/bin/env python3
"""Mock IOC serving waveform PVs shaped like the real HST buffer PVs.

Uses caproto (same library as slicops.pkcli.ioc), with PVs dynamically
created following the exact ``self._pvs_[name] = pvproperty(...)`` pattern
from ~/src/slaclab/slicops/slicops/pkcli/ioc.py, and periodic updates via
caproto's own ``pvproperty(...).scan(period=...)`` mechanism (see
caproto/ioc_examples/decay.py) -- fully asyncio-native, no threading.

PV names: CAGET_TEST:SIG{i}:HST{b} for i in range(PVS_PER_BUFFER),
b in range(N_BUFFERS) -- mirrors real HST PVs, which are named
"{signal}HST{buffer_number}" with a small, reused pool of buffer numbers.
Plus one unscanned scalar, CAGET_TEST:CANARY, used only as a latency probe
target by the client scripts.

--port and --buffer-start support running several instances at once, each
on its own port and its own non-overlapping slice of buffer numbers, so a
single client (multi-entry EPICS_CA_ADDR_LIST) can spread its channels
across many servers instead of overloading one -- see
run_channel_cost_test.py, and the "why split across servers" discussion in
README.md.
"""

import argparse
import os

import common  # noqa: F401  (sets CA env vars before caproto/epics import)

import caproto.server
import numpy as np


async def _periodic_update(group, instance, async_lib):
    await instance.write(value=list(np.random.random(common.ARRAY_LEN) * 1000.0))


def _make_group_class(n_buffers: int, pvs_per_buffer: int, buffer_start: int, with_canary: bool) -> type:
    class _PVGroup(caproto.server.PVGroup):
        def __init__(self, *args, **kwargs):
            names = common.all_pv_names(n_buffers, pvs_per_buffer, buffer_start=buffer_start)
            for name in names:
                p = caproto.server.pvproperty(value=[0.0] * common.ARRAY_LEN)
                p = p.scan(period=1.0 / common.UPDATE_HZ)(_periodic_update)
                self._pvs_[name] = p
                p.__set_name__(self, name)
            if with_canary:
                canary = caproto.server.pvproperty(value=0.0)
                self._pvs_[common.CANARY_NAME] = canary
                canary.__set_name__(self, common.CANARY_NAME)
            super().__init__(*args, **kwargs)

    return _PVGroup


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-buffers", type=int, default=common.N_BUFFERS)
    parser.add_argument("--pvs-per-buffer", type=int, default=common.PVS_PER_BUFFER)
    parser.add_argument(
        "--buffer-start", type=int, default=0,
        help="serve buffer numbers [buffer_start, buffer_start + n_buffers) instead of [0, n_buffers)",
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help="EPICS_CA_SERVER_PORT for this instance (UDP search + first-tried TCP port); "
             "default is the standard EPICS port (5064)",
    )
    parser.add_argument(
        "--no-canary", action="store_true",
        help="don't create CAGET_TEST:CANARY -- use this on every instance but one when "
             "running several servers at once, so the name isn't defined on more than one",
    )
    args = parser.parse_args()

    if args.port is not None:
        os.environ["EPICS_CA_SERVER_PORT"] = str(args.port)

    total = args.n_buffers * args.pvs_per_buffer
    print(
        f"Starting mock IOC (pid={os.getpid()}, port={args.port or 5064}): {total} waveform PVs "
        f"({args.n_buffers} buffers x {args.pvs_per_buffer} signals, "
        f"buffers [{args.buffer_start}, {args.buffer_start + args.n_buffers})), "
        f"{common.ARRAY_LEN} elements each, {common.UPDATE_HZ} Hz"
        + ("" if args.no_canary else f", plus {common.CANARY_NAME}")
    )

    group_cls = _make_group_class(
        args.n_buffers, args.pvs_per_buffer, args.buffer_start, with_canary=not args.no_canary
    )
    caproto.server.run(
        pvdb=group_cls(prefix="").pvdb,
        interfaces=["127.0.0.1"],
        module_name="caproto.asyncio.server",
        log_pv_names=False,
    )


if __name__ == "__main__":
    main()
