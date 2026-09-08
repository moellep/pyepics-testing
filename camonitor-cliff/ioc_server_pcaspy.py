#!/usr/bin/env python3
"""Mock IOC serving the same PV layout as ioc_server.py, but via pcaspy
instead of caproto -- to check whether the caproto server's unbounded
memory growth under active monitors (see README.md) is caproto-specific,
or also present in pcaspy (which wraps the actual reference libcas C
library real IOCs use).

KNOWN BROKEN in at least one environment (see README.md, "ioc_server_pcaspy.py"
section): even EPICS Base's own caget/camonitor command-line tools failed
against this server there (get: "Channel read request failed"; monitor:
"Virtual circuit disconnect"), pointing to a pcaspy/EPICS-Base version or
build mismatch, not a bug in this script. Verify with a plain `caget` against
a scalar PV before trusting any comparison run through this server.

pcaspy API confirmed during the original planning pass (source-read, not
guessed): pvdb dict of {name: {"type", "count", "value", ...}},
Driver.setParam()/updatePVs() to push new values, SimpleServer.process(delay)
run in a loop. Deliberately single-threaded here (not a separate update
thread) -- pcaspy's own source has no documented guarantee that
setParam()/updatePVs() are safe to call from a different thread than the
one running process().

Same PV names/shape as ioc_server.py: CAGET_TEST:SIG{i}:HST{b}, 2800-element
float arrays, 13 Hz, plus CAGET_TEST:CANARY.
"""

import argparse
import time

import common  # noqa: F401  (sets CA env vars before pcaspy/epics import)

import numpy as np
from pcaspy import Driver, SimpleServer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-buffers", type=int, default=common.N_BUFFERS)
    parser.add_argument("--pvs-per-buffer", type=int, default=common.PVS_PER_BUFFER)
    args = parser.parse_args()

    names = common.all_pv_names(args.n_buffers, args.pvs_per_buffer)
    pvdb = {
        name: {"type": "float", "count": common.ARRAY_LEN, "value": [0.0] * common.ARRAY_LEN}
        for name in names
    }
    pvdb[common.CANARY_NAME] = {"type": "float", "value": 0.0}

    server = SimpleServer()
    server.createPV("", pvdb)
    driver = Driver()

    total = args.n_buffers * args.pvs_per_buffer
    print(
        f"Starting pcaspy mock IOC (pid={__import__('os').getpid()}): {total} waveform PVs "
        f"({args.n_buffers} buffers x {args.pvs_per_buffer} signals), "
        f"{common.ARRAY_LEN} elements each, {common.UPDATE_HZ} Hz, "
        f"plus {common.CANARY_NAME}"
    )

    period = 1.0 / common.UPDATE_HZ
    next_update = time.monotonic()
    while True:
        server.process(0.05)
        now = time.monotonic()
        if now >= next_update:
            for name in names:
                driver.setParam(name, list(np.random.random(common.ARRAY_LEN) * 1000.0))
            driver.updatePVs()
            next_update = now + period


if __name__ == "__main__":
    main()
