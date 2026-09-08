"""Shared setup and constants for the caget monitor-accumulation test harness.

Every entry point in this directory must ``import common`` as its first
import (before ``epics``/``caproto``) so the CA isolation env vars below are
set before those libraries read them at import time.
"""

import os
import threading

# Not os.environ[...] = ... unconditionally: that would clobber a
# deliberate override a caller already set in this process's environment
# before exec'ing (e.g. run_channel_cost_test.py's multi-server
# "127.0.0.1:5064 127.0.0.1:5065 ..."), silently collapsing it back to a
# single address -- confirmed as the actual cause of ~20s stalls that
# looked exactly like a CA/threading bug but were really just the client
# never searching past server 0. Not os.environ.setdefault() either: this
# process's own inherited shell environment can carry a *stray, empty*
# EPICS_CA_ADDR_LIST="" (observed directly in this project's dev shell),
# which setdefault() would never override since the key already exists.
# Checking truthiness handles both: a real override survives, an empty or
# absent value gets the default.
if not os.environ.get("EPICS_CA_ADDR_LIST"):
    os.environ["EPICS_CA_ADDR_LIST"] = "127.0.0.1"
if not os.environ.get("EPICS_CA_AUTO_ADDR_LIST"):
    os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"

# pyepics/libca calls are not safe to run truly concurrently from two
# threads even when each has attached to the CA context (use_initial_context()
# fixes context-less crashes, but not this) -- observed as a hard segfault
# under load in this harness. Every actual CA call (caget/PV().get()/etc.,
# in both the client scripts and the watcher's canary probe) takes this lock.
CA_LOCK = threading.Lock()

PREFIX = "CAGET_TEST:"
N_BUFFERS = 5
PVS_PER_BUFFER = 10
ARRAY_LEN = 2800
UPDATE_HZ = 13
CANARY_NAME = f"{PREFIX}CANARY"


def pv_name(sig_idx: int, buffer_num: int) -> str:
    return f"{PREFIX}SIG{sig_idx}:HST{buffer_num}"


def all_pv_names(
    n_buffers: int = N_BUFFERS,
    pvs_per_buffer: int = PVS_PER_BUFFER,
    buffer_start: int = 0,
) -> list[str]:
    """PV names for buffer numbers [buffer_start, buffer_start + n_buffers).

    buffer_start lets multiple mock-IOC processes each own a distinct,
    non-overlapping slice of the buffer-number range (see
    run_channel_cost_test.py) -- server i serving buffers
    [i*n_buffers, (i+1)*n_buffers) means no two servers ever define the
    same PV name, so a client searching across all of them (multi-entry
    EPICS_CA_ADDR_LIST) routes each channel to exactly the right server.
    """
    return [
        pv_name(sig_idx, buf)
        for buf in range(buffer_start, buffer_start + n_buffers)
        for sig_idx in range(pvs_per_buffer)
    ]
