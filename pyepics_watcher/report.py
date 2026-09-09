"""Table/CSV rendering for tracker.Snapshot, matching the
camonitor-cliff CacheWatcher.CSV_COLUMNS convention (one column list, a
header-writing function, and a row-appending function) so results load into
a notebook the same way every other measurement in this repo does.
"""

import csv

CSV_COLUMNS = [
    "elapsed_s",
    "pvname",
    "has_pv_object",
    "monitored",
    "auto_monitor",
    "connected",
    "cache_bytes",
    "update_hz",
]


def _fmt(value) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def write_header(csv_path: str) -> None:
    with open(csv_path, "w", newline="") as f:
        csv.writer(f).writerow(CSV_COLUMNS)


def append_rows(csv_path: str, snapshot, elapsed_s: float) -> None:
    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        for pv in snapshot.pvs.values():
            writer.writerow(
                [round(elapsed_s, 1)]
                + [getattr(pv, col) for col in CSV_COLUMNS if col != "elapsed_s"]
            )


def _summary_line(snapshot, elapsed_s: float = None) -> str:
    monitored = sum(1 for p in snapshot.pvs.values() if p.monitored)
    unmonitored = sum(1 for p in snapshot.pvs.values() if p.monitored is False)
    unknown = sum(1 for p in snapshot.pvs.values() if p.monitored is None)
    prefix = f"[{elapsed_s:>9.1f}s] " if elapsed_s is not None else ""
    rates = [(p.pvname, p.update_hz) for p in snapshot.pvs.values() if p.update_hz]
    if rates:
        top_name, top_hz = max(rates, key=lambda r: r[1])
        avg_hz = sum(hz for _, hz in rates) / len(rates)
        hz_part = f", avg_update_hz={avg_hz:.2f} (max {top_hz:.2f} on {top_name})"
    else:
        hz_part = ", update_hz=n/a (no PV watched long enough yet to have a rate)"
    return (
        f"{prefix}{len(snapshot.pvs)} PVs touched: "
        f"{monitored} monitored, {unmonitored} not monitored, {unknown} unknown -- "
        f"{snapshot.total_cache_bytes} bytes cached{hz_part}"
    )


def print_tick(snapshot, elapsed_s: float) -> None:
    """One status line per PVTracker.start() poll -- used internally by
    tracker._run(). For a one-off terse summary, see print_summary()."""
    print(_summary_line(snapshot, elapsed_s))


def print_summary(snapshot) -> None:
    """Terse, one-line summary of a snapshot's aggregate values -- how many
    PVs touched, how many are monitored/unmonitored/unknown, and total
    cache memory. See print_snapshot() for the full per-PV breakdown."""
    print(_summary_line(snapshot))


def write_summary(log_path: str, snapshot, elapsed_s: float) -> None:
    """Append one terse summary line (see print_summary()) to log_path --
    used internally by tracker._run() for PVTracker's log_path option.
    Opened in append mode: restarting the tracker adds to the log rather
    than truncating it.
    """
    with open(log_path, "a") as f:
        f.write(_summary_line(snapshot, elapsed_s) + "\n")


def _snapshot_lines(snapshot, elapsed_s: float = None) -> list:
    prefix = f"[{elapsed_s:>9.1f}s] " if elapsed_s is not None else ""
    lines = [
        f"{prefix}{len(snapshot.pvs)} PVs, {snapshot.total_cache_bytes} bytes cached total",
        f"{'pvname':<24} {'monitored':<9} {'connected':<9} {'cache_bytes':>11} {'update_hz':>10}",
    ]
    for pv in sorted(snapshot.pvs.values(), key=lambda p: -p.cache_bytes):
        lines.append(
            f"{pv.pvname:<24} {_fmt(pv.monitored):<9} {_fmt(pv.connected):<9} "
            f"{pv.cache_bytes:>11} {_fmt(pv.update_hz):>10}"
        )
    return lines


def print_snapshot(snapshot) -> None:
    """Full per-PV breakdown. See print_summary() for a one-line aggregate."""
    for line in _snapshot_lines(snapshot):
        print(line)


def write_snapshot(log_path: str, snapshot, elapsed_s: float) -> None:
    """Append the full per-PV breakdown (see print_snapshot()) to log_path --
    used internally by tracker._run() for PVTracker's snapshot_interval_s
    option, independent of and typically coarser than log_interval_s's terse
    summary line (a full breakdown is much more log volume per write).
    Opened in append mode, like write_summary().
    """
    with open(log_path, "a") as f:
        f.write("\n".join(_snapshot_lines(snapshot, elapsed_s)) + "\n")
