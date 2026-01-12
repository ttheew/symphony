from typing import Dict, List, Tuple

from symphony.util.resource_monitoring.models import CpuTimes
from symphony.util.resource_monitoring.utils import read_lines, safe_int


def parse_cpu_times_from_proc_stat() -> Tuple[CpuTimes, Dict[str, CpuTimes]]:
    lines = read_lines("/proc/stat")
    global_line = None
    per_core: Dict[str, CpuTimes] = {}

    for ln in lines:
        if ln.startswith("cpu "):
            global_line = ln
        elif ln.startswith("cpu") and ln[3].isdigit():
            parts = ln.split()
            name = parts[0]
            per_core[name] = _cpu_times_from_parts(parts)

    if global_line is None:
        raise RuntimeError("Could not read global cpu line from /proc/stat")

    gparts = global_line.split()
    global_times = _cpu_times_from_parts(gparts)
    return global_times, per_core


def _cpu_times_from_parts(parts: List[str]) -> CpuTimes:
    vals = [0] * 8
    for i in range(8):
        if 1 + i < len(parts):
            vals[i] = safe_int(parts[1 + i])
    return CpuTimes(
        user=vals[0],
        nice=vals[1],
        system=vals[2],
        idle=vals[3],
        iowait=vals[4],
        irq=vals[5],
        softirq=vals[6],
        steal=vals[7],
    )


def cpu_percent(prev: CpuTimes, cur: CpuTimes) -> float:
    dt_total = cur.total - prev.total
    dt_idle = cur.idle_all - prev.idle_all
    if dt_total <= 0:
        return 0.0
    usage = 1.0 - (dt_idle / dt_total)
    if usage < 0:
        usage = 0.0
    if usage > 1:
        usage = 1.0
    return usage * 100.0
