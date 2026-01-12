from typing import Any, Dict

from symphony.util.resource_monitoring.utils import read_lines, safe_int


def _meminfo() -> Dict[str, int]:
    out: Dict[str, int] = {}
    for ln in read_lines("/proc/meminfo"):
        if ":" not in ln:
            continue
        k, v = ln.split(":", 1)
        parts = v.strip().split()
        if not parts:
            continue
        out[k] = safe_int(parts[0])
    return out


def ram_snapshot() -> Dict[str, Any]:
    mi = _meminfo()
    total_kb = mi.get("MemTotal", 0)
    avail_kb = mi.get("MemAvailable", 0)
    free_kb = mi.get("MemFree", 0)
    buffers_kb = mi.get("Buffers", 0)
    cached_kb = mi.get("Cached", 0)
    used_kb = max(total_kb - avail_kb, 0)

    pct = (used_kb / total_kb * 100.0) if total_kb > 0 else 0.0
    return {
        "total_bytes": total_kb * 1024,
        "available_bytes": avail_kb * 1024,
        "used_bytes": used_kb * 1024,
        "used_percent": pct,
        "free_bytes": free_kb * 1024,
        "buffers_bytes": buffers_kb * 1024,
        "cached_bytes": cached_kb * 1024,
    }
