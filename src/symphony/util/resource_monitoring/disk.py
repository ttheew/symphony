import os
from typing import Any, Dict


def space_for_mount(path: str) -> Dict[str, Any]:
    st = os.statvfs(path)
    frsize = st.f_frsize if st.f_frsize != 0 else st.f_bsize
    total = st.f_blocks * frsize
    free = st.f_bfree * frsize
    avail = st.f_bavail * frsize
    used = total - free
    used_pct = (used / total * 100.0) if total > 0 else 0.0
    return {
        "path": path,
        "total_bytes": total,
        "used_bytes": used,
        "free_bytes": free,
        "available_bytes": avail,
        "used_percent": used_pct,
    }
