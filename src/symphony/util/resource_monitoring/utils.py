import time
from typing import List


def read_lines(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.readlines()


def now() -> float:
    return time.time()


def safe_int(x: str, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default
