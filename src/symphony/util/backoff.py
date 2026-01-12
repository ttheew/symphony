import random
from typing import Iterator


def backoff(
    *,
    base: float = 1.0,
    factor: float = 2.0,
    max_delay: float = 30.0,
    jitter: float = 0.2,
) -> Iterator[float]:
    """
    Unbounded exponential backoff generator with jitter.
    """
    delay = base
    while True:
        low = delay * (1.0 - jitter)
        high = delay * (1.0 + jitter)
        yield random.uniform(low, high)
        delay = min(delay * factor, max_delay)
