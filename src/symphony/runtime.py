import asyncio
import logging
import signal
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


async def run_with_signals(main_coro: Callable[[], Awaitable[None]]) -> None:
    stop = asyncio.Event()

    def _set_stop() -> None:
        logger.info("Shutdown signal received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _set_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _set_stop())

    main_task = asyncio.create_task(main_coro(), name="main")
    stop_task = asyncio.create_task(stop.wait(), name="stop_wait")

    done, pending = await asyncio.wait(
        {main_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    if main_task in done:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        await main_task
        return

    logger.info("Cancelling main task...")
    main_task.cancel()
    await asyncio.gather(main_task, return_exceptions=True)

    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
