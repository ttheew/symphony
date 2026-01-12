import asyncio
from contextlib import suppress

from loguru import logger

from symphony.conductor.http_server import run_fastapi_server
from symphony.conductor.scheduler import NodeScheduler
from symphony.config import AppConfig
from symphony.interface.sqlite import SQLiteAsyncDB
from symphony.transport.grpc_server import create_grpc_server
from symphony.transport.security import build_server_credentials

sqlite_conn = SQLiteAsyncDB()


async def run_conductor(cfg: AppConfig) -> None:
    server = create_grpc_server()
    creds = build_server_credentials(cfg.conductor.cert_path, cfg.conductor.server)

    server.add_secure_port(cfg.conductor.listen, creds)
    await server.start()

    logger.info("Conductor listening on {}, For Nodes to Connect", cfg.conductor.listen)

    await sqlite_conn.connect()
    await sqlite_conn.create_tables()

    scheduler = NodeScheduler()
    scheduler_task = asyncio.create_task(scheduler.run())

    try:
        await run_fastapi_server()
    finally:
        await scheduler.stop()
        scheduler_task.cancel()
        with suppress(asyncio.CancelledError):
            await scheduler_task
        await server.stop(grace=None)
        await sqlite_conn.close()
