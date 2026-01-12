import uvicorn
from loguru import logger

from symphony.conductor.api.server import create_app


async def run_fastapi_server(
    host: str = "0.0.0.0",
    port: int = 8000,
    log_level: str = "warning",
) -> None:
    app = create_app()
    logger.info("Conductor listening on {}:{}, API,", host, port)
    logger.info("Use {}:{}/ui for web ui,", host, port)
    config = uvicorn.Config(
        app=app,
        host=host,
        port=port,
        log_level=log_level,
        loop="asyncio",
        lifespan="off",
    )
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        pass
