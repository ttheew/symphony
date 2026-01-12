import asyncio

from loguru import logger

from symphony.config import AppConfig
from symphony.node.agent import NodeAgent


async def run_node(cfg: AppConfig) -> None:
    agent = NodeAgent(cfg.node)
    logger.info("Node starting id={}", cfg.node.node_id)
    try:
        await agent.start()
    except asyncio.CancelledError:
        logger.info("Node shutting down...")
        await agent.stop()
