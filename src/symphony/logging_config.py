import logging
import sys

from loguru import logger

from .config import LoggingConfig


def setup_logging(cfg: LoggingConfig) -> None:
    logging.root.setLevel(getattr(logging, cfg.level, logging.INFO))
    logger.remove()

    logger.level("DEBUG", color="<blue>")
    logger.level("INFO", color="<green>")
    logger.level("WARNING", color="<yellow>")
    logger.level("ERROR", color="<red>")
    logger.level("CRITICAL", color="<bold red>")

    level = cfg.level.upper() if isinstance(cfg.level, str) else cfg.level

    logger.add(
        sys.stdout,
        level=level,
        colorize=sys.stdout.isatty(),
        backtrace=False,
        diagnose=False,
        format=(
            "<blue>{time:YYYY-MM-DD HH:mm:ss.SSS}</blue> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
    )
