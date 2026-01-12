import asyncio

from symphony.cli import build_parser
from symphony.config import load_config
from symphony.logging_config import setup_logging
from symphony.runtime import run_with_signals


def main() -> None:
    args = build_parser().parse_args()
    cfg = load_config(args.config)
    setup_logging(cfg.logging)

    if cfg.mode == "conductor":
        from symphony.conductor.app import run_conductor

        asyncio.run(run_with_signals(lambda: run_conductor(cfg)))
    else:
        from symphony.node.app import run_node

        asyncio.run(run_with_signals(lambda: run_node(cfg)))


if __name__ == "__main__":
    main()
