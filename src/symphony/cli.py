import argparse


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="symphony", description="Symphony orchestrator (Conductor/Node)"
    )
    p.add_argument(
        "--config", "-c", default="config.yaml", help="Path to YAML config file"
    )
    p.add_argument(
        "--mode", choices=["conductor", "node"], help="Override mode from config"
    )
    p.add_argument("--log-level", help="Override log level (INFO, DEBUG, ...)")
    p.add_argument("--log-json", action="store_true", help="Enable JSON logs")
    return p
