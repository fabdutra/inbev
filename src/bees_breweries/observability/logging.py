"""Structured logging helpers."""

import logging


def configure_logging(level: int = logging.INFO) -> None:
    """Set a consistent logging format for local and orchestrated runs."""

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
