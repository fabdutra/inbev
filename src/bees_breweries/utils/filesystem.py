"""Filesystem helpers used by writers and local execution."""

from pathlib import Path


def ensure_directory(path: Path) -> Path:
    """Create a directory if it does not exist and return it."""

    path.mkdir(parents=True, exist_ok=True)
    return path
