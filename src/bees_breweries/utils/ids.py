"""Identifier helpers."""

from uuid import uuid4


def generate_run_id() -> str:
    """Generate a unique pipeline run identifier."""

    return str(uuid4())
