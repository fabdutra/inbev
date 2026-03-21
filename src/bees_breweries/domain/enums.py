"""Domain enumerations used across the pipeline."""

from enum import Enum


class Layer(str, Enum):
    """Supported lakehouse layers."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
