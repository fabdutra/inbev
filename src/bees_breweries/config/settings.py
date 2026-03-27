"""Central configuration for local and orchestrated runs."""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    environment: str = "local"
    project_root: Path = Path(__file__).resolve().parents[3]
    data_root: Path = Path(__file__).resolve().parents[3] / "data"
    bronze_root: Path = Path(__file__).resolve().parents[3] / "data" / "bronze"
    silver_root: Path = Path(__file__).resolve().parents[3] / "data" / "silver"
    gold_root: Path = Path(__file__).resolve().parents[3] / "data" / "gold"
    api_base_url: str = "https://api.openbrewerydb.org/v1"
    api_timeout_seconds: int = 30
    api_per_page: int = 200


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Build and cache settings from the current environment."""

    project_root = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[3]))
    data_root = Path(os.getenv("DATA_ROOT", project_root / "data"))

    return Settings(
        environment=os.getenv("APP_ENV", "local"),
        project_root=project_root,
        data_root=data_root,
        bronze_root=Path(os.getenv("BRONZE_ROOT", data_root / "bronze")),
        silver_root=Path(os.getenv("SILVER_ROOT", data_root / "silver")),
        gold_root=Path(os.getenv("GOLD_ROOT", data_root / "gold")),
        api_base_url=os.getenv("API_BASE_URL", "https://api.openbrewerydb.org/v1"),
        api_timeout_seconds=int(os.getenv("API_TIMEOUT_SECONDS", "30")),
        api_per_page=int(os.getenv("API_PER_PAGE", "200")),
    )
