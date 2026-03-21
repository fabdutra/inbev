"""HTTP client for Open Brewery DB extraction."""

from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter
from typing import Any

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bees_breweries.config.settings import Settings
from bees_breweries.domain.models import ApiCallResult


class OpenBreweryApiClient:
    """Thin HTTP client responsible for resilient API requests."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session = self._build_session()

    def fetch_breweries(self, page: int, per_page: int) -> ApiCallResult:
        """Fetch a single breweries page."""

        return self._get(
            endpoint="/breweries",
            params={"page": page, "per_page": per_page},
        )

    def fetch_metadata(self, per_page: int) -> ApiCallResult:
        """Fetch dataset metadata used for pagination planning."""

        return self._get(
            endpoint="/breweries/meta",
            params={"page": 1, "per_page": per_page},
        )

    def _build_session(self) -> Session:
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.headers.update({"Accept": "application/json"})
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get(self, endpoint: str, params: dict[str, Any]) -> ApiCallResult:
        requested_at = datetime.now(timezone.utc).isoformat()
        url = f"{self._settings.api_base_url}{endpoint}"
        started_at = perf_counter()

        response = self._session.get(
            url,
            params=params,
            timeout=self._settings.api_timeout_seconds,
        )

        duration_ms = int((perf_counter() - started_at) * 1000)
        payload = self._parse_payload(response)
        response.raise_for_status()

        return ApiCallResult(
            endpoint=endpoint,
            request_url=response.url,
            request_params=params,
            status_code=response.status_code,
            response_headers=dict(response.headers),
            requested_at_utc=requested_at,
            duration_ms=duration_ms,
            payload=payload,
        )

    @staticmethod
    def _parse_payload(response: Response) -> Any:
        return response.json()
