"""Metadata-specific extraction helpers."""

from __future__ import annotations

from bees_breweries.domain.models import BronzeMetadataResult, PipelineRunContext
from bees_breweries.ingestion.api_client import OpenBreweryApiClient


class OpenBreweryMetadataService:
    """Service that fetches and normalizes source dataset metadata."""

    def __init__(self, api_client: OpenBreweryApiClient) -> None:
        self._api_client = api_client

    def fetch(self, run_context: PipelineRunContext, per_page: int) -> BronzeMetadataResult:
        """Return metadata plus request execution details."""

        request_result = self._api_client.fetch_metadata(per_page=per_page)
        return BronzeMetadataResult(
            run_context=run_context,
            per_page=per_page,
            metadata_payload=request_result.payload,
            request_result=request_result,
        )
