"""Bronze extraction orchestration."""

from __future__ import annotations

import logging
from typing import Any

from bees_breweries.domain.models import BronzePageResult, PipelineRunContext
from bees_breweries.ingestion.api_client import OpenBreweryApiClient
from bees_breweries.ingestion.bronze_writer import BronzeWriter
from bees_breweries.ingestion.metadata_client import OpenBreweryMetadataService
from bees_breweries.ingestion.paginator import PaginationPlanner

logger = logging.getLogger(__name__)


class BronzeExtractor:
    """Coordinate metadata fetch, pagination planning, and raw writes."""

    def __init__(
        self,
        api_client: OpenBreweryApiClient,
        metadata_service: OpenBreweryMetadataService,
        paginator: PaginationPlanner,
        writer: BronzeWriter,
    ) -> None:
        self._api_client = api_client
        self._metadata_service = metadata_service
        self._paginator = paginator
        self._writer = writer

    def extract(self, run_context: PipelineRunContext, per_page: int, max_pages: int | None = None) -> dict[str, Any]:
        """Extract Bronze source data and persist it to storage."""

        metadata_result = self._metadata_service.fetch(run_context=run_context, per_page=per_page)
        self._writer.write_metadata(metadata_result)

        total_records = int(metadata_result.metadata_payload["total"])
        pages = self._paginator.build_pages(total_records=total_records, per_page=per_page, max_pages=max_pages)

        total_extracted_records = 0
        for page in pages:
            request_result = self._api_client.fetch_breweries(page=page, per_page=per_page)
            brewery_records = self._validate_records(request_result.payload, page=page)
            total_extracted_records += len(brewery_records)

            self._writer.write_page(
                BronzePageResult(
                    run_context=run_context,
                    page=page,
                    per_page=per_page,
                    brewery_records=brewery_records,
                    request_result=request_result,
                )
            )

            logger.info(
                "Bronze page extracted",
                extra={
                    "run_id": run_context.run_id,
                    "page": page,
                    "record_count": len(brewery_records),
                },
            )

        return {
            "run_id": run_context.run_id,
            "extract_date": run_context.extract_date,
            "planned_pages": len(pages),
            "total_records_reported_by_api": total_records,
            "total_records_extracted": total_extracted_records,
            "per_page": per_page,
        }

    @staticmethod
    def _validate_records(payload: Any, page: int) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            raise ValueError(f"Expected list payload for breweries page {page}, received {type(payload)!r}")
        return payload
