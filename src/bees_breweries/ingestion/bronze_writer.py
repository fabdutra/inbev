"""Writers for Bronze raw data and request logs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bees_breweries.config.settings import Settings
from bees_breweries.domain.models import BronzeMetadataResult, BronzePageResult
from bees_breweries.utils.filesystem import ensure_directory


class BronzeWriter:
    """Persist Bronze payloads and execution metadata to local storage."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def write_metadata(self, result: BronzeMetadataResult) -> None:
        """Write metadata payload and request log files."""

        metadata_dir = self._build_run_path("metadata", result.run_context.extract_date, result.run_context.run_id)
        request_log_dir = self._build_run_path(
            "request_logs",
            result.run_context.extract_date,
            result.run_context.run_id,
        )

        self._write_json(
            metadata_dir / "meta.json",
            {
                "run_id": result.run_context.run_id,
                "extract_date": result.run_context.extract_date,
                "per_page": result.per_page,
                "metadata_payload": result.metadata_payload,
            },
        )
        self._write_json(
            request_log_dir / "meta.json",
            self._request_log_document(
                request_result=result.request_result,
                run_id=result.run_context.run_id,
                extract_date=result.run_context.extract_date,
                record_count=None,
            ),
        )

    def write_page(self, result: BronzePageResult) -> None:
        """Write a raw brewery page and its request log."""

        breweries_dir = self._build_run_path("breweries", result.run_context.extract_date, result.run_context.run_id)
        request_log_dir = self._build_run_path(
            "request_logs",
            result.run_context.extract_date,
            result.run_context.run_id,
        )

        self._write_json(
            breweries_dir / f"page={result.page}.json",
            result.brewery_records,
        )
        self._write_json(
            request_log_dir / f"page={result.page}.json",
            self._request_log_document(
                request_result=result.request_result,
                run_id=result.run_context.run_id,
                extract_date=result.run_context.extract_date,
                record_count=len(result.brewery_records),
                page=result.page,
                per_page=result.per_page,
            ),
        )

    def _build_run_path(self, dataset: str, extract_date: str, run_id: str) -> Path:
        return ensure_directory(
            self._settings.bronze_root
            / "openbrewerydb"
            / dataset
            / f"extract_date={extract_date}"
            / f"run_id={run_id}"
        )

    @staticmethod
    def _request_log_document(
        request_result: Any,
        run_id: str,
        extract_date: str,
        record_count: int | None,
        page: int | None = None,
        per_page: int | None = None,
    ) -> dict[str, Any]:
        document = {
            "run_id": run_id,
            "extract_date": extract_date,
            "endpoint": request_result.endpoint,
            "request_url": request_result.request_url,
            "request_params": request_result.request_params,
            "status_code": request_result.status_code,
            "response_headers": request_result.response_headers,
            "requested_at_utc": request_result.requested_at_utc,
            "duration_ms": request_result.duration_ms,
            "record_count_in_page": record_count,
        }

        if page is not None:
            document["page"] = page
        if per_page is not None:
            document["per_page"] = per_page

        return document

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
