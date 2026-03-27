"""Domain models shared across pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

@dataclass(frozen=True)
class PipelineRunContext:
    """Metadata describing a pipeline run."""

    run_id: str
    extract_date: str


@dataclass(frozen=True)
class ApiCallResult:
    """Normalized result of a source API call."""

    endpoint: str
    request_url: str
    request_params: dict[str, Any]
    status_code: int
    response_headers: dict[str, str]
    requested_at_utc: str
    duration_ms: int
    payload: Any


@dataclass(frozen=True)
class BronzePageResult:
    """Raw brewery page plus execution metadata."""

    run_context: PipelineRunContext
    page: int
    per_page: int
    brewery_records: list[dict[str, Any]]
    request_result: ApiCallResult


@dataclass(frozen=True)
class BronzeMetadataResult:
    """Source metadata response plus execution metadata."""

    run_context: PipelineRunContext
    per_page: int
    metadata_payload: dict[str, Any]
    request_result: ApiCallResult
