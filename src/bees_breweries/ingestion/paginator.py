"""Pagination planning for source extraction."""

from __future__ import annotations

from math import ceil


class PaginationPlanner:
    """Compute deterministic page ranges for API extraction."""

    def build_pages(self, total_records: int, per_page: int, max_pages: int | None = None) -> list[int]:
        """Return a 1-based list of pages to ingest."""

        if total_records < 0:
            raise ValueError("total_records must be greater than or equal to zero")
        if per_page <= 0:
            raise ValueError("per_page must be greater than zero")

        total_pages = ceil(total_records / per_page) if total_records else 0
        pages = list(range(1, total_pages + 1))

        if max_pages is not None:
            if max_pages <= 0:
                raise ValueError("max_pages must be greater than zero when provided")
            return pages[:max_pages]

        return pages
