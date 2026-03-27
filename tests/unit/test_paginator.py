import pytest

from bees_breweries.ingestion.paginator import PaginationPlanner


def test_build_pages_returns_expected_range() -> None:
    planner = PaginationPlanner()

    assert planner.build_pages(total_records=401, per_page=200) == [1, 2, 3]


def test_build_pages_respects_max_pages() -> None:
    planner = PaginationPlanner()

    assert planner.build_pages(total_records=1000, per_page=100, max_pages=2) == [1, 2]


def test_build_pages_rejects_invalid_page_size() -> None:
    planner = PaginationPlanner()

    with pytest.raises(ValueError):
        planner.build_pages(total_records=10, per_page=0)
