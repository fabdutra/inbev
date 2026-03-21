import json

from bees_breweries.config.settings import Settings
from bees_breweries.domain.models import ApiCallResult, BronzePageResult, PipelineRunContext
from bees_breweries.ingestion.bronze_writer import BronzeWriter


def test_bronze_writer_persists_page_and_request_log(tmp_path) -> None:
    settings = Settings(
        project_root=tmp_path,
        data_root=tmp_path / "data",
        bronze_root=tmp_path / "data" / "bronze",
        silver_root=tmp_path / "data" / "silver",
        gold_root=tmp_path / "data" / "gold",
    )
    writer = BronzeWriter(settings=settings)
    result = BronzePageResult(
        run_context=PipelineRunContext(run_id="run-123", extract_date="2026-03-21"),
        page=1,
        per_page=200,
        brewery_records=[{"id": "abc", "name": "Test Brewery"}],
        request_result=ApiCallResult(
            endpoint="/breweries",
            request_url="https://example.test/breweries?page=1",
            request_params={"page": 1, "per_page": 200},
            status_code=200,
            response_headers={"x-test": "true"},
            requested_at_utc="2026-03-21T00:00:00+00:00",
            duration_ms=10,
            payload=[{"id": "abc", "name": "Test Brewery"}],
        ),
    )

    writer.write_page(result)

    page_file = (
        tmp_path
        / "data"
        / "bronze"
        / "openbrewerydb"
        / "breweries"
        / "extract_date=2026-03-21"
        / "run_id=run-123"
        / "page=1.json"
    )
    log_file = (
        tmp_path
        / "data"
        / "bronze"
        / "openbrewerydb"
        / "request_logs"
        / "extract_date=2026-03-21"
        / "run_id=run-123"
        / "page=1.json"
    )

    assert json.loads(page_file.read_text(encoding="utf-8")) == [{"id": "abc", "name": "Test Brewery"}]
    assert json.loads(log_file.read_text(encoding="utf-8"))["status_code"] == 200
