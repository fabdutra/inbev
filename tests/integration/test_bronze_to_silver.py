import json

from bees_breweries.config.settings import Settings
from bees_breweries.processing.bronze_to_silver import SilverTransformer
from bees_breweries.processing.spark_session_factory import SparkSessionFactory
from bees_breweries.processing.validators import DataQualityValidator


def test_bronze_to_silver_transforms_and_deduplicates_records(tmp_path) -> None:
    bronze_breweries_dir = (
        tmp_path / "data" / "bronze" / "openbrewerydb" / "breweries" / "extract_date=2026-03-21" / "run_id=run-1"
    )
    bronze_request_logs_dir = (
        tmp_path / "data" / "bronze" / "openbrewerydb" / "request_logs" / "extract_date=2026-03-21" / "run_id=run-1"
    )
    bronze_breweries_dir.mkdir(parents=True, exist_ok=True)
    bronze_request_logs_dir.mkdir(parents=True, exist_ok=True)

    (bronze_breweries_dir / "page=1.json").write_text(
        json.dumps(
            [
                {
                    "id": "brewery-1",
                    "name": " Brewery One ",
                    "brewery_type": "micro",
                    "address_1": "123 Main St",
                    "address_2": None,
                    "address_3": None,
                    "city": "Austin",
                    "state_province": "Texas",
                    "postal_code": "78701",
                    "country": "United States",
                    "longitude": -97.0,
                    "latitude": 30.0,
                    "phone": "123",
                    "website_url": "https://example.com",
                    "state": "Texas",
                    "street": "123 Main St",
                },
                {
                    "id": "brewery-1",
                    "name": "Brewery One Updated",
                    "brewery_type": "regional",
                    "address_1": "123 Main St",
                    "address_2": None,
                    "address_3": None,
                    "city": "Austin",
                    "state_province": "Texas",
                    "postal_code": "78701",
                    "country": "United States",
                    "longitude": -97.0,
                    "latitude": 30.0,
                    "phone": "123",
                    "website_url": "https://example.com",
                    "state": "Texas",
                    "street": "123 Main St",
                },
            ]
        ),
        encoding="utf-8",
    )
    (bronze_request_logs_dir / "page=1.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "extract_date": "2026-03-21",
                "endpoint": "/breweries",
                "request_url": "https://api.test/breweries?page=1",
                "request_params": {"page": 1, "per_page": 200},
                "status_code": 200,
                "response_headers": {},
                "requested_at_utc": "2026-03-21T00:00:00+00:00",
                "duration_ms": 100,
                "record_count_in_page": 2,
                "page": 1,
                "per_page": 200,
            }
        ),
        encoding="utf-8",
    )

    settings = Settings(
        project_root=tmp_path,
        data_root=tmp_path / "data",
        bronze_root=tmp_path / "data" / "bronze",
        silver_root=tmp_path / "data" / "silver",
        gold_root=tmp_path / "data" / "gold",
    )
    spark = SparkSessionFactory().create(app_name="test-bronze-to-silver")

    try:
        output_path, validations, row_count = SilverTransformer(
            spark=spark,
            settings=settings,
            validator=DataQualityValidator(),
        ).transform()

        silver_df = spark.read.format("delta").load(output_path)

        assert row_count == 1
        assert all(result.passed for result in validations)
        assert silver_df.count() == 1
        row = silver_df.collect()[0]
        assert row["id"] == "brewery-1"
        assert row["brewery_type"] in {"micro", "regional"}
        assert row["country"] == "United States"
        assert row["state_province"] == "Texas"
    finally:
        spark.stop()
