from bees_breweries.config.settings import Settings
from bees_breweries.processing.silver_to_gold import GoldAggregator
from bees_breweries.processing.spark_session_factory import SparkSessionFactory
from bees_breweries.processing.validators import DataQualityValidator
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def test_silver_to_gold_filters_closed_and_aggregates_active_breweries(tmp_path) -> None:
    settings = Settings(
        project_root=tmp_path,
        data_root=tmp_path / "data",
        bronze_root=tmp_path / "data" / "bronze",
        silver_root=tmp_path / "data" / "silver",
        gold_root=tmp_path / "data" / "gold",
    )
    spark = SparkSessionFactory().create(app_name="test-silver-to-gold")

    try:
        silver_input_path = settings.silver_root / "openbrewerydb" / "breweries_curated"
        silver_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("brewery_type", StringType(), True),
                StructField("address_1", StringType(), True),
                StructField("address_2", StringType(), True),
                StructField("address_3", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state_province", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("country", StringType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("phone", StringType(), True),
                StructField("website_url", StringType(), True),
                StructField("state", StringType(), True),
                StructField("street", StringType(), True),
                StructField("source_system", StringType(), True),
                StructField("source_endpoint", StringType(), True),
                StructField("ingestion_run_id", StringType(), True),
                StructField("extract_date", StringType(), True),
                StructField("source_page", IntegerType(), True),
                StructField("source_requested_at_utc", TimestampType(), True),
                StructField("silver_processed_at_utc", TimestampType(), True),
                StructField("source_status_code", IntegerType(), True),
                StructField("record_hash", StringType(), True),
            ]
        )
        silver_df = spark.createDataFrame(
            [
                (
                    "brewery-1",
                    "Brewery One",
                    "micro",
                    "123 Main St",
                    None,
                    None,
                    "Austin",
                    "Texas",
                    "78701",
                    "United States",
                    -97.0,
                    30.0,
                    "123",
                    "https://example.com",
                    "Texas",
                    "123 Main St",
                    "openbrewerydb",
                    "/breweries",
                    "run-1",
                    "2026-03-21",
                    1,
                    None,
                    None,
                    200,
                    "hash-1",
                ),
                (
                    "brewery-2",
                    "Brewery Two",
                    "closed",
                    "456 Main St",
                    None,
                    None,
                    "Austin",
                    "Texas",
                    "78702",
                    "United States",
                    -97.1,
                    30.1,
                    "456",
                    "https://example.org",
                    "Texas",
                    "456 Main St",
                    "openbrewerydb",
                    "/breweries",
                    "run-1",
                    "2026-03-21",
                    1,
                    None,
                    None,
                    200,
                    "hash-2",
                ),
                (
                    "brewery-3",
                    "Brewery Three",
                    "micro",
                    "789 Main St",
                    None,
                    None,
                    "Austin",
                    "Texas",
                    "78703",
                    "United States",
                    -97.2,
                    30.2,
                    "789",
                    "https://example.net",
                    "Texas",
                    "789 Main St",
                    "openbrewerydb",
                    "/breweries",
                    "run-2",
                    "2026-03-21",
                    2,
                    None,
                    None,
                    200,
                    "hash-3",
                ),
            ]
            ,
            schema=silver_schema,
        )
        (
            silver_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("country", "state_province")
            .save(str(silver_input_path))
        )

        output_path, validations, row_count = GoldAggregator(
            spark=spark,
            settings=settings,
            validator=DataQualityValidator(),
        ).transform()

        gold_df = spark.read.format("delta").load(output_path)

        assert row_count == 1
        assert all(result.passed for result in validations)
        assert gold_df.count() == 1
        row = gold_df.collect()[0]
        assert row["brewery_type"] == "micro"
        assert row["brewery_count"] == 2
        assert row["country"] == "United States"
        assert row["state_province"] == "Texas"
        assert row["city"] == "Austin"
    finally:
        spark.stop()
