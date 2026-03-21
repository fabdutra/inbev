"""Bronze to Silver transformation logic."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from bees_breweries.config.schemas import BRONZE_REQUEST_LOG_SCHEMA, BREWERY_RAW_SCHEMA
from bees_breweries.config.settings import Settings
from bees_breweries.processing.validators import DataQualityValidator, ValidationResult


class SilverTransformer:
    """Transform Bronze raw brewery payloads into a curated Silver Delta table."""

    def __init__(
        self,
        spark: SparkSession,
        settings: Settings,
        validator: DataQualityValidator,
    ) -> None:
        self._spark = spark
        self._settings = settings
        self._validator = validator

    def transform(self) -> tuple[str, list[ValidationResult], int]:
        """Build the Silver curated dataset from Bronze data and return output details."""

        breweries_df = self._load_bronze_breweries()
        request_logs_df = self._load_bronze_request_logs()

        silver_df = self._build_silver_dataframe(breweries_df, request_logs_df)
        validation_results = self._validator.validate_silver(silver_df)
        self._validator.assert_valid(validation_results)

        output_path = self._silver_output_path()
        (
            silver_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("country", "state_province")
            .save(output_path)
        )

        return output_path, validation_results, silver_df.count()

    def _load_bronze_breweries(self) -> DataFrame:
        breweries_glob = str(self._settings.bronze_root / "openbrewerydb" / "breweries" / "*" / "*" / "*.json")

        dataframe = (
            self._spark.read.option("multiLine", True)
            .schema(BREWERY_RAW_SCHEMA)
            .json(breweries_glob)
            .withColumn("source_file_path", F.input_file_name())
            .withColumn("extract_date", F.regexp_extract("source_file_path", r"extract_date=([^/]+)", 1))
            .withColumn("ingestion_run_id", F.regexp_extract("source_file_path", r"run_id=([^/]+)", 1))
            .withColumn(
                "source_page",
                F.regexp_extract("source_file_path", r"page=(\\d+)\\.json", 1).cast("int"),
            )
        )

        return dataframe

    def _load_bronze_request_logs(self) -> DataFrame:
        request_logs_glob = str(
            self._settings.bronze_root / "openbrewerydb" / "request_logs" / "*" / "*" / "page=*.json"
        )

        return (
            self._spark.read.schema(BRONZE_REQUEST_LOG_SCHEMA)
            .json(request_logs_glob)
            .select(
                "run_id",
                "extract_date",
                F.col("page").alias("log_page"),
                "requested_at_utc",
                "status_code",
                "request_url",
            )
        )

    def _build_silver_dataframe(self, breweries_df: DataFrame, request_logs_df: DataFrame) -> DataFrame:
        joined_df = breweries_df.join(
            request_logs_df,
            on=[
                breweries_df.ingestion_run_id == request_logs_df.run_id,
                breweries_df.extract_date == request_logs_df.extract_date,
                breweries_df.source_page == request_logs_df.log_page,
            ],
            how="left",
        )

        selected_df = joined_df.select(
            breweries_df["id"],
            breweries_df["name"],
            breweries_df["brewery_type"],
            breweries_df["address_1"],
            breweries_df["address_2"],
            breweries_df["address_3"],
            breweries_df["city"],
            breweries_df["state_province"],
            breweries_df["postal_code"],
            breweries_df["country"],
            breweries_df["longitude"].cast("double").alias("longitude"),
            breweries_df["latitude"].cast("double").alias("latitude"),
            breweries_df["phone"],
            breweries_df["website_url"],
            breweries_df["state"],
            breweries_df["street"],
            F.lit("openbrewerydb").alias("source_system"),
            F.lit("/breweries").alias("source_endpoint"),
            breweries_df["ingestion_run_id"],
            breweries_df["extract_date"],
            breweries_df["source_page"],
            F.to_timestamp("requested_at_utc").alias("source_requested_at_utc"),
            F.current_timestamp().alias("silver_processed_at_utc"),
            F.col("status_code").alias("source_status_code"),
        )

        normalized_df = self._normalize_strings(selected_df).withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(column).cast("string"), F.lit(""))
                        for column in [
                            "id",
                            "name",
                            "brewery_type",
                            "address_1",
                            "address_2",
                            "address_3",
                            "city",
                            "state_province",
                            "postal_code",
                            "country",
                            "longitude",
                            "latitude",
                            "phone",
                            "website_url",
                            "state",
                            "street",
                        ]
                    ],
                ),
                256,
            ),
        )

        window = Window.partitionBy("id").orderBy(
            F.col("source_requested_at_utc").desc_nulls_last(),
            F.col("extract_date").desc(),
            F.col("source_page").desc_nulls_last(),
        )

        return (
            normalized_df.withColumn("row_rank", F.row_number().over(window))
            .filter(F.col("row_rank") == 1)
            .drop("row_rank")
        )

    @staticmethod
    def _normalize_strings(dataframe: DataFrame) -> DataFrame:
        string_columns = [
            field.name
            for field in dataframe.schema.fields
            if field.dataType.simpleString() == "string"
        ]

        normalized_df = dataframe
        for column in string_columns:
            normalized_df = normalized_df.withColumn(
                column,
                F.when(F.trim(F.col(column)) == "", F.lit(None)).otherwise(F.trim(F.col(column))),
            )
        return normalized_df

    def _silver_output_path(self) -> str:
        return str(self._settings.silver_root / "openbrewerydb" / "breweries_curated")
