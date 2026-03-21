"""Silver to Gold transformation logic."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bees_breweries.config.settings import Settings
from bees_breweries.processing.validators import DataQualityValidator, ValidationResult


class GoldAggregator:
    """Build the active-breweries analytical Gold Delta table from Silver."""

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
        """Aggregate active breweries by type and location and persist them as Gold Delta."""

        silver_df = self._load_silver()
        gold_df = self._build_gold_dataframe(silver_df)

        validation_results = self._validator.validate_gold(gold_df)
        self._validator.assert_valid(validation_results)

        output_path = self._gold_output_path()
        (
            gold_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("country")
            .save(output_path)
        )

        return output_path, validation_results, gold_df.count()

    def _load_silver(self):
        return self._spark.read.format("delta").load(self._silver_input_path())

    @staticmethod
    def _build_gold_dataframe(silver_df):
        active_df = silver_df.filter(F.lower(F.col("brewery_type")) != "closed")

        return (
            active_df.groupBy("country", "state_province", "city", "brewery_type")
            .agg(F.countDistinct("id").alias("brewery_count"))
            .withColumn("gold_processed_at_utc", F.current_timestamp())
            .orderBy("country", "state_province", "city", "brewery_type")
        )

    def _silver_input_path(self) -> str:
        return str(self._settings.silver_root / "openbrewerydb" / "breweries_curated")

    def _gold_output_path(self) -> str:
        return str(self._settings.gold_root / "openbrewerydb" / "active_breweries_by_type_location")
