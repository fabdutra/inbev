"""Data quality validators for curated layers."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of a single validation check."""

    name: str
    passed: bool
    details: str


class DataQualityValidator:
    """Lightweight validation checks for Silver and Gold datasets."""

    def validate_silver(self, dataframe: DataFrame) -> list[ValidationResult]:
        """Run a focused set of Silver data quality checks."""

        row_count = dataframe.count()
        duplicate_count = (
            dataframe.groupBy("id").count().filter(F.col("count") > 1).count() if row_count else 0
        )
        null_id_count = dataframe.filter(F.col("id").isNull()).count() if row_count else 0
        blank_type_count = (
            dataframe.filter(F.col("brewery_type").isNull() | (F.trim(F.col("brewery_type")) == "")).count()
            if row_count
            else 0
        )

        return [
            ValidationResult(
                name="silver_row_count_positive",
                passed=row_count > 0,
                details=f"row_count={row_count}",
            ),
            ValidationResult(
                name="silver_unique_ids",
                passed=duplicate_count == 0,
                details=f"duplicate_id_count={duplicate_count}",
            ),
            ValidationResult(
                name="silver_non_null_ids",
                passed=null_id_count == 0,
                details=f"null_id_count={null_id_count}",
            ),
            ValidationResult(
                name="silver_non_blank_brewery_type",
                passed=blank_type_count == 0,
                details=f"blank_brewery_type_count={blank_type_count}",
            ),
        ]

    @staticmethod
    def assert_valid(results: list[ValidationResult]) -> None:
        """Raise an error when one or more checks fail."""

        failed = [result for result in results if not result.passed]
        if failed:
            joined = "; ".join(f"{result.name}: {result.details}" for result in failed)
            raise ValueError(f"Data quality validation failed: {joined}")

    def validate_gold(self, dataframe: DataFrame) -> list[ValidationResult]:
        """Run a focused set of Gold data quality checks."""

        row_count = dataframe.count()
        negative_count = dataframe.filter(F.col("brewery_count") < 0).count() if row_count else 0
        closed_count = dataframe.filter(F.lower(F.col("brewery_type")) == "closed").count() if row_count else 0
        null_location_count = (
            dataframe.filter(
                F.col("country").isNull() | F.col("state_province").isNull() | F.col("city").isNull()
            ).count()
            if row_count
            else 0
        )

        return [
            ValidationResult(
                name="gold_row_count_positive",
                passed=row_count > 0,
                details=f"row_count={row_count}",
            ),
            ValidationResult(
                name="gold_non_negative_counts",
                passed=negative_count == 0,
                details=f"negative_brewery_count_rows={negative_count}",
            ),
            ValidationResult(
                name="gold_excludes_closed_breweries",
                passed=closed_count == 0,
                details=f"closed_rows={closed_count}",
            ),
            ValidationResult(
                name="gold_non_null_location_fields",
                passed=null_location_count == 0,
                details=f"null_location_rows={null_location_count}",
            ),
        ]
