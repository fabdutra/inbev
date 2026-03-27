"""Spark schemas used across processing layers."""

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


BREWERY_RAW_SCHEMA = StructType(
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
    ]
)


BRONZE_REQUEST_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("extract_date", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("request_url", StringType(), False),
        StructField("request_params", StructType(
            [
                StructField("page", IntegerType(), True),
                StructField("per_page", IntegerType(), True),
            ]
        ), True),
        StructField("status_code", IntegerType(), False),
        StructField("response_headers", StructType([]), True),
        StructField("requested_at_utc", StringType(), False),
        StructField("duration_ms", IntegerType(), False),
        StructField("record_count_in_page", IntegerType(), True),
        StructField("page", IntegerType(), True),
        StructField("per_page", IntegerType(), True),
    ]
)
