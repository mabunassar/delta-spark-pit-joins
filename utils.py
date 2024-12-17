from datetime import datetime
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
)
from pyspark.sql import SparkSession


def get_spark_session_instance():
    spark = (
        SparkSession.builder.appName("PITJoins")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .getOrCreate()
    )
    return spark


class FeaturesTable:
    schema = [
        ("account_id", IntegerType()),
        ("contact_id", StringType()),
        ("timestamp", TimestampType()),  # New field for feature computation time
        ("f_contact_click_count_over_30_days", IntegerType()),
        ("f_contact_click_count_over_60_days", IntegerType()),
        ("f_contact_open_count_over_30_days", IntegerType()),
        ("f_contact_open_count_over_60_days", IntegerType()),
    ]
    mock_data = [
        (100, "first_user", datetime(2024, 12, 1, 6, 0, 0, 0), 20, 40, 20, 40),
        (100, "second_user", datetime(2024, 12, 3, 8, 0, 0, 0), 0, 5, 10, 15),
        (101, "first_user", datetime(2024, 12, 7, 10, 0, 0, 0), 20, 40, 20, 40),
        (102, "third_user", datetime(2024, 12, 5, 7, 0, 0, 0), 100, 100, 100, 100),
    ]

    batch_data = [
        (100, "first_user", datetime(2024, 12, 1, 6, 0, 0, 0), 20, 40, 20, 40),
        (100, "first_user", datetime(2024, 12, 2, 6, 0, 0, 0), 40, 80, 40, 80),
        (101, "first_user", datetime(2024, 12, 9, 10, 0, 0, 0), 20, 40, 20, 40),
        (100, "fourth_user", datetime(2024, 12, 11, 9, 0, 0, 0), 200, 400, 200, 400),
    ]


class ObservationsTable:
    schema = [
        ("account_id", IntegerType()),
        ("contact_id", StringType()),
        ("timestamp", TimestampType()),  # event_timestamp
        ("event_type", StringType()),
    ]

    mock_data = [
        (100, "first_user", datetime(2024, 12, 1, 6, 1, 0, 0), "click"),
        (100, "first_user", datetime(2024, 12, 2, 12, 0, 0, 0), "click"),
        (101, "first_user", datetime(2024, 12, 7, 11, 0, 0, 0), "open"),
        (102, "third_user", datetime(2024, 12, 4, 0, 0, 0, 0), "open"),
    ]
