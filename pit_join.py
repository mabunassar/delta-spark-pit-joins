import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructField,
    StructType,
)
from utils import FeaturesTable, ObservationsTable, get_spark_session_instance

delta_table_path = "/tmp/delta-table"


def create_features_table(data, schema):
    schema = StructType([StructField(x, y) for x, y in schema])
    spark = get_spark_session_instance()
    return spark.createDataFrame(data, schema=schema)


def point_in_time_join(observations_df, features_df):
    # Join observation table on the features table
    # We can extend the functionality here to multiple features tables (joins)
    joined_df = observations_df.alias("obs").join(
        features_df.alias("feat"),
        (F.col("obs.contact_id") == F.col("feat.contact_id"))
        & (F.col("obs.account_id") == F.col("feat.account_id"))
        & (F.col("feat.timestamp") <= F.col("obs.timestamp")),
        "left",
    )

    # Get the most recent feature from the observation timestamp
    window_spec = Window.partitionBy(
        "obs.contact_id", "obs.account_id", "obs.timestamp"
    ).orderBy(F.col("feat.timestamp").desc())

    ranked_df = joined_df.withColumn("row_number", F.row_number().over(window_spec))
    closest_features_df = ranked_df.filter(F.col("row_number") == 1).drop("row_number")
    return closest_features_df


def main():
    spark = get_spark_session_instance()

    # Setup delta tables
    features_df = create_features_table(FeaturesTable.mock_data, FeaturesTable.schema)
    observations_df = create_features_table(
        ObservationsTable.mock_data, ObservationsTable.schema
    )

    features_path = os.path.join(delta_table_path, "features_table")
    features_df.write.format("delta").mode("overwrite").save(features_path)

    observations_path = os.path.join(delta_table_path, "observations_table")
    observations_df.write.format("delta").mode("overwrite").save(observations_path)

    features_df = spark.read.format("delta").load(features_path)
    observations_df = spark.read.format("delta").load(observations_path)
    print("Feature table with initial data:")
    features_df.show()
    ####### Setup done

    # Batch update delta tables
    delta_table = DeltaTable.forPath(spark, features_path)
    new_data = create_features_table(FeaturesTable.batch_data, FeaturesTable.schema)

    exclude_column = "timestamp"
    columns_to_match = [col for col in features_df.columns if col != exclude_column]
    matching_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in columns_to_match]
    )
    delta_table.alias("target").merge(
        new_data.alias("source"), matching_condition
    ).whenMatchedUpdate(
        set={"timestamp": F.col("source.timestamp")}
    ).whenNotMatchedInsertAll().execute()
    ####### Batch update done

    features_path = os.path.join(delta_table_path, "features_table")
    features_df = spark.read.format("delta").load(features_path)
    print("Feature table after batch update:")
    features_df.show()

    observations_path = os.path.join(delta_table_path, "observations_table")
    observations_df = spark.read.format("delta").load(observations_path)
    print("Observations table:")
    observations_df.show()

    joined_df = point_in_time_join(observations_df, features_df)
    joined_df.show()


if __name__ == "__main__":
    main()
