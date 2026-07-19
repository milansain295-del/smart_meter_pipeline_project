"""
spark_session.py
-----------------
One place to build a SparkSession, so every stage script (bronze / silver /
gold) configures Spark the same way.

TABLE_FORMAT controls the physical storage format for every layer:
  - "delta"   : true Delta Lake tables (ACID, time travel, MERGE/upsert).
                This is what the pipeline is designed for, and what the
                problem statement asks for. It needs the io.delta:delta-spark
                Maven package, which Spark downloads over the network the
                first time it runs.
  - "parquet" : plain partitioned Parquet. Same read/write pattern, just
                without ACID transactions or time travel. Used as a
                drop-in fallback in network-restricted environments (CI
                runners, offline sandboxes) where Maven Central isn't
                reachable to fetch the Delta JARs.

To run this project on real Delta Lake: set the TABLE_FORMAT environment
variable to "delta" (or change the default below) on a machine with normal
internet access. No other code changes are needed -- every stage script
reads and writes through this one function.
"""

import os

from pyspark.sql import SparkSession

TABLE_FORMAT = os.environ.get("TABLE_FORMAT", "parquet")  # "delta" or "parquet"


def get_spark(app_name: str = "smart-meter-pipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
    )

    if TABLE_FORMAT == "delta":
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark
