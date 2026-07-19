"""
bronze_ingest.py
-----------------
Bronze layer: raw, append-only ingest of meter_readings.csv into Delta.

- No cleaning happens here on purpose. Malformed rows still get in.
- Adds an _ingestion_time column so Silver can reason about late-arriving data.
- Partitioned by ingestion date, as called for in the problem statement.
"""

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from spark_session import TABLE_FORMAT, get_spark


def run(data_path: str, bronze_path: str):
    spark = get_spark("bronze-ingest")

    schema_hint = "meter_id STRING, household_id STRING, timestamp STRING, units_consumed STRING"

    raw = (
        spark.read.option("header", True)
        .schema(schema_hint)  # read everything as STRING first -> nothing crashes on bad rows
        .csv(data_path)
    )

    bronze = (
        raw.withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_ingestion_date", F.to_date(F.col("_ingestion_time")))
        .withColumn("_source_file", F.input_file_name())
    )

    (
        bronze.write.format(TABLE_FORMAT)
        .mode("overwrite")
        .partitionBy("_ingestion_date")
        .save(bronze_path)
    )

    count = bronze.count()
    print(f"[bronze] wrote {count} raw rows to {bronze_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path", default="../data/meter_readings.csv")
    parser.add_argument("--bronze-path", default="../lake/bronze/meter_readings")
    args = parser.parse_args()
    run(args.data_path, args.bronze_path)
