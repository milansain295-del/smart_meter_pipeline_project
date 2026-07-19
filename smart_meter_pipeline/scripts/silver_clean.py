"""
silver_clean.py
----------------
Silver layer: cleans, validates, standardizes, and enriches Bronze data,
then derives the three anomaly flags called for in the problem statement.

Steps:
  1. Cast types, drop rows with unparsable timestamps
  2. De-duplicate (same meter_id + timestamp) keeping the latest ingestion
  3. Flag nulls / negatives as is_valid = false (don't silently drop them --
     they still get imputed below so downstream aggregates stay complete)
  4. Impute missing / invalid readings via forward-fill per meter, flag is_imputed
  5. Join with household_info for city / house_type context
  6. Derive anomaly flags: is_spike, is_zero_extended, is_deviation
"""

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from spark_session import TABLE_FORMAT, get_spark


def run(bronze_path: str, household_path: str, silver_path: str):
    spark = get_spark("silver-clean")

    bronze = spark.read.format(TABLE_FORMAT).load(bronze_path)

    # ---- 1. type casting + parseable timestamps ---------------------------------
    typed = (
        bronze.withColumn("units_consumed", F.col("units_consumed").cast("double"))
        .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("meter_id").isNotNull() & F.col("household_id").isNotNull())
    )

    # ---- 2. de-duplicate: same meter_id + timestamp, keep most recent ingest ----
    dedup_window = Window.partitionBy("meter_id", "timestamp").orderBy(F.col("_ingestion_time").desc())
    deduped = (
        typed.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ---- 3. validity flag (nulls / negatives) ------------------------------------
    flagged = deduped.withColumn(
        "is_valid",
        F.when(F.col("units_consumed").isNull() | (F.col("units_consumed") < 0), F.lit(False)).otherwise(
            F.lit(True)
        ),
    )

    # ---- 4. impute invalid readings via forward-fill per meter -------------------
    order_window = Window.partitionBy("meter_id").orderBy("timestamp")
    ffill_window = order_window.rowsBetween(Window.unboundedPreceding, 0)

    imputed = (
        flagged.withColumn(
            "units_clean_tmp", F.when(F.col("is_valid"), F.col("units_consumed"))
        )
        .withColumn(
            "units_consumed_final",
            F.last("units_clean_tmp", ignorenulls=True).over(ffill_window),
        )
        .withColumn(
            "is_imputed",
            (~F.col("is_valid")) & F.col("units_consumed_final").isNotNull(),
        )
        # if a meter's very first reading was invalid, there's nothing to ffill from -> fall back to 0
        .withColumn(
            "units_consumed_final",
            F.coalesce(F.col("units_consumed_final"), F.lit(0.0)),
        )
        .drop("units_clean_tmp")
        .withColumnRenamed("units_consumed", "units_consumed_raw")
        .withColumnRenamed("units_consumed_final", "units_consumed")
    )

    # ---- 5. join household dimension for context ----------------------------------
    households = spark.read.option("header", True).csv(household_path)
    households = households.withColumn(
        "avg_daily_consumption", F.col("avg_daily_consumption").cast("double")
    )

    enriched = imputed.join(households, on="household_id", how="left")

    # ---- 6a. is_spike: > 3x the meter's trailing 7-day (168hr) rolling mean --------
    hour_window = Window.partitionBy("meter_id").orderBy(F.col("timestamp").cast("long")).rangeBetween(
        -7 * 24 * 3600, -1
    )
    with_rolling = enriched.withColumn(
        "rolling_avg_7d", F.avg("units_consumed").over(hour_window)
    )
    with_spike = with_rolling.withColumn(
        "is_spike",
        F.when(
            F.col("rolling_avg_7d").isNotNull() & (F.col("units_consumed") > 3 * F.col("rolling_avg_7d")),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    # ---- 6b. is_zero_extended: >= 3 consecutive zero-consumption hours ------------
    zero_flag = F.when(F.col("units_consumed") == 0, 1).otherwise(0)
    seq_window = Window.partitionBy("meter_id").orderBy("timestamp")
    with_zero_run = with_spike.withColumn("_is_zero", zero_flag).withColumn(
        "_zero_group",
        F.sum(F.when(F.col("_is_zero") == 0, 1).otherwise(0)).over(
            seq_window.rowsBetween(Window.unboundedPreceding, 0)
        ),
    )
    run_len_window = Window.partitionBy("meter_id", "_zero_group")
    with_zero_len = with_zero_run.withColumn(
        "_zero_run_length", F.sum("_is_zero").over(run_len_window)
    )
    with_zero_extended = with_zero_len.withColumn(
        "is_zero_extended",
        (F.col("_is_zero") == 1) & (F.col("_zero_run_length") >= 3),
    ).drop("_is_zero", "_zero_group", "_zero_run_length")

    # ---- 6c. is_deviation: outside mean +/- 2 std-dev for same hour-of-week -------
    hod = with_zero_extended.withColumn("hour_of_week", (F.dayofweek("timestamp") - 1) * 24 + F.hour("timestamp"))
    hod_stats_window = Window.partitionBy("meter_id", "hour_of_week")
    with_stats = hod.withColumn("_hod_mean", F.avg("units_consumed").over(hod_stats_window)).withColumn(
        "_hod_std", F.stddev_pop("units_consumed").over(hod_stats_window)
    )
    with_deviation = with_stats.withColumn(
        "is_deviation",
        F.when(
            F.col("_hod_std").isNotNull()
            & (F.col("_hod_std") > 0)
            & (
                (F.col("units_consumed") > F.col("_hod_mean") + 2 * F.col("_hod_std"))
                | (F.col("units_consumed") < F.col("_hod_mean") - 2 * F.col("_hod_std"))
            ),
            F.lit(True),
        ).otherwise(F.lit(False)),
    ).drop("_hod_mean", "_hod_std")

    silver = with_deviation.select(
        "meter_id",
        "household_id",
        "city",
        "house_type",
        "avg_daily_consumption",
        "timestamp",
        "units_consumed_raw",
        "units_consumed",
        "is_valid",
        "is_imputed",
        "rolling_avg_7d",
        "is_spike",
        "is_zero_extended",
        "is_deviation",
        "hour_of_week",
    )

    (
        silver.write.format(TABLE_FORMAT)
        .mode("overwrite")
        .partitionBy("meter_id")
        .save(silver_path)
    )

    total = silver.count()
    invalid = silver.filter(~F.col("is_valid")).count()
    spikes = silver.filter(F.col("is_spike")).count()
    zero_ext = silver.filter(F.col("is_zero_extended")).count()
    deviations = silver.filter(F.col("is_deviation")).count()

    print(f"[silver] wrote {total} cleaned rows to {silver_path}")
    print(f"[silver] invalid readings encountered (then imputed): {invalid}")
    print(f"[silver] flagged spikes           : {spikes}")
    print(f"[silver] flagged zero-extended    : {zero_ext}")
    print(f"[silver] flagged pattern deviations: {deviations}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", default="../lake/bronze/meter_readings")
    parser.add_argument("--household-path", default="../data/household_info.csv")
    parser.add_argument("--silver-path", default="../lake/silver/meter_readings")
    args = parser.parse_args()
    run(args.bronze_path, args.household_path, args.silver_path)
