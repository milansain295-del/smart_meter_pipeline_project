"""
gold_aggregate.py
------------------
Gold layer: business-ready tables built off Silver.

Produces:
  - gold.hourly_consumption      : avg/min/max/sum per meter per hour
  - gold.daily_consumption       : total kWh, peak hour, anomaly count per meter/day
  - gold.monthly_consumption     : total kWh, avg daily, anomaly rate per household/month
  - gold.consumption_trends      : dashboard-facing table incl. 7-day SMA prediction
  - gold.anomaly_alerts          : one row per detected anomaly, with severity
"""

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from spark_session import TABLE_FORMAT, get_spark


def build_hourly(silver):
    return (
        silver.groupBy("meter_id", F.window("timestamp", "1 hour").alias("w"))
        .agg(
            F.avg("units_consumed").alias("avg_kwh"),
            F.min("units_consumed").alias("min_kwh"),
            F.max("units_consumed").alias("max_kwh"),
            F.sum("units_consumed").alias("sum_kwh"),
        )
        .select(
            "meter_id",
            F.col("w.start").alias("hour_start"),
            "avg_kwh",
            "min_kwh",
            "max_kwh",
            "sum_kwh",
        )
    )


def build_daily(silver):
    daily_base = silver.withColumn("date", F.to_date("timestamp"))

    totals = daily_base.groupBy("meter_id", "household_id", "date").agg(
        F.sum("units_consumed").alias("total_kwh_daily"),
        F.avg("units_consumed").alias("avg_hourly_kwh"),
        (
            F.sum(F.col("is_spike").cast("int"))
            + F.sum(F.col("is_zero_extended").cast("int"))
            + F.sum(F.col("is_deviation").cast("int"))
        ).alias("anomaly_count"),
    )

    # peak hour needs its own pass: the hour with the max reading that day
    hour_rank_window = Window.partitionBy("meter_id", "date").orderBy(F.col("units_consumed").desc())
    peak = (
        daily_base.withColumn("rn", F.row_number().over(hour_rank_window))
        .filter(F.col("rn") == 1)
        .select("meter_id", "date", F.hour("timestamp").alias("peak_hour"))
    )

    return totals.join(peak, on=["meter_id", "date"], how="left")


def build_monthly(daily):
    monthly_base = daily.withColumn("month", F.date_format("date", "yyyy-MM"))
    return monthly_base.groupBy("household_id", "month").agg(
        F.sum("total_kwh_daily").alias("total_kwh_monthly"),
        F.avg("total_kwh_daily").alias("avg_daily_kwh"),
        F.sum("anomaly_count").alias("total_anomalies"),
        F.count("*").alias("days_recorded"),
    ).withColumn(
        "anomaly_rate", F.round(F.col("total_anomalies") / (F.col("days_recorded") * 24), 4)
    )


def build_trends(daily, silver):
    dims = silver.select("meter_id", "household_id", "city", "house_type").dropDuplicates(["meter_id"])

    sma_window = Window.partitionBy("meter_id").orderBy("date").rowsBetween(-6, 0)
    with_sma = daily.withColumn("sma_7day", F.round(F.avg("total_kwh_daily").over(sma_window), 3))

    trends = with_sma.join(dims.drop("household_id"), on="meter_id", how="left")

    return trends.select(
        "meter_id",
        "city",
        "house_type",
        "date",
        F.round("total_kwh_daily", 3).alias("total_kwh_daily"),
        F.round("avg_hourly_kwh", 3).alias("avg_hourly_kwh"),
        "peak_hour",
        "sma_7day",
        "anomaly_count",
    )


def build_anomaly_alerts(silver):
    long_form = []
    for flag, label in [
        ("is_spike", "SPIKE"),
        ("is_zero_extended", "ZERO_EXTENDED"),
        ("is_deviation", "DEVIATION"),
    ]:
        subset = (
            silver.filter(F.col(flag))
            .withColumn("anomaly_type", F.lit(label))
            .withColumn(
                "expected_value",
                F.when(F.col("rolling_avg_7d").isNotNull(), F.col("rolling_avg_7d")).otherwise(
                    F.col("avg_daily_consumption") / 24
                ),
            )
        )
        long_form.append(subset)

    unioned = long_form[0]
    for part in long_form[1:]:
        unioned = unioned.unionByName(part)

    with_dev_pct = unioned.withColumn(
        "deviation_pct",
        F.when(
            F.col("expected_value") > 0,
            F.round(
                (F.col("units_consumed") - F.col("expected_value")) / F.col("expected_value") * 100, 2
            ),
        ).otherwise(F.lit(None)),
    )

    with_severity = with_dev_pct.withColumn(
        "severity",
        F.when(F.abs(F.coalesce(F.col("deviation_pct"), F.lit(0))) >= 200, F.lit("HIGH"))
        .when(F.abs(F.coalesce(F.col("deviation_pct"), F.lit(0))) >= 75, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )

    return with_severity.select(
        "meter_id",
        "household_id",
        "timestamp",
        F.col("units_consumed").alias("units_consumed"),
        "expected_value",
        "anomaly_type",
        "deviation_pct",
        "severity",
    ).orderBy("timestamp")


def run(silver_path: str, gold_path: str):
    spark = get_spark("gold-aggregate")
    silver = spark.read.format(TABLE_FORMAT).load(silver_path)

    hourly = build_hourly(silver)
    daily = build_daily(silver)
    monthly = build_monthly(daily)
    trends = build_trends(daily, silver)
    alerts = build_anomaly_alerts(silver)

    tables = {
        "hourly_consumption": hourly,
        "daily_consumption": daily,
        "monthly_consumption": monthly,
        "consumption_trends": trends,
        "anomaly_alerts": alerts,
    }

    for name, df in tables.items():
        path = f"{gold_path}/{name}"
        df.write.format(TABLE_FORMAT).mode("overwrite").save(path)
        print(f"[gold] wrote {df.count()} rows -> {path}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", default="../lake/silver/meter_readings")
    parser.add_argument("--gold-path", default="../lake/gold")
    args = parser.parse_args()
    run(args.silver_path, args.gold_path)
