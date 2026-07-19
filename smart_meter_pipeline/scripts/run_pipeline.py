"""
run_pipeline.py
----------------
Runs the full pipeline end to end: Bronze -> Silver -> Gold, then exports
the Gold tables as CSV into ../reports/ so they're easy to preview (e.g. in
GitHub) without needing to spin up Spark again.

Usage:
    python run_pipeline.py
    python run_pipeline.py --skip-generate     # reuse existing data/*.csv
"""

import argparse
import subprocess
import sys
import time

from spark_session import TABLE_FORMAT, get_spark


def run_step(description: str, cmd: list):
    print(f"\n{'=' * 70}\n{description}\n{'=' * 70}")
    start = time.time()
    result = subprocess.run(cmd)
    elapsed = time.time() - start
    if result.returncode != 0:
        print(f"[FAILED] {description} (after {elapsed:.1f}s)")
        sys.exit(1)
    print(f"[OK] {description} ({elapsed:.1f}s)")


def export_gold_to_csv(gold_path: str, reports_dir: str):
    spark = get_spark("export-reports")
    tables = [
        "hourly_consumption",
        "daily_consumption",
        "monthly_consumption",
        "consumption_trends",
        "anomaly_alerts",
    ]
    for name in tables:
        df = spark.read.format(TABLE_FORMAT).load(f"{gold_path}/{name}")
        # coalesce to a single file and write via pandas so we get one clean .csv,
        # not a Spark part-file directory
        df.toPandas().to_csv(f"{reports_dir}/{name}.csv", index=False)
        print(f"[export] {reports_dir}/{name}.csv")
    spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--households", type=int, default=40)
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()

    print(f"Table format: {TABLE_FORMAT}  (set TABLE_FORMAT=delta to use real Delta Lake)")

    if not args.skip_generate:
        run_step(
            "STEP 0/3 - Generating synthetic meter data",
            [
                sys.executable,
                "generate_data.py",
                "--households",
                str(args.households),
                "--days",
                str(args.days),
            ],
        )

    run_step("STEP 1/3 - Bronze: raw ingestion", [sys.executable, "bronze_ingest.py"])
    run_step("STEP 2/3 - Silver: cleansing + anomaly flags", [sys.executable, "silver_clean.py"])
    run_step("STEP 3/3 - Gold: aggregations + predictions", [sys.executable, "gold_aggregate.py"])

    print(f"\n{'=' * 70}\nExporting Gold tables to ../reports/ as CSV\n{'=' * 70}")
    export_gold_to_csv("../lake/gold", "../reports")

    print("\nPipeline complete. Explore ../lake/{bronze,silver,gold} or ../reports/*.csv")


if __name__ == "__main__":
    main()
