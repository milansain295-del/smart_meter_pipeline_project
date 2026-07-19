# Pipeline Configuration Reference

These are the thresholds and parameters used across the pipeline. Kept here
as a single reference rather than buried in code, in case they need tuning
for a different dataset.

## Anomaly detection

| Rule | Definition | Where it's computed |
|---|---|---|
| Spike | `units_consumed > 3 x rolling_7day_avg` for that meter | `silver_clean.py` |
| Zero-extended | `units_consumed == 0` for 3+ consecutive hours | `silver_clean.py` |
| Pattern deviation | reading falls outside `mean +/- 2*stddev` for the same hour-of-week | `silver_clean.py` |

## Severity bands (`gold.anomaly_alerts`)

| Severity | Condition |
|---|---|
| HIGH | \|deviation_pct\| >= 200% |
| MEDIUM | \|deviation_pct\| >= 75% |
| LOW | anything else flagged as an anomaly |

These bands are a starting point, not a calibrated model — on a real dataset
you'd want to tune them against a labelled anomaly set (see the "Prediction
MAPE" / "detection recall" targets in the problem statement).

## Late / missing data handling

| Scenario | Strategy |
|---|---|
| Missing hourly reading | forward-fill from the meter's last valid reading; flagged `is_imputed = true` |
| Duplicate (same meter_id + timestamp) | de-duplicated in Silver, keeping the most recently ingested copy (`ROW_NUMBER()` by `_ingestion_time`) |
| Negative consumption | flagged `is_valid = false`, then imputed the same way as a missing reading |
| Late-arriving data | Bronze retains `_ingestion_time` on every row, so Silver can always tell how late a record showed up relative to its event time; with real Delta Lake this is where a `MERGE` (upsert) with a watermark would slot in |

## Storage format

Set via `TABLE_FORMAT` in `scripts/spark_session.py` (or the environment
variable of the same name):

- `parquet` (default here) — works anywhere, no external dependencies beyond PySpark.
- `delta` — true Delta Lake tables with ACID/time-travel/MERGE. Requires
  network access to Maven Central the first time `delta-spark` resolves its
  JARs. Flip this and rerun `run_pipeline.py` on a machine with normal
  internet access (a laptop, Databricks, a CI runner without an egress
  allow-list) and the rest of the code doesn't change.
