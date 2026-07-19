# Smart Meter Electricity Consumption — Data Pipeline

I built this for a Big Data Engineering project: an end-to-end pipeline that takes raw, hourly smart-meter readings and turns them into something a grid operator could actually use — anomaly alerts, billing-ready aggregates, and short-term consumption forecasts.

It follows the Medallion architecture (Bronze → Silver → Gold), built on PySpark.

## The problem this solves

Smart meters throw off a lot of data, but raw readings on their own aren't useful to anyone. What energy providers actually need is a way to:

- ingest meter data continuously without losing records
- catch consumption anomalies early — spikes, outages, readings that just don't look right
- roll readings up into hourly/daily/monthly numbers for billing and capacity planning
- get a short-term forecast of expected consumption
- not fall over the moment data arrives late or goes missing

That's what this pipeline is built to do, layer by layer.

## Architecture

```
Bronze  →  raw, append-only ingest. Nothing gets cleaned or dropped here,
           on purpose — even malformed rows land in Bronze so there's a
           full audit trail of what actually arrived.

Silver  →  cleaned, validated, de-duplicated readings. Nulls and negative
           values get flagged and imputed rather than silently discarded.
           This is also where the three anomaly flags get computed.

Gold    →  business-ready tables: hourly/daily/monthly rollups, a 7-day
           moving-average forecast, and a flattened anomaly alert table
           ready for a BI dashboard or an alerting system.
```

## Project layout

```
smart-meter-pipeline/
├── data/                      # generated raw CSVs (gitignored — regenerate via script)
│   ├── meter_readings.csv
│   └── household_info.csv
├── lake/                      # Bronze / Silver / Gold tables (gitignored — regenerate)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── scripts/
│   ├── generate_data.py       # synthetic data generator (messy on purpose)
│   ├── spark_session.py       # shared Spark config + storage-format switch
│   ├── bronze_ingest.py       # Bronze layer
│   ├── silver_clean.py        # Silver layer: cleaning + anomaly detection
│   ├── gold_aggregate.py      # Gold layer: aggregates + forecast + alerts
│   └── run_pipeline.py        # runs all of the above end to end
├── reports/                   # CSV exports of the Gold tables, for a quick look
├── config/
│   └── pipeline_config.md     # thresholds, severity bands, handling rules
├── requirements.txt
└── README.md
```

## Data

**meter_readings.csv** — one row per meter, per hour: `meter_id`, `household_id`, `timestamp`, `units_consumed` (kWh).

**household_info.csv** — dimension table: `household_id`, `city`, `house_type`, `avg_daily_consumption`.

The generator (`scripts/generate_data.py`) doesn't just produce clean numbers — it deliberately injects the kind of mess you'd actually see from real meters: duplicate rows, missing readings, negative values from a faulty sensor, an offline meter that goes dark for several hours, and a meter with a handful of genuine consumption spikes. The point was to give the cleaning and anomaly-detection logic something real to catch, not to grade myself on a pipeline that only ever sees perfect input.

## Anomaly detection

Three rules, computed in Silver:

- **Spike** — a reading more than 3x the meter's trailing 7-day rolling average
- **Zero-extended** — 3 or more consecutive hours of zero consumption
- **Pattern deviation** — a reading outside `mean ± 2σ` for that specific hour-of-week (comparing, say, Tuesday 8pm against other Tuesday 8pms, not against the whole week)

Every flagged reading also gets a severity band (`LOW` / `MEDIUM` / `HIGH`) in the Gold `anomaly_alerts` table, based on how far off the expected value it is. Thresholds are documented in `config/pipeline_config.md` if they need retuning.

## Handling messy/late data

| Scenario | What happens |
|---|---|
| Missing reading | forward-filled from the meter's last valid reading, flagged `is_imputed = true` |
| Duplicate row | de-duplicated in Silver, keeping the most recently ingested copy |
| Negative reading | flagged invalid, then imputed the same way as a missing one |
| Late-arriving data | Bronze keeps an `_ingestion_time` column on every row, so downstream stages can always tell how late a record showed up relative to when it happened |

## Gold tables

- `gold.hourly_consumption` — avg/min/max/sum per meter, per hour
- `gold.daily_consumption` — total kWh, peak hour, anomaly count per meter/day
- `gold.monthly_consumption` — total kWh, average daily kWh, anomaly rate per household/month
- `gold.consumption_trends` — dashboard-facing table with a 7-day simple moving average forecast
- `gold.anomaly_alerts` — one row per detected anomaly, with expected value, % deviation, and severity

## A note on Delta Lake

The problem statement calls for Delta Lake, and the code is written for it — `scripts/spark_session.py` has a `TABLE_FORMAT` switch that toggles every read/write between `"delta"` and `"parquet"`. I built and tested this in a sandboxed environment without open internet access, so Spark couldn't pull the `delta-spark` JARs from Maven Central on first run. Rather than fake it, I ran the whole pipeline on Parquet instead — same medallion structure, same PySpark logic, just without ACID transactions and time travel.

To run it on real Delta Lake (which is what I'd actually use for the late-data `MERGE`/upsert behavior described in the problem statement): set `TABLE_FORMAT=delta` as an environment variable on a machine with normal internet access — Databricks, your own laptop, a CI runner without an egress allow-list — and nothing else in the code needs to change.

```bash
export TABLE_FORMAT=delta   # instead of the parquet default
```

## Running it

```bash
pip install -r requirements.txt

cd scripts
python run_pipeline.py --households 40 --days 30
```

That single command generates the synthetic data, runs Bronze → Silver → Gold, and exports the Gold tables to `../reports/*.csv` so you can look at the results without spinning Spark back up. Individual stages can also be run on their own (`generate_data.py`, `bronze_ingest.py`, `silver_clean.py`, `gold_aggregate.py`) if you want to inspect the lake between steps.

## Tech stack

- Apache Spark (PySpark 3.5)
- Delta Lake (design target) / Parquet (fallback used in this build — see note above)
- Python 3.10+, Pandas, NumPy
- SQL (window functions throughout: rolling averages, `ROW_NUMBER`, `RANK`-style dedup logic)

## What I'd do next

- Swap in real Delta Lake and add an actual `MERGE` statement for late-arriving upserts, rather than the current append + de-dup pass
- Replace the 7-day SMA with a slightly smarter forecast (even a simple exponential smoothing model would likely beat a flat moving average)
- Wire the Gold tables into an actual dashboard (Power BI / Databricks SQL) instead of just exporting CSVs
- Orchestrate with Airflow instead of a single Python script, so each stage is retried/monitored independently

---
*Big Data Engineering project, built around a smart-meter data pipeline problem statement — April 2026.*
