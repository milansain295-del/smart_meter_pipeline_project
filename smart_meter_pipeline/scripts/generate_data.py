"""
generate_data.py
-----------------
Generates synthetic (but realistic) smart-meter data:
  - household_info.csv : dimension table, one row per household
  - meter_readings.csv : hourly readings per meter, with realistic
    daily/weekly consumption patterns AND deliberately injected
    problems (duplicates, nulls, negatives, gaps, an offline meter,
    a few spikes) so the Bronze -> Silver -> Gold pipeline has real
    work to do downstream.

Usage:
    python generate_data.py --households 40 --days 30 --seed 42
"""

import argparse
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

CITIES = ["Delhi", "Mumbai", "Jaipur", "Bengaluru", "Pune", "Hyderabad"]
HOUSE_TYPES = ["Apartment", "Independent House", "Villa", "Studio"]


def build_household_info(n_households: int, rng: random.Random) -> pd.DataFrame:
    rows = []
    for i in range(1, n_households + 1):
        house_type = rng.choice(HOUSE_TYPES)
        # bigger dwellings tend to consume more on average
        base = {"Studio": 4, "Apartment": 8, "Independent House": 12, "Villa": 18}[house_type]
        rows.append(
            {
                "household_id": f"H{i:03d}",
                "city": rng.choice(CITIES),
                "house_type": house_type,
                "avg_daily_consumption": base + rng.randint(-2, 4),
            }
        )
    return pd.DataFrame(rows)


def hourly_profile(hour: int) -> float:
    """Rough daily load curve: low overnight, morning bump, evening peak."""
    curve = {
        0: 0.3, 1: 0.25, 2: 0.2, 3: 0.2, 4: 0.25, 5: 0.4,
        6: 0.7, 7: 0.9, 8: 0.8, 9: 0.6, 10: 0.5, 11: 0.5,
        12: 0.6, 13: 0.6, 14: 0.5, 15: 0.5, 16: 0.6, 17: 0.8,
        18: 1.1, 19: 1.4, 20: 1.5, 21: 1.3, 22: 0.9, 23: 0.5,
    }
    return curve[hour]


def build_meter_readings(households: pd.DataFrame, n_days: int, rng: random.Random,
                          np_rng: np.random.Generator) -> pd.DataFrame:
    start = datetime(2026, 4, 1, 0, 0, 0)
    n_hours = n_days * 24
    timestamps = [start + timedelta(hours=h) for h in range(n_hours)]

    rows = []
    meter_map = {}
    for idx, hh in households.iterrows():
        meter_id = f"M{idx + 1:03d}"
        meter_map[meter_id] = hh

    # pick a couple of meters to misbehave, so downstream anomaly detection
    # actually has something to catch
    offline_meter = rng.choice(list(meter_map.keys()))
    offline_start = rng.randint(100, n_hours - 30)
    offline_len = rng.randint(6, 14)  # hours of zero/dead readings

    spike_meter = rng.choice(list(meter_map.keys()))
    spike_hours = rng.sample(range(n_hours), k=5)

    for meter_id, hh in meter_map.items():
        daily_avg = hh["avg_daily_consumption"]
        hourly_base = daily_avg / 24
        for h, ts in enumerate(timestamps):
            weekday_factor = 1.15 if ts.weekday() >= 5 else 1.0  # a bit more on weekends
            profile = hourly_profile(ts.hour)
            noise = np_rng.normal(loc=1.0, scale=0.12)
            value = round(max(hourly_base * profile * weekday_factor * noise, 0), 3)

            # inject an offline stretch for one meter
            if meter_id == offline_meter and offline_start <= h < offline_start + offline_len:
                value = 0.0

            # inject spikes for one meter
            if meter_id == spike_meter and h in spike_hours:
                value = round(hourly_base * rng.uniform(6, 10), 3)

            rows.append(
                {
                    "meter_id": meter_id,
                    "household_id": hh["household_id"],
                    "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "units_consumed": value,
                }
            )

    df = pd.DataFrame(rows)

    # ---- inject realistic mess -------------------------------------------------
    # 1) duplicate rows (same meter+timestamp resubmitted)
    dup_sample = df.sample(frac=0.004, random_state=rng.randint(0, 10_000))
    df = pd.concat([df, dup_sample], ignore_index=True)

    # 2) missing values (sensor dropout)
    null_idx = df.sample(frac=0.01, random_state=rng.randint(0, 10_000)).index
    df.loc[null_idx, "units_consumed"] = np.nan

    # 3) negative readings (faulty sensor)
    neg_idx = df.sample(frac=0.003, random_state=rng.randint(0, 10_000)).index
    df.loc[neg_idx, "units_consumed"] = -df.loc[neg_idx, "units_consumed"].abs()

    # 4) a handful of completely missing hours (gap in the sequence) -> just drop rows
    drop_idx = df.sample(frac=0.006, random_state=rng.randint(0, 10_000)).index
    df = df.drop(index=drop_idx)

    # 5) a few late-arriving rows: shove their ingestion far after their event time
    #    (captured via a separate _ingestion_time column added at Bronze load time,
    #    so nothing to do here except leave timestamps as-is)

    df = df.sample(frac=1.0, random_state=rng.randint(0, 10_000)).reset_index(drop=True)
    return df


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic smart meter data")
    parser.add_argument("--households", type=int, default=40)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--outdir", type=str, default="../data")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    np_rng = np.random.default_rng(args.seed)

    households = build_household_info(args.households, rng)
    readings = build_meter_readings(households, args.days, rng, np_rng)

    households.to_csv(f"{args.outdir}/household_info.csv", index=False)
    readings.to_csv(f"{args.outdir}/meter_readings.csv", index=False)

    print(f"households.csv rows        : {len(households)}")
    print(f"meter_readings.csv rows    : {len(readings)}")
    print(f"nulls injected             : {readings['units_consumed'].isna().sum()}")
    print(f"negative readings injected : {(readings['units_consumed'] < 0).sum()}")
    print("Done. Files written to:", args.outdir)


if __name__ == "__main__":
    main()
