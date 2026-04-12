"""
Generates customer_changes.csv — simulated attribute change events.

These represent customers who changed city (e.g. moved) or were
upgraded/downgraded in tier (e.g. reached gold status) during the
Oct 2024 – Mar 2025 period.

This is the source data that drives the SCD Type 2 dim_customer_scd2 model.
Run this once from the repo root before `dbt build`.

Usage:
    python scripts/generate_changes.py
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(99)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
CUSTOMERS_CSV = os.path.join(RAW_DIR, "customers.csv")
OUTPUT_CSV    = os.path.join(RAW_DIR, "customer_changes.csv")

START_DATE = datetime(2024, 10, 1)
END_DATE   = datetime(2025, 3, 31)

TURKISH_CITIES = [
    "Istanbul", "Ankara", "Izmir", "Bursa", "Antalya",
    "Adana", "Konya", "Gaziantep", "Mersin", "Kayseri",
    "Eskisehir", "Trabzon", "Diyarbakir", "Samsun", "Denizli",
]

TIER_UPGRADES = {
    "bronze": "silver",
    "silver": "gold",
    "gold":   "gold",    # already at top
}

def rand_date(start=START_DATE, end=END_DATE):
    delta = (end - start).days
    return start + timedelta(days=random.randint(1, delta))

def main():
    # read existing customers
    with open(CUSTOMERS_CSV, newline="", encoding="utf-8") as f:
        customers = list(csv.DictReader(f))

    # pick ~18% to have at least one change
    changers = random.sample(customers, k=int(len(customers) * 0.18))

    changes = []
    change_id = 1

    for customer in changers:
        cid   = customer["customer_id"]
        city  = customer["city"] or "Istanbul"
        tier  = customer["customer_tier"]

        # decide what changes: city, tier, or both
        change_type = random.choice(["city", "tier", "city", "both"])

        # first change event
        date1 = rand_date()
        new_city1  = city
        new_tier1  = tier

        if change_type in ("city", "both"):
            candidates = [c for c in TURKISH_CITIES if c != city]
            new_city1 = random.choice(candidates)
        if change_type in ("tier", "both"):
            new_tier1 = TIER_UPGRADES[tier]

        changes.append({
            "change_id":    f"CHG{change_id:05d}",
            "customer_id":  cid,
            "effective_date": date1.strftime("%Y-%m-%d"),
            "city":         new_city1,
            "customer_tier": new_tier1,
            "change_reason": change_type,
        })
        change_id += 1

        # ~30% get a second change later in the period
        second_start = date1 + timedelta(days=14)
        if random.random() < 0.30 and second_start < END_DATE:
            date2 = rand_date(start=second_start, end=END_DATE)
            if date2 > date1:
                candidates = [c for c in TURKISH_CITIES if c != new_city1]
                changes.append({
                    "change_id":     f"CHG{change_id:05d}",
                    "customer_id":   cid,
                    "effective_date": date2.strftime("%Y-%m-%d"),
                    "city":          random.choice(candidates),
                    "customer_tier": TIER_UPGRADES[new_tier1],
                    "change_reason": "city",
                })
                change_id += 1

    # write output
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "change_id", "customer_id", "effective_date",
            "city", "customer_tier", "change_reason"
        ])
        writer.writeheader()
        writer.writerows(changes)

    print(f"  OK    customer_changes.csv  {len(changes):,} change events "
          f"across {len(changers)} customers")

if __name__ == "__main__":
    main()
