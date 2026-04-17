"""
Loads all CSVs from data/raw/ into DuckDB as tables under the 'raw' schema.
Run this once before `dbt build`.

Usage (from repo root):
    python scripts/load_raw.py
"""

import duckdb
import os

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "ecom.duckdb")

TABLES = [
    "customers",
    "sellers",
    "products",
    "orders",
    "order_items",
    "sessions",
    "customer_changes",
    "campaigns",
    "seller_changes",
    "seller_daily_stats",
]

def main():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    for table in TABLES:
        csv_path = os.path.join(RAW_DIR, f"{table}.csv")
        if not os.path.exists(csv_path):
            print(f"  SKIP  {table}.csv not found")
            continue

        con.execute(f"DROP TABLE IF EXISTS raw.{table}")
        con.execute(f"""
            CREATE TABLE raw.{table} AS
            SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true)
        """)

        count = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        print(f"  OK    raw.{table:<15} {count:>6,} rows")

    con.close()
    print(f"\nDatabase written to: {DB_PATH}")

if __name__ == "__main__":
    main()
