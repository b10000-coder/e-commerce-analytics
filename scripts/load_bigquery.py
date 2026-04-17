"""
Loads all CSVs from data/raw/ into BigQuery under the 'raw' dataset.
Run this before dbt build --target bigquery.
"""
from google.cloud import bigquery
import os

PROJECT = "pc-api-5223742488799605973-592"
DATASET = "raw"
RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

TABLES = [
    "customers", "sellers", "products", "orders", "order_items",
    "sessions", "customer_changes", "campaigns",
    "seller_changes", "seller_daily_stats",
]

client = bigquery.Client(project=PROJECT)

for table in TABLES:
    path = os.path.join(RAW_DIR, f"{table}.csv")
    if not os.path.exists(path):
        print(f"SKIP {table} — file not found")
        continue

    table_id = f"{PROJECT}.{DATASET}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(path, "rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

    tbl = client.get_table(table_id)
    print(f"OK   {DATASET}.{table:<25} {tbl.num_rows:>6,} rows")

print("\nAll tables loaded into BigQuery raw dataset.")
