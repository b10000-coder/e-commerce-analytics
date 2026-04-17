# E-Commerce Analytics Engineering Project

![dbt CI](https://github.com/b10000-coder/e-commerce-analytics/actions/workflows/dbt_ci.yml/badge.svg?branch=dev)
![Python](https://img.shields.io/badge/python-3.12-blue)
![dbt](https://img.shields.io/badge/dbt-1.11-orange)
![DuckDB](https://img.shields.io/badge/DuckDB-1.10-yellow)
![BigQuery](https://img.shields.io/badge/BigQuery-Google_Cloud-4285F4)
![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE)

An end-to-end analytics engineering portfolio project built on a **Trendyol-style e-commerce domain**. Covers the full modern data stack: synthetic data generation, a fully tested dbt star schema, dual-engine deployment (DuckDB locally, BigQuery in production), Airflow orchestration, and a live Looker Studio dashboard.

---

## Architecture

```
generator/generate.py
        │
        ▼
  data/raw/ (10 CSVs)
        │
        ├──► scripts/load_raw.py ──► DuckDB (local dev)
        │                                  │
        └──► scripts/load_bigquery.py ──► BigQuery raw dataset (production)
                                               │
                                          dbt build
                                          (--target bigquery)
                                               │
                              ┌────────────────┼────────────────┐
                              ▼                ▼                ▼
                        stg_* views      dim_* tables     fact_* tables
                        (staging)         (marts)           (marts)
                                               │
                                    ┌──────────┴──────────┐
                                    ▼                     ▼
                             Airflow DAG           Looker Studio
                          (orchestration)           (dashboard)
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data generation | Python · Faker |
| Local database | DuckDB 1.10 |
| Production database | Google BigQuery |
| Transformation | dbt 1.11 (dbt-duckdb + dbt-bigquery) |
| Testing | dbt schema tests (100 tests) |
| Orchestration | Apache Airflow 2.9 |
| Dashboard | Looker Studio |
| CI/CD | GitHub Actions |

---

## Data Model

### Star schema (19 models, 100/100 tests)

```
                            dim_date
                           (182 rows)
                                │
          ┌─────────────────────┼──────────────────────┐
          │                     │                      │
    dim_customer          fact_orders            dim_product
    dim_customer_scd2    (4,500+ rows)           (400 rows)
      (SCD Type 2)              │
                                │
                    ┌───────────┴───────────┐
                    │                       │
               dim_seller             dim_campaign
               dim_seller_scd2         (20 rows)
                (SCD Type 2)

               fact_seller_daily
               (incremental, 3,993 rows)
```

**Fact table grain:** one row per order line item — the most granular level, supporting roll-up to order, customer, product, category, campaign, or day.

### Staging layer (10 models)

| Model | Source | Key transformations |
|---|---|---|
| `stg_customers` | `raw.customers` | Deduplication, referential integrity |
| `stg_orders` | `raw.orders` | Dual date format parsing (`ISO` + `dd/mm/yyyy`), target-conditional SQL |
| `stg_order_items` | `raw.order_items` | Derives `line_revenue`, `is_flash_sale` |
| `stg_products` | `raw.products` | Derives `gross_margin_pct` |
| `stg_sellers` | `raw.sellers` | Type casting |
| `stg_sessions` | `raw.sessions` | Preserves anonymous sessions |
| `stg_customer_changes` | `raw.customer_changes` | SCD2 source events |
| `stg_seller_changes` | `raw.seller_changes` | Seller SCD2 source events |
| `stg_seller_daily_stats` | `raw.seller_daily_stats` | Aggregated seller metrics |
| `stg_campaigns` | `raw.campaigns` | Campaign metadata cleaning |

### SCD Type 2 — `dim_customer_scd2` and `dim_seller_scd2`

Tracks historical changes to customer `city` / `customer_tier` and seller `rating` / `category`. Each version gets its own row with Trendyol's production column pattern:

```sql
effective_date   -- when this version became active
expiry_date      -- when this version was superseded (NULL = current)
is_current_flag  -- boolean shortcut for current version queries
```

Point-in-time join pattern:

```sql
JOIN dim_customer_scd2 scd
  ON  fact.customer_id  = scd.customer_id
  AND fact.order_date  >= scd.effective_date
  AND (fact.order_date  < scd.expiry_date OR scd.expiry_date IS NULL)
```

### Incremental models

Both `fact_orders` and `fact_seller_daily` are incremental. On each run, only new records since the last watermark are inserted — no full table scans.

BigQuery-compatible watermark pattern using CROSS JOIN to avoid subquery-in-WHERE restrictions:

```sql
{% if is_incremental() %}
, watermark as (
    select coalesce(max(cast(order_date as date)), date '1900-01-01') as max_date
    from {{ this }}
)
{% endif %}

select joined.*
from joined
{% if is_incremental() %}
cross join watermark
where cast(joined.order_date as date) > watermark.max_date
{% endif %}
```

---

## Dual-Engine Deployment

The same dbt models run against both DuckDB (local dev) and BigQuery (production) by switching a single line in `profiles.yml`:

```yaml
ecom:
  target: dev        # ← change to 'bigquery' for production
  outputs:
    dev:
      type: duckdb
      path: "../data/ecom.duckdb"
    bigquery:
      type: bigquery
      method: service-account
      project: <gcp-project-id>
      dataset: marts
      keyfile: ~/.dbt/dbt-ecom-key.json
      location: EU
```

Engine-specific SQL is handled with `{% if target.type == 'bigquery' %}` blocks. Key differences resolved:

| Pattern | DuckDB | BigQuery |
|---|---|---|
| Date parsing | `try_strptime()` | `safe.parse_date()` |
| Date spine | `generate_series()` | `generate_date_array()` |
| Date formatting | `strftime()` | `format_date()` |
| Decimal types | `decimal(10,2)` | `numeric` |
| Type casting | `::date` | `cast(x as date)` |

---

## Airflow Orchestration

Daily pipeline DAG (`ecom_daily_pipeline`) running at 06:00 UTC:

```
start
  └── check_source_freshness (BranchPythonOperator)
        ├── [FRESH] load_raw_to_bigquery
        │             └── dbt_build_bigquery
        │                     └── notify_success
        └── [STALE] notify_stale
```

**Freshness check:** verifies `orders.csv` was modified within 24 hours before spending compute on a BigQuery build. If data is stale, the pipeline branches to `notify_stale` and skips the build entirely.

**Dependency isolation:** Airflow runs in a dedicated venv (`airflow_venv/`) separate from the dbt venv (`.venv/`). This mirrors production deployments where orchestrators and the tools they call have independent dependency graphs.

---

## Looker Studio Dashboard

Live dashboard connected directly to BigQuery marts:

🔗 [View Dashboard](https://datastudio.google.com/reporting/cd32c187-2909-4b3e-8456-19d2efd0b275)

Charts:
- **Daily Revenue** — line chart over `order_date`
- **Revenue by Order Status** — bar chart (delivered, cancelled, processing, etc.)
- **Revenue by Platform** — pie chart (Web 49%, Android 40%, iOS 11%)
- **Total GMV** — scorecard

---

## Project Structure

```
e-commerce-analytics/
├── .github/
│   └── workflows/
│       └── dbt_ci.yml              CI: full pipeline on every push + PR
├── airflow/
│   └── dags/
│       └── ecom_pipeline.py        Airflow DAG (freshness → load → dbt → notify)
├── generator/
│   └── generate.py                 Synthetic data generator (10 CSVs, Faker, seed=42)
├── scripts/
│   ├── load_raw.py                 Loads CSVs → DuckDB raw schema
│   └── load_bigquery.py            Loads CSVs → BigQuery raw dataset
├── ecom/                           dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml                Dev (DuckDB) + bigquery targets
│   └── models/
│       ├── staging/                10 stg_* views
│       └── marts/                  9 mart models
│           ├── dim_date.sql
│           ├── dim_customer.sql
│           ├── dim_customer_scd2.sql
│           ├── dim_seller.sql
│           ├── dim_seller_scd2.sql
│           ├── dim_product.sql
│           ├── dim_campaign.sql
│           ├── fact_orders.sql     (incremental)
│           └── fact_seller_daily.sql (incremental)
├── data/
│   └── raw/                        Generated CSVs (git-ignored)
├── requirements.txt
└── .gitignore
```

---

## Quick Start

### Local (DuckDB)

```bash
git clone https://github.com/b10000-coder/e-commerce-analytics.git
cd e-commerce-analytics
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python generator/generate.py        # generates 10 CSVs in data/raw/
python scripts/load_raw.py          # loads into DuckDB

cd ecom
dbt build --profiles-dir .          # 100/100 tests
```

### Production (BigQuery)

```bash
# Authenticate GCP
gcloud auth application-default login

# Load raw data to BigQuery
python scripts/load_bigquery.py

# Run dbt against BigQuery
cd ecom
dbt build --profiles-dir . --target bigquery   # 100/100 tests
```

### Airflow

```bash
python -m venv airflow_venv && source airflow_venv/bin/activate
pip install "apache-airflow==2.9.1" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt"

export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone
# UI at http://localhost:8080
```

---

## dbt Test Coverage

100 tests across all 19 models:

- `unique` and `not_null` on all primary and surrogate keys
- `accepted_values` on `order_status`, `payment_method`, `customer_tier`, `campaign_type`, `device_type`, `platform`
- `relationships` enforcing referential integrity across all foreign keys
- Tests pass identically on both DuckDB and BigQuery

---

## CI/CD

Every push to `dev` and every PR to `main` triggers the full pipeline on GitHub Actions:

```
checkout → install deps → generate data → load DuckDB → dbt build (100 tests)
```

---

## Domain

Trendyol-style Turkish e-commerce — 6 months of synthetic data (Oct 2024 → Mar 2025):

- **8 product categories** — Electronics, Fashion, Home & Life, Beauty, Sports, Books, Toys, Food
- **80 sellers** across Turkish cities with rating and category change history
- **618 customers** with bronze / silver / gold tiers and city change history
- **2,500 orders** / **4,881 line items** across iOS, Android, and web
- **20 campaigns** — flash sale, coupon, seasonal, loyalty types
- **4,000 sessions** including anonymous (null `customer_id`)
- **Intentional data quality issues** — mixed date formats, orphaned foreign keys — resolved in the staging layer

---

## Key Concepts Demonstrated

- **Layered architecture** — raw → staging → marts with clear separation of concerns
- **Star schema** — fact + dimension tables with surrogate keys
- **SCD Type 2** — full customer and seller attribute history using `LEAD()` window functions and Trendyol's `effective_date / expiry_date / is_current_flag` pattern
- **Incremental models** — watermark-based append-only loads on both fact tables
- **Dual-engine SQL** — target-conditional Jinja blocks, same models on DuckDB and BigQuery
- **Orchestration** — Airflow DAG with branching, freshness gates, and retry logic
- **dbt best practices** — sources, refs, schema tests, documented models
- **Production deployment** — GCP service account auth, BigQuery datasets, Looker Studio BI layer
