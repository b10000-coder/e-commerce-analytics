# E-Commerce Analytics Engineering Project

![dbt CI](https://github.com/b10000-coder/e-commerce-analytics/actions/workflows/dbt_ci.yml/badge.svg?branch=dev)
![Python](https://img.shields.io/badge/python-3.12-blue)
![dbt](https://img.shields.io/badge/dbt-1.11-orange)
![DuckDB](https://img.shields.io/badge/DuckDB-1.10-yellow)

A end-to-end analytics engineering project built on a **Trendyol-style e-commerce domain** — from raw synthetic data through a fully tested dbt star schema to a live Streamlit dashboard. Demonstrates modern data warehousing practices including SCD Type 2, CI/CD, and metric-layer design.

---

## Architecture

```
generate.py          generate_changes.py
     │                       │
     ▼                       ▼
data/raw/  ──load_raw.py──▶  DuckDB (raw schema)
                                  │
                             dbt build
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
              stg_* views    dim_* tables  fact_orders
              (staging)       (marts)       (marts)
                                  │
                             Streamlit
                             dashboard
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data generation | Python · Faker |
| Database | DuckDB |
| Transformation | dbt-duckdb |
| Testing | dbt schema tests |
| Dashboard | Streamlit · Plotly |
| CI/CD | GitHub Actions |

---

## Data Model

### Star schema (`main_marts`)

```
                       dim_date
                      (182 rows)
                          │
          ┌───────────────┼───────────────┐
          │               │               │
     dim_customer    fact_orders     dim_product
     dim_customer    (4,531 rows)    (400 rows)
       _scd2              │
     (SCD Type 2)         │
                     dim_seller
                     (80 rows)
```

**Fact table grain:** one row per order line item — the most granular level, allowing roll-up to order, customer, product, category, or day.

### Staging layer (`main_staging`)

| Model | Source | Key transformations |
|---|---|---|
| `stg_customers` | `raw.customers` | Deduplicates re-registrations, removes soft-deletes |
| `stg_orders` | `raw.orders` | Parses two date formats (`ISO` + `dd/mm/yyyy`) via `TRY_STRPTIME` |
| `stg_order_items` | `raw.order_items` | Casts types, derives `line_revenue` and `is_flash_sale` |
| `stg_products` | `raw.products` | Derives `gross_margin_pct` |
| `stg_sellers` | `raw.sellers` | Type casting |
| `stg_sessions` | `raw.sessions` | Preserves anonymous sessions (null `customer_id`) |
| `stg_customer_changes` | `raw.customer_changes` | Cleans change events for SCD2 |

### SCD Type 2 — `dim_customer_scd2`

Tracks historical changes to customer `city` and `customer_tier`. Each version of a customer gets a separate row stamped with `valid_from` / `valid_to` dates, enabling point-in-time joins to the fact table.

```sql
-- Get the customer's attributes at the time of their order
JOIN dim_customer_scd2 scd
  ON  fact.customer_id = scd.customer_id
  AND fact.order_date >= scd.valid_from
  AND (fact.order_date < scd.valid_to OR scd.valid_to IS NULL)
```

---

## Project Structure

```
e-commerce-analytics/
├── .github/
│   └── workflows/
│       └── dbt_ci.yml          CI: full pipeline on every push + PR
├── generator/
│   └── generate.py             Synthetic e-commerce data (Faker, seed=42)
├── scripts/
│   ├── load_raw.py             Loads CSVs into DuckDB raw schema
│   └── generate_changes.py     Customer attribute change events (SCD2 source)
├── ecom/                       dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/            7 stg_* views — clean, typed, tested
│       └── marts/              Star schema — 5 tables (4 dims + 1 fact)
│           ├── dim_date.sql
│           ├── dim_customer.sql
│           ├── dim_customer_scd2.sql
│           ├── dim_product.sql
│           ├── dim_seller.sql
│           └── fact_orders.sql
├── dashboard/
│   └── app.py                  Streamlit BI dashboard
├── data/
│   └── raw/                    Generated CSVs (git-ignored)
├── requirements.txt
└── .gitignore
```

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/b10000-coder/e-commerce-analytics.git
cd e-commerce-analytics
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Generate data

```bash
python generator/generate.py        # creates 6 CSVs in data/raw/
python scripts/generate_changes.py  # creates customer_changes.csv
```

### 3. Load into DuckDB

```bash
python scripts/load_raw.py
```

Output:
```
OK    raw.customers          618 rows
OK    raw.sellers             80 rows
OK    raw.products           400 rows
OK    raw.orders           2,500 rows
OK    raw.order_items      4,881 rows
OK    raw.sessions         4,000 rows
OK    raw.customer_changes   142 rows
```

### 4. Run dbt (models + tests)

```bash
cd ecom
dbt build --profiles-dir .
```

Expected output:
```
Done. PASS=78 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=78
```

### 5. Launch the dashboard

```bash
cd ..
streamlit run dashboard/app.py
```

Opens at `http://localhost:8501`

---

## dbt Test Coverage

78 tests across all models covering:

- `unique` and `not_null` on all primary and surrogate keys
- `accepted_values` on status, tier, platform, and device_type columns
- `relationships` tests enforcing referential integrity across all foreign keys
- Point-in-time join integrity for SCD Type 2

---

## CI/CD

Every push to `dev` and every PR to `main` triggers the full pipeline on a clean GitHub Actions runner:

```
checkout → install deps → generate data → load DuckDB → dbt build (78 tests)
```

Total runtime: ~46 seconds. The compiled `ecom.duckdb` is uploaded as a workflow artifact.

---

## Domain

Trendyol-style Turkish e-commerce — 6 months of data (Oct 2024 → Mar 2025):

- **8 product categories** — Electronics, Fashion, Home & Life, Beauty, Sports, Books, Toys
- **80 sellers** across major Turkish cities
- **~600 customers** with bronze / silver / gold tiers
- **2,500 orders** / **4,881 line items** across iOS, Android, and web
- **4,000 sessions** including 20% anonymous (null customer_id)
- **Intentional data quality issues** — mixed date formats, duplicate customers, orphaned foreign keys — resolved in the staging layer

---

## Key Concepts Demonstrated

- **Layered architecture** — raw → staging → marts with clear separation of concerns
- **Star schema** — fact + dimension tables with surrogate keys
- **SCD Type 2** — full customer attribute history via `LEAD()` window function
- **dbt best practices** — sources, refs, schema tests, documented models
- **CI/CD for data** — reproducible pipeline from raw data to tested marts in one command
- **Metric layer** — GMV, AOV, gross margin, flash sale rate defined once in SQL
